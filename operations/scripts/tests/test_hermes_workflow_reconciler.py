import importlib.util, json, sqlite3, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch

P = Path(__file__).parents[1] / "hermes_workflow_reconciler.py"
spec = importlib.util.spec_from_file_location("reconciler", P); m = importlib.util.module_from_spec(spec); sys.modules[spec.name] = m; spec.loader.exec_module(m)

class ReconcilerTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory(); self.root = Path(self.t.name)
        self.target_patcher = patch.object(
            m, "compass_prepare_current_working_target", return_value="2033-05-18"
        )
        self.target_patcher.start(); self.addCleanup(self.target_patcher.stop)
        (self.root/"cron").mkdir(); (self.root/"scripts").mkdir(); (self.root/"cron/jobs.json").write_text('{"jobs":[]}')
        (self.root/"cron/ticker_last_success").touch()
        c=sqlite3.connect(self.root/"kanban.db"); c.execute("create table tasks(id text,status text,started_at integer,worker_pid integer,last_heartbeat_at integer,claim_expires integer)"); c.execute("create table task_events(id integer primary key, task_id text, kind text, payload text)"); c.commit(); c.close()
    def tearDown(self): self.t.cleanup()
    def put(self, values):
        c=sqlite3.connect(self.root/"kanban.db"); c.execute("insert into tasks values(?,?,?,?,?,?)",values); c.commit(); c.close()
    def test_expired_pidless_heartbeatless_is_reclaimable(self):
        self.put(("t_x","running",1,None,None,100))
        f=m.detect_kanban(self.root,2000); self.assertEqual(f[0].repair,"reclaim")
    def test_live_worker_is_never_reclaimed(self):
        live_pid=__import__('os').getpid()
        for row in [("pid","running",1,live_pid,None,100),("beat","running",1,None,1500,100),("claim","running",1,None,None,3000)]: self.put(row)
        self.assertEqual(m.detect_kanban(self.root,2000),[])
    def test_dead_pid_outside_this_namespace_is_observe_only(self):
        self.put(("dead","running",1,99999999,500,100))
        with patch.object(m, "_pid_exists", return_value=False):
            finding=[f for f in m.detect_kanban(self.root,2000) if f.kind=="kanban_dead_worker_unverified"][0]
        self.assertIsNone(finding.repair)
    def test_visible_pid_is_not_reclaimed_even_when_projection_is_old(self):
        self.put(("live","running",1,44,500,100))
        with patch.object(m, "_pid_exists", return_value=True):
            self.assertFalse(any(f.kind=="kanban_dead_worker" for f in m.detect_kanban(self.root,2000)))
    def test_ready_crash_reclaim_loop_is_circuit_blocked(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table task_events add column created_at integer")
        c.execute("insert into tasks values(?,?,?,?,?,?)",("t_churn","ready",1,None,None,None))
        c.executemany("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)", [
            ("t_churn", kind, "{}", stamp)
            for kind, stamp in (("reclaimed",1900),("crashed",1910),("reclaimed",1920),("reclaimed",1930))
        ])
        c.commit(); c.close()
        finding=[f for f in m.detect_kanban(self.root,2000)
                 if f.kind=="kanban_worker_churn_circuit"][0]
        self.assertEqual(finding.repair,"block_transient")
        calls=[]
        def runner(cmd,**kw):
            calls.append(cmd)
            return type("R",(),{"returncode":0,"stdout":"ok","stderr":""})()
        ok,_=m.mutate(finding,Path("/hermes"),runner)
        self.assertTrue(ok)
        self.assertEqual(calls[0][2:6],["block","t_churn","--kind","transient"])
    def test_cross_board_mutation_clears_inherited_board_pin(self):
        finding=m.Finding("x","t_x","project","reclaim","test")
        calls=[]
        def runner(cmd,**kw):
            calls.append((cmd,kw))
            return type("R",(),{"returncode":0,"stdout":"ok","stderr":""})()
        with patch.dict(m.os.environ,{"HERMES_KANBAN_DB":"/wrong.db","HERMES_KANBAN_BOARD":"default"}):
            ok,_=m.mutate(finding,Path("/hermes"),runner)
        self.assertTrue(ok)
        self.assertEqual(calls[0][0][2:4],["--board","project"])
        self.assertNotIn("HERMES_KANBAN_DB",calls[0][1]["env"])
        self.assertNotIn("HERMES_KANBAN_BOARD",calls[0][1]["env"])
    def test_running_worker_is_not_circuit_blocked(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table task_events add column created_at integer")
        c.execute("insert into tasks values(?,?,?,?,?,?)",("t_live","running",1,42,1999,3000))
        c.executemany("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",
                      [("t_live","reclaimed","{}",stamp) for stamp in (1900,1910,1920,1930)])
        c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_worker_churn_circuit"
                             for f in m.detect_kanban(self.root,2000)))
    def test_historical_host_rollback_is_observe_only(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("create table task_comments(id integer primary key,task_id text,author text,body text)")
        c.execute("insert into tasks values(?,?,?,?,?,?)",("t_rollback","ready",1,None,1900,None))
        c.execute("insert into task_events(task_id,kind,payload) values(?,?,?)",(
            "t_rollback","blocked",json.dumps({"reason":"Host-root action required: run rollback now"})))
        c.execute("insert into task_comments(task_id,author,body) values(?,?,?)",(
            "t_rollback","default","HOST ROLLBACK COMPLETED 2026-08-20T11:11:53Z: verified"))
        c.execute("insert into task_comments(task_id,author,body) values(?,?,?)",(
            "t_rollback","default","SCHEDULED: unrelated later projection event"))
        c.commit(); c.close()
        finding=[f for f in m.detect_kanban(self.root,2000)
                 if f.kind=="kanban_host_rollback_terminal_drift"][0]
        self.assertIsNone(finding.repair)
    def _install_compass_stage_schema(self):
        db=self.root/"kanban/boards/compass/kanban.db"; db.parent.mkdir(parents=True)
        c=sqlite3.connect(db)
        c.execute("create table tasks(id text primary key,title text,body text,status text,created_at integer,started_at integer,worker_pid integer,last_heartbeat_at integer,claim_expires integer,priority integer,idempotency_key text,block_kind text)")
        c.execute("create table task_links(parent_id text,child_id text)")
        c.execute("create table task_events(id integer primary key,task_id text,kind text,payload text,created_at integer)")
        c.commit(); c.close()
        return db

    def _install_spare_capacity_and_gateway(self, now):
        (self.root/"kanban").mkdir(exist_ok=True)
        (self.root/"state").mkdir(exist_ok=True)
        (self.root/"kanban/capacity-scheduler-snapshot.json").write_text(json.dumps({
            "mode":"healthy","target":4,"workers":3,"checked_at":now,
        }))
        (self.root/"state/gateway.lifecycle.json").write_text(json.dumps({
            "phase":"running","pid":123,"start_time":now-100,
        }))
        heartbeat=self.root/"state/gateway.heartbeat"; heartbeat.touch()
        __import__('os').utime(heartbeat,(now,now))

    def _install_compass_published_week(self, published="2033-05-11"):
        folder=self.root/"compass/content/en/newsletters"
        folder.mkdir(parents=True,exist_ok=True)
        (folder/f"{published}-newsletter.md").write_text("---\ndraft: false\n---\n")

    def test_compass_target_uses_prepare_origin_main_resolver_not_stale_checkout(self):
        self._install_compass_published_week("2033-05-04")
        with patch.object(
            m, "compass_prepare_current_working_target", return_value="2033-05-18"
        ) as resolver:
            self.assertEqual(m.compass_current_working_target(self.root), "2033-05-18")
        resolver.assert_called_once_with(self.root / "compass")

    def test_current_capacity_snapshot_is_authoritative(self):
        (self.root/"kanban").mkdir()
        (self.root/"kanban/capacity-scheduler-snapshot.json").write_text(json.dumps({
            "mode":"healthy","target":6,"workers":2,"checked_at":2_000_000_000,
        }))
        self.assertTrue(m.dispatcher_is_healthy(self.root, 2_000_000_000))

    def test_fresh_heavy_running_snapshot_with_spare_capacity_is_admissible(self):
        (self.root/"kanban").mkdir()
        snapshot = self.root/"kanban/capacity-scheduler-snapshot.json"
        snapshot.write_text(json.dumps({
            "mode":"heavy-running","target":6,"workers":2,"checked_at":2_000_000_000,
        }))
        self.assertTrue(m.dispatcher_is_healthy(self.root, 2_000_000_060))
        snapshot.write_text(json.dumps({
            "mode":"heavy-running","target":6,"workers":6,"checked_at":2_000_000_000,
        }))
        self.assertFalse(m.dispatcher_is_healthy(self.root, 2_000_000_060))

    def test_stale_or_maintenance_drain_snapshot_is_not_admissible(self):
        (self.root/"kanban").mkdir()
        snapshot = self.root/"kanban/capacity-scheduler-snapshot.json"
        snapshot.write_text(json.dumps({
            "mode":"maintenance-drain","target":6,"workers":0,"checked_at":2_000_000_000,
        }))
        self.assertFalse(m.dispatcher_is_healthy(self.root, 2_000_000_060))
        snapshot.write_text(json.dumps({
            "mode":"healthy","target":6,"workers":0,"checked_at":2_000_000_000,
        }))
        self.assertFalse(m.dispatcher_is_healthy(self.root, 2_000_000_121))

    def test_compass_focus_parked_stage_is_released_with_spare_capacity(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        c.executemany("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("parent","Compass Research and Preparation 2033-05-18","","done",now-4000,now-3900,None,now-3500,None,10000,"compass-research-2033-05-18",None),
            ("child","Compass Section Writing 2033-05-18","","scheduled",now-3000,None,None,None,None,10000,"compass-writing-2033-05-18",None),
        ])
        c.execute("insert into task_links values('parent','child')")
        c.execute("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",(
            "child","scheduled",json.dumps({"reason":"Paused by cross-board focus control while issue-intake is urgent"}),now-1200,
        ))
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        finding=[f for f in m.detect_kanban(self.root,now)
                 if f.kind=="compass_weekly_stage_focus_stall"][0]
        self.assertEqual((finding.subject,finding.repair),("child","unblock"))

    def test_compass_recovery_requires_exact_focus_ownership_and_p0(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        c.executemany("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("parent","Compass Research and Preparation 2033-05-18","","done",now-4000,None,None,None,None,10000,"compass-research-2033-05-18",None),
            ("manual","Compass Section Writing 2033-05-18","","scheduled",now-3000,None,None,None,None,10000,"compass-writing-2033-05-18",None),
            ("low","Compass Newsletter Pipeline 2033-05-18","","scheduled",now-3000,None,None,None,None,100,"compass-pipeline-2033-05-18",None),
        ])
        c.executemany("insert into task_links values('parent',?)",[("manual",),("low",)])
        c.executemany("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",[
            ("manual","scheduled",json.dumps({"reason":"Explicit user hold"}),now-1200),
            ("low","scheduled",json.dumps({"reason":"Paused by cross-board focus control while issue-intake is urgent"}),now-1200),
        ])
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        self.assertFalse(any(f.kind=="compass_weekly_stage_focus_stall"
                             for f in m.detect_kanban(self.root,now)))

    def test_compass_transient_retry_is_released_only_after_health_recovers(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        c.executemany("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("parent","Compass Section Writing 2033-05-18","","done",now-4000,None,None,None,None,10000,"compass-writing-2033-05-18",None),
            ("child","Compass Newsletter Pipeline 2033-05-18","","blocked",now-3000,now-2500,None,now-2400,None,10000,"compass-pipeline-2033-05-18","transient"),
        ])
        c.execute("insert into task_links values('parent','child')")
        c.executemany("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",[
            ("child","blocked",json.dumps({"reason":"worker spawn failed after retry budget"}),now-1200),
            ("child","gave_up",json.dumps({"trigger_outcome":"spawn_failed","error":"worker process spawn failed"}),now-1200),
        ])
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        finding=[f for f in m.detect_kanban(self.root,now)
                 if f.kind=="compass_weekly_stage_retry_stall"][0]
        self.assertEqual(finding.repair,"unblock")
        (self.root/"state/gateway.heartbeat").unlink()
        self.assertFalse(any(f.kind=="compass_weekly_stage_retry_stall"
                             for f in m.detect_kanban(self.root,now)))

    def test_compass_partial_graph_is_reconciled_idempotently(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        c.execute("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)",(
            "research","Compass Research and Preparation 2033-05-18","","done",
            now-4000,now-3900,None,now-3500,None,10000,
            "compass-research-2033-05-18",None,
        ))
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        finding=[f for f in m.detect_kanban(self.root,now)
                 if f.kind=="compass_weekly_graph_incomplete"][0]
        self.assertEqual((finding.subject,finding.repair),
                         ("2033-05-18","reconcile_compass_weekly"))
        calls=[]
        def runner(cmd,**kw):
            calls.append((cmd,kw)); return type("R",(),{"returncode":0,"stdout":"ok","stderr":""})()
        ok,_=m.mutate(finding,Path("/hermes"),runner,root=self.root)
        self.assertTrue(ok)
        self.assertEqual(calls[0][0],[
            sys.executable,str(self.root/"scripts/compass_tuesday_prepare.py"),
            "--reconcile-target","2033-05-18",
        ])
        self.assertNotIn("HERMES_KANBAN_DB",calls[0][1]["env"])

    def test_latest_target_terminal_three_card_week_is_not_reconciled(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        c=sqlite3.connect(db)
        for stage,status in (("pipeline","done"),("translate","archived"),("podcast","done")):
            c.execute("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)",(
                stage,f"Compass {stage} 2033-05-18","",status,
                now-4000,None,None,None,None,10000,
                f"compass-{stage}-2033-05-18",None,
            ))
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        with patch.object(m,"compass_current_working_target",return_value="2033-05-18"):
            findings=m.detect_kanban(self.root,now)
        self.assertFalse(any(
            f.kind=="compass_weekly_graph_incomplete" and f.subject=="2033-05-18"
            for f in findings
        ))

    def test_historical_partial_graph_is_not_replayed(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        for target in ("2033-05-11","2033-05-18"):
            c.execute("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)",(
                target,f"Compass Research and Preparation {target}","","done",
                now-4000,None,None,None,None,10000,
                f"compass-research-{target}",None,
            ))
        for stage in ("writing","pipeline","translate","podcast"):
            c.execute("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)",(
                f"new-{stage}",f"Compass {stage} 2033-05-18","","todo",
                now-3000,None,None,None,None,10000,
                f"compass-{stage}-2033-05-18",None,
            ))
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        self.assertFalse(any(
            f.kind=="compass_weekly_graph_incomplete" and f.subject=="2033-05-11"
            for f in m.detect_kanban(self.root,now)
        ))

    def test_compass_complete_graph_does_not_reconcile_or_duplicate(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        stages=("research","writing","pipeline","translate","podcast")
        for index,stage in enumerate(stages):
            c.execute("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)",(
                stage,f"Compass {stage} 2033-05-18","","done" if stage=="research" else "todo",
                now-4000+index,None,None,None,None,10000,
                f"compass-{stage}-2033-05-18",None,
            ))
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        self.assertFalse(any(f.kind=="compass_weekly_graph_incomplete"
                             for f in m.detect_kanban(self.root,now)))

    def test_published_historical_stage_is_never_reactivated(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        self._install_compass_published_week()
        c=sqlite3.connect(db)
        c.executemany("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("old-parent","Compass Research and Preparation 2033-05-11","","done",now-4000,None,None,None,None,10000,"compass-research-2033-05-11",None),
            ("old-child","Compass Section Writing 2033-05-11","","scheduled",now-3000,None,None,None,None,10000,"compass-writing-2033-05-11",None),
        ])
        c.execute("insert into task_links values('old-parent','old-child')")
        c.execute("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",(
            "old-child","scheduled",json.dumps({"reason":"Paused by cross-board focus control while issue-intake is urgent"}),now-1200,
        ))
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        self.assertFalse(any(
            f.kind=="compass_weekly_stage_focus_stall" and f.subject=="old-child"
            for f in m.detect_kanban(self.root,now)
        ))

    def test_compass_outcome_marker_is_never_replayed_by_reconciler(self):
        now=2_000_000_000; db=self._install_compass_stage_schema()
        c=sqlite3.connect(db)
        c.execute("create table task_comments(id integer primary key,task_id text,author text,body text)")
        c.execute("insert into tasks values(?,?,?,?,?,?,?,?,?,?,?,?)",(
            "pipeline","Compass Newsletter Pipeline 2033-05-18","","done",
            now-4000,now-3900,None,now-3500,None,10000,
            "compass-pipeline-2033-05-18",None,
        ))
        c.executemany("insert into task_comments(task_id,author,body) values(?,?,?)",[
            ("pipeline","default","MARMOT_OUTCOME:\n[Newsletter Markdown](/safe/newsletter.md)"),
            ("pipeline","default","MARMOT_OUTCOME:\n[Newsletter Markdown](/safe/newsletter.md)"),
        ])
        c.commit(); c.close(); self._install_spare_capacity_and_gateway(now)
        findings=m.detect_kanban(self.root,now)
        duplicate=[f for f in findings if f.kind=="compass_weekly_outcome_duplicate"][0]
        self.assertIsNone(duplicate.repair)
        self.assertFalse(any(f.repair in {"unblock","promote","reconcile_compass_weekly"}
                             and f.subject=="pipeline" for f in findings))

    def test_guarded_capacity_does_not_resume_circuit_blocks(self):
        (self.root/"kanban").mkdir()
        (self.root/"kanban/capacity-scheduler-snapshot.json").write_text(
            '{"mode":"guarded","target":6}'
        )
        self.assertFalse(m.dispatcher_is_healthy(self.root))
    def test_arbitrary_comment_cannot_authorize_capability_unblock(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column block_kind text")
        c.execute("create table task_comments(id integer primary key,task_id text,body text)")
        c.execute("insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,block_kind) values(?,?,?,?,?,?,?)",
                  ("t_cap","blocked",1,None,None,None,"capability"))
        c.execute("insert into task_comments(task_id,body) values(?,?)",("t_cap","github authentication is unavailable"))
        c.commit(); c.close()
        with patch.object(m,"github_auth_available",return_value=True):
            self.assertFalse(any(f.repair=="unblock" for f in m.detect_kanban(self.root,2000)))
    def test_recent_retry_exhaustion_is_unblocked_for_recovery(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column block_kind text")
        c.execute("alter table task_events add column created_at integer")
        c.execute("insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,block_kind) values(?,?,?,?,?,?,?)",
                  ("t_crash","blocked",1,None,None,None,"transient"))
        c.execute("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",
                  ("t_crash","gave_up",'{"trigger_outcome":"crashed","error":"pid 9 not alive"}',1900))
        c.commit(); c.close()
        finding=[f for f in m.detect_kanban(self.root,2000)
                 if f.kind=="kanban_worker_retry_exhausted"][0]
        self.assertIsNone(finding.repair)
    def test_completion_like_block_reason_is_observe_only(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column block_kind text")
        c.execute("insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,block_kind) values(?,?,?,?,?,?,?)",
                  ("t_quote","blocked",1,None,None,None,"capability"))
        c.execute("insert into task_events(task_id,kind,payload) values(?,?,?)",
                  ("t_quote","blocked",'{"reason":"focused verification passed. continue automated"}'))
        c.commit(); c.close()
        finding=[f for f in m.detect_kanban(self.root,2000) if f.kind=="kanban_completed_work_parked"][0]
        self.assertIsNone(finding.repair)
    def test_non_epoch_retry_timestamps_fail_closed(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column block_kind text")
        c.execute("alter table task_events add column created_at integer")
        for task_id, stamp in (("iso","2026-08-20T00:00:00Z"),("ms",2000000)):
            c.execute("insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,block_kind) values(?,?,?,?,?,?,?)",
                      (task_id,"blocked",1,None,None,None,"transient"))
            c.execute("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",
                      (task_id,"gave_up",'{"trigger_outcome":"crashed","error":"pid 9 not alive"}',stamp))
        c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_worker_retry_exhausted"
                             for f in m.detect_kanban(self.root,2000)))
    def test_needs_input_is_never_unblocked_as_crash_recovery(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column block_kind text")
        c.execute("alter table task_events add column created_at integer")
        c.execute("insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,block_kind) values(?,?,?,?,?,?,?)",
                  ("t_gate","blocked",1,None,None,None,"needs_input"))
        c.execute("insert into task_events(task_id,kind,payload,created_at) values(?,?,?,?)",
                  ("t_gate","gave_up",'{"trigger_outcome":"crashed","error":"pid 9 not alive"}',1900))
        c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_worker_retry_exhausted"
                             for f in m.detect_kanban(self.root,2000)))
    def test_empty_needs_input_gate_is_observe_only(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column block_kind text")
        c.execute("insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,block_kind) values(?,?,?,?,?,?,?)",
                  ("t_empty","blocked",1,None,None,None,"needs_input"))
        c.commit(); c.close()
        finding=[f for f in m.detect_kanban(self.root,2000) if f.kind=="kanban_malformed_owner_gate"][0]
        self.assertIsNone(finding.repair)
    def test_missing_claim_or_start_never_reclaims(self):
        self.put(("no_claim","running",1,None,None,None))
        self.put(("no_start","running",None,None,None,100))
        self.assertFalse(any(f.repair=="reclaim" for f in m.detect_kanban(self.root,2000)))
    def test_completed_dependencies_are_promoted_but_cycles_are_not(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("create table task_links(parent_id text,child_id text)")
        for row in (("p","done",1,None,None,None),("child","todo",1,None,None,None),
                    ("a","todo",1,None,None,None),("b","todo",1,None,None,None)):
            c.execute("insert into tasks values(?,?,?,?,?,?)",row)
        c.executemany("insert into task_links values(?,?)",[("p","child"),("a","b"),("b","a")])
        c.commit(); c.close()
        findings=m.detect_kanban(self.root,2000)
        promoted=[f for f in findings if f.kind=="kanban_dependencies_satisfied"]
        self.assertEqual([f.subject for f in promoted],["child"])
        self.assertEqual({f.subject for f in findings if f.kind=="kanban_dependency_cycle"},{"a","b"})
        calls=[]
        def runner(cmd,**kw):
            calls.append(cmd); return type("R",(),{"returncode":0,"stdout":"ok","stderr":""})()
        ok,_=m.mutate(promoted[0],Path("/hermes"),runner)
        self.assertTrue(ok)
        self.assertEqual(calls[0],["/hermes","kanban","promote","child",
                                  "self-healer: all recorded parent dependencies are terminal","--json"])
    def test_integer_dependency_cycle_is_never_promoted(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("drop table tasks")
        c.execute("create table tasks(id integer,status text,started_at integer,worker_pid integer,last_heartbeat_at integer,claim_expires integer)")
        c.execute("create table task_links(parent_id integer,child_id integer)")
        c.executemany("insert into tasks values(?,?,?,?,?,?)",[(1,"todo",1,None,None,None),(2,"todo",1,None,None,None)])
        c.executemany("insert into task_links values(?,?)",[(1,2),(2,1),(1,1)])
        c.commit(); c.close()
        findings=m.detect_kanban(self.root,2000)
        self.assertFalse(any(f.kind=="kanban_dependencies_satisfied" for f in findings))
        self.assertEqual({f.subject for f in findings if f.kind=="kanban_dependency_cycle"},{"1","2"})
    def test_stale_cron_is_observe_only_outside_gateway_pid_namespace(self):
        c=sqlite3.connect(self.root/"cron/executions.db"); c.execute("create table executions(id text,job_id text,status text,claimed_at text,started_at text,pid integer,process_started_at integer)"); c.execute("insert into executions values('e','j','running','1970-01-01T00:00:01+00:00',null,99999999,null)"); c.commit(); c.close()
        f=m.detect_cron(self.root,2000); stale=[x for x in f if x.kind=='cron_execution_stale'][0]
        self.assertIsNone(stale.repair)

    def test_proc_stat_comm_with_spaces_preserves_owner_match(self):
        stat="123 (worker name with spaces) " + " ".join(["S"] + ["0"] * 18 + ["777"])
        with patch.object(Path,"read_text",return_value=stat):
            self.assertTrue(m._owner_matches(123,777))

    def test_live_long_running_cron_is_not_reported_stale(self):
        import os
        start=int(Path(f'/proc/{os.getpid()}/stat').read_text().split()[21])
        c=sqlite3.connect(self.root/"cron/executions.db"); c.execute("create table executions(id text,job_id text,status text,claimed_at text,started_at text,pid integer,process_started_at integer)"); c.execute("insert into executions values('e','j','running','1970-01-01T00:00:01+00:00',null,?,?)",(os.getpid(),start)); c.commit(); c.close()
        self.assertFalse(any(x.kind=='cron_execution_stale' for x in m.detect_cron(self.root,2000)))
    def test_queued_claim_is_not_reported_as_uncertain_execution(self):
        c=sqlite3.connect(self.root/"cron/executions.db"); c.execute("create table executions(id text,job_id text,status text,claimed_at text,started_at text,pid integer,process_started_at integer)"); c.execute("insert into executions values('e','j','claimed','1970-01-01T00:00:01+00:00',null,99999999,null)"); c.commit(); c.close()
        self.assertFalse(any(x.kind=='cron_execution_stale' for x in m.detect_cron(self.root,2000)))
    def test_cooldown_and_circuit_state_are_durable(self):
        self.put(("t_x","running",1,None,None,100)); state=self.root/"state.json"; audit=self.root/"audit.jsonl"; calls=[]
        def runner(cmd,**kw): calls.append(cmd); return type("R",(),{"returncode":1,"stdout":"","stderr":"no"})()
        m.reconcile(self.root,10000,state,audit,runner=runner); m.reconcile(self.root,10001,state,audit,runner=runner)
        self.assertEqual(len(calls),1)
        records=[json.loads(line) for line in audit.read_text().splitlines()]
        ghost=[r for r in records if r["finding"]["kind"]=="kanban_never_started"]
        self.assertEqual(len(ghost),2); self.assertEqual([r["action"] for r in ghost],["reclaim","observe"])

    def test_failed_repair_is_retried_after_capped_backoff_not_abandoned(self):
        self.put(("t_x","running",1,None,None,100)); state=self.root/"state.json"; audit=self.root/"audit.jsonl"; calls=[]
        def runner(cmd,**kw): calls.append(cmd); return type("R",(),{"returncode":1,"stdout":"","stderr":"no"})()
        now=10000
        for _ in range(5):
            m.reconcile(self.root,now,state,audit,runner=runner)
            now += m.MAX_RETRY_BACKOFF + 1
        self.assertEqual(len(calls),5)
    def test_completion_protocol_duplicate_decomposition_routes_to_safe_reconciliation(self):
        self.put(("t_root","blocked",1,None,None,None))
        c=sqlite3.connect(self.root/"kanban.db")
        c.executemany("insert into task_events(task_id,kind,payload) values(?,?,?)", [
            ("t_root","block_loop_detected",'{"reason":"completion tool unavailable"}'),
            ("t_root","decomposed",'{"child_ids":["t_a"]}'),
            ("t_root","decomposed",'{"child_ids":["t_b"]}'),
        ]); c.commit(); c.close()
        finding=[f for f in m.detect_kanban(self.root,2000)
                 if f.kind=="kanban_completion_decomposition_loop"][0]
        self.assertEqual(finding.subject,"t_root")
        self.assertIsNone(finding.repair, "must never blindly unblock/replay")
    def test_single_decomposition_is_not_a_reconciliation_incident(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.executemany("insert into task_events(task_id,kind,payload) values(?,?,?)", [
            ("t_root","protocol_violation",'{}'),
            ("t_root","decomposed",'{"child_ids":["t_a"]}'),
        ]); c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_completion_decomposition_loop"
                             for f in m.detect_kanban(self.root,2000)))

    def test_observe_only_incident_is_marked_new_once(self):
        self.put(("t_root","blocked",1,None,None,None))
        c=sqlite3.connect(self.root/"kanban.db")
        c.executemany("insert into task_events(task_id,kind,payload) values(?,?,?)", [
            ("t_root","protocol_violation",'{}'),
            ("t_root","decomposed",'{}'),
            ("t_root","decomposed",'{}'),
        ]); c.commit(); c.close()
        state=self.root/"state.json"; audit=self.root/"audit.jsonl"
        _, first=m.reconcile(self.root,2000,state,audit)
        _, second=m.reconcile(self.root,2001,state,audit)
        first_loop=[a for a in first if a["finding"]["kind"]=="kanban_completion_decomposition_loop"][0]
        second_loop=[a for a in second if a["finding"]["kind"]=="kanban_completion_decomposition_loop"][0]
        self.assertTrue(first_loop["new"])
        self.assertFalse(second_loop["new"])

    def test_terminal_or_intentionally_scheduled_decomposition_history_is_not_stalled(self):
        for task_id, status in (("t_done", "done"), ("t_parked", "scheduled"),
                                ("t_archived", "archived")):
            self.put((task_id,status,1,None,None,None))
        c=sqlite3.connect(self.root/"kanban.db")
        for task_id in ("t_done", "t_parked", "t_archived"):
            c.executemany("insert into task_events(task_id,kind,payload) values(?,?,?)", [
                (task_id,"protocol_violation",'{}'),
                (task_id,"decomposed",'{}'),
                (task_id,"decomposed",'{}'),
            ])
        c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_completion_decomposition_loop"
                             for f in m.detect_kanban(self.root,2000)))

    def _install_implementation_owner_schema(self):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("alter table tasks add column title text")
        c.execute("alter table tasks add column body text")
        c.execute("alter table tasks add column created_at integer")
        c.execute("create table task_links(parent_id text,child_id text)")
        c.commit(); c.close()

    def _authority_and_body(self, key="github:marmot-protocol/whitenoise-android#2182"):
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)",
            ("t_authority","done",None,None,None,None,"Select implementation owner",
             f"IMPLEMENTATION_AUTHORITY_GRANTED: {key}",m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH),
        )
        c.commit(); c.close()
        return (
            "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing\n"
            f"IMPLEMENTATION_OWNER_KEY: {key}\n"
            "IMPLEMENTATION_AUTHORITY_TASK: t_authority"
        )

    def test_later_owner_with_terminal_parent_is_blocked_before_promotion(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body()
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_first","ready",None,None,None,None,"Implement #2182",body,now),
                ("t_later","todo",None,None,None,None,"Implementation: IME cleanup",body,now+1),
                ("t_parent","done",None,None,None,None,"Published issue","",now-1),
            ]
        )
        c.execute("insert into task_links values('t_parent','t_later')")
        c.commit(); c.close()
        findings=m.detect_kanban(self.root,now+100)
        finding=[f for f in findings if f.kind=="kanban_duplicate_implementation_owner"][0]
        self.assertEqual(finding.subject,"t_later")
        self.assertEqual(finding.repair,"block_dependency")
        self.assertFalse(any(f.kind=="kanban_dependencies_satisfied" and f.subject=="t_later"
                             for f in findings))
        calls=[]
        def runner(cmd,**kw):
            calls.append(cmd)
            return type("R",(),{"returncode":0,"stdout":"ok","stderr":""})()
        ok,_=m.mutate(finding,Path("/hermes"),runner)
        self.assertTrue(ok)
        self.assertEqual(calls[0][2:6],["block","t_later","--kind","dependency"])

    def test_serialized_implementation_followup_is_not_a_duplicate_owner(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body("github:example/project#7")
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_first","ready",None,None,None,None,"Implement #7",body,now),
                ("t_follow","todo",None,None,None,None,"Implementation follow-up #7",body,now+1),
            ]
        )
        c.execute("insert into task_links values('t_first','t_follow')")
        c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_duplicate_implementation_owner"
                             for f in m.detect_kanban(self.root,now+100)))

    def test_blocked_started_owner_retains_lease(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body("github:example/project#8")
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_blocked","blocked",now,None,None,None,"Implement #8",body,now),
                ("t_later","ready",None,None,None,None,"Implementation: #8",body,now+1),
            ]
        )
        c.commit(); c.close()
        finding=[f for f in m._implementation_owner_findings(self.root)
                 if f.kind=="kanban_duplicate_implementation_owner"][0]
        self.assertEqual(finding.subject,"t_later")
        self.assertIn("t_blocked",finding.detail)

    def test_running_later_writer_is_preserved_and_earlier_ready_is_blocked(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body("github:example/project#9")
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_earlier","ready",None,None,None,None,"Implement #9",body,now),
                ("t_running","running",now+1,999999,None,None,"Implementation: #9",body,now+1),
            ]
        )
        c.commit(); c.close()
        finding=[f for f in m._implementation_owner_findings(self.root)
                 if f.kind=="kanban_duplicate_implementation_owner"][0]
        self.assertEqual(finding.subject,"t_earlier")
        self.assertIn("t_running",finding.detail)

    def test_future_writer_without_authority_is_blocked(self):
        self._install_implementation_owner_schema()
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.execute(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)",
            ("t_unauthorized","ready",None,None,None,None,"Implement #10",
             "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing",now),
        )
        c.commit(); c.close()
        finding=[f for f in m._implementation_owner_findings(self.root)
                 if f.kind=="kanban_missing_implementation_authority"][0]
        self.assertEqual(finding.subject,"t_unauthorized")
        self.assertEqual(finding.repair,"block_dependency")

    def test_duplicate_owner_is_detected_across_boards(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body("github:example/project#11")
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.execute(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)",
            ("t_first","ready",None,None,None,None,"Implement #11",body,now),
        )
        c.commit(); c.close()
        db=self.root/"kanban/boards/other/kanban.db"; db.parent.mkdir(parents=True)
        c=sqlite3.connect(db)
        c.execute("create table tasks(id text,title text,body text,status text,created_at integer,started_at integer)")
        c.execute("create table task_links(parent_id text,child_id text)")
        c.execute("insert into tasks values(?,?,?,?,?,?)",
                  ("t_other","Implementation: #11",body,"todo",now+1,None))
        c.commit(); c.close()
        finding=[f for f in m._implementation_owner_findings(self.root)
                 if f.kind=="kanban_duplicate_implementation_owner"][0]
        self.assertEqual((finding.board,finding.subject),("other","t_other"))

    def test_authority_grants_are_exact_casefolded_and_not_issue_only(self):
        self._install_implementation_owner_schema()
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_exact","done",None,None,None,None,"Select owner",
                 "IMPLEMENTATION_AUTHORITY_GRANTED: github:Org/Repo#7",now),
                ("t_long","done",None,None,None,None,"Select owner",
                 "IMPLEMENTATION_AUTHORITY_GRANTED: github:Org/Repo#70",now),
                ("t_issue_only","done",None,None,None,None,"Publish issue",
                 "ISSUE_ONLY_WORKFLOW\nIMPLEMENTATION_AUTHORITY_GRANTED: github:Org/Repo#8",now),
                ("t_valid","ready",None,None,None,None,"Implementation: #7",
                 "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing\nIMPLEMENTATION_OWNER_KEY: github:ORG/REPO#7\nIMPLEMENTATION_AUTHORITY_TASK: t_exact",now+1),
                ("t_prefix","ready",None,None,None,None,"Implementation: prefix",
                 "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing\nIMPLEMENTATION_OWNER_KEY: github:org/repo#7\nIMPLEMENTATION_AUTHORITY_TASK: t_long",now+2),
                ("t_bad_issue","ready",None,None,None,None,"Implementation: issue-only grant",
                 "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing\nIMPLEMENTATION_OWNER_KEY: github:org/repo#8\nIMPLEMENTATION_AUTHORITY_TASK: t_issue_only",now+3),
            ]
        )
        c.commit(); c.close()
        findings=m._implementation_owner_findings(self.root)
        missing={f.subject for f in findings if f.kind=="kanban_missing_implementation_authority"}
        self.assertNotIn("t_valid",missing)
        self.assertEqual(missing,{"t_prefix","t_bad_issue"})

    def test_running_unauthorized_writer_is_observe_only(self):
        self._install_implementation_owner_schema()
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.execute(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)",
            ("t_running_bad","running",now,999999,None,None,"Implement #12",
             "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing",now),
        )
        c.commit(); c.close()
        finding=[f for f in m._implementation_owner_findings(self.root)
                 if f.subject=="t_running_bad"][0]
        self.assertEqual(finding.kind,"kanban_running_without_implementation_authority")
        self.assertIsNone(finding.repair)

    def test_done_owner_blocks_second_graph_and_ready_linked_child(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body("github:example/project#13")
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_done","done",now,None,None,None,"Implement #13",body,now),
                ("t_second","ready",None,None,None,None,"Implementation: second graph",body,now+1),
                ("t_ready_repair","ready",None,None,None,None,"Implementation: linked repair",body,now+2),
                ("t_todo_repair","todo",None,None,None,None,"Implementation: queued repair",body,now+3),
            ]
        )
        c.execute("insert into task_links values('t_done','t_ready_repair')")
        c.execute("insert into task_links values('t_done','t_todo_repair')")
        c.commit(); c.close()
        blocked={f.subject for f in m._implementation_owner_findings(self.root)
                 if f.kind=="kanban_duplicate_implementation_owner"}
        self.assertEqual(blocked,{"t_second","t_todo_repair"})
        self.assertNotIn("t_ready_repair",blocked)

    def test_todo_serialized_repair_promotes_then_remains_canonical(self):
        self._install_implementation_owner_schema()
        body=self._authority_and_body("github:example/project#14")
        c=sqlite3.connect(self.root/"kanban.db")
        now=m.IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH+10
        c.executemany(
            "insert into tasks(id,status,started_at,worker_pid,last_heartbeat_at,claim_expires,title,body,created_at) "
            "values(?,?,?,?,?,?,?,?,?)", [
                ("t_done","done",now,None,None,None,"Implement #14",body,now),
                ("t_repair","todo",None,None,None,None,"Repair #14",body,now+1),
            ]
        )
        c.execute("insert into task_links values('t_done','t_repair')")
        c.commit(); c.close()
        findings=m.detect_kanban(self.root,now+100)
        self.assertTrue(any(f.kind=="kanban_dependencies_satisfied" and f.subject=="t_repair"
                            for f in findings))
        self.assertFalse(any(f.kind=="kanban_duplicate_implementation_owner" and f.subject=="t_repair"
                             for f in findings))
        c=sqlite3.connect(self.root/"kanban.db")
        c.execute("update tasks set status='ready' where id='t_repair'")
        c.commit(); c.close()
        self.assertFalse(any(f.kind=="kanban_duplicate_implementation_owner" and f.subject=="t_repair"
                             for f in m._implementation_owner_findings(self.root)))

if __name__ == '__main__': unittest.main()
