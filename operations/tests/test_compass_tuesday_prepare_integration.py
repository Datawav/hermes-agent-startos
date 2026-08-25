import importlib.util
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


PATH = Path(__file__).parents[1] / "scripts/compass_tuesday_prepare.py"
SPEC = importlib.util.spec_from_file_location("compass_tuesday_prepare", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


class TuesdayPrepareTests(unittest.TestCase):
    def test_previous_publication_accepts_current_json_proofs(self):
        with tempfile.TemporaryDirectory() as root:
            repo = Path(root)
            published = repo / "data/newsletter_workspace/published"
            published.mkdir(parents=True)
            for kind in (30023, 1):
                (published / f"2026-08-05_{kind}.json").write_text(
                    json.dumps({"kind": kind, "id": f"id-{kind}", "sig": f"sig-{kind}"}),
                    encoding="utf-8",
                )
            with patch.object(
                MODULE,
                "run",
                side_effect=["content/en/newsletters/2026-08-05-newsletter.md", "[]"],
            ):
                self.assertEqual(
                    MODULE.verify_previous_publication(repo, repo / "worktrees"),
                    "2026-08-05",
                )

    def test_previous_publication_requires_both_json_proofs(self):
        with tempfile.TemporaryDirectory() as root:
            repo = Path(root)
            published = repo / "data/newsletter_workspace/published"
            published.mkdir(parents=True)
            (published / "2026-08-05_30023.json").write_text(
                json.dumps({"kind": 30023, "id": "id", "sig": "sig"}), encoding="utf-8"
            )
            with patch.object(
                MODULE,
                "run",
                return_value="content/en/newsletters/2026-08-05-newsletter.md",
            ):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    MODULE.verify_previous_publication(repo, repo / "worktrees")

    def test_created_tasks_use_goal_loop(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.object(MODULE, "run", return_value='{"id":"t_test"}') as mocked_run:
                task = MODULE.create_task(
                    "hermes", "Pipeline", "body", "/tmp/workspace", "key", "10h",
                    board_db=Path(root) / "kanban.db",
                )

        self.assertEqual(task["id"], "t_test")
        args = mocked_run.call_args.args[0]
        self.assertIn("--goal", args)
        self.assertEqual(args[args.index("--goal-max-turns") + 1], "50")
        self.assertEqual(args[args.index("--priority") + 1], "10000")
        self.assertEqual(args[args.index("--skill") + 1], "compass-newsletter")

    def test_archived_idempotency_key_never_mints_second_pipeline(self):
        with tempfile.TemporaryDirectory() as root:
            db = Path(root) / "kanban.db"
            connection = sqlite3.connect(db)
            connection.execute("create table tasks(id text,status text,idempotency_key text)")
            connection.execute(
                "insert into tasks values(?,?,?)",
                ("t_old", "archived", "compass-pipeline-2026-08-05"),
            )
            connection.commit()
            connection.close()
            with patch.object(MODULE, "run") as mocked_run:
                with self.assertRaisesRegex(RuntimeError, "archived idempotency key"):
                    MODULE.create_task(
                        "hermes", "Compass Newsletter Pipeline 2026-08-05", "body",
                        "/tmp/workspace", "compass-pipeline-2026-08-05", "5h",
                        board_db=db,
                    )
            mocked_run.assert_not_called()

    def test_geminiwriter_goal_keeps_sol_as_top_level_owner(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.object(MODULE, "run", return_value='{"id":"t_writing"}') as mocked_run:
                task = MODULE.create_task(
                    "hermes", "Compass Section Writing 2026-08-26", "body", "/tmp/workspace",
                    "compass-writing-2026-08-26", "5h", parent="t_research",
                    assignee="geminiwriter", board_db=Path(root) / "kanban.db",
                )

        self.assertEqual(task["id"], "t_writing")
        args = mocked_run.call_args.args[0]
        self.assertEqual(args[args.index("--assignee") + 1], "geminiwriter")
        self.assertNotIn("--model", args)
        self.assertNotIn("--provider", args)

    def test_dispatch_resumes_after_idempotent_research_result(self):
        calls = []

        def fake_create(*args, **kwargs):
            calls.append((args, kwargs))
            return {"id": f"t_{len(calls)}"}

        with patch.object(MODULE, "create_task", side_effect=fake_create):
            graph = MODULE.dispatch_graph(
                "hermes", "2026-08-26", "/tmp/workspace", ["research", "writing", "pipeline", "translation", "podcast"]
            )

        self.assertEqual(list(graph), ["research", "writing", "pipeline", "translation", "podcast"])
        self.assertEqual(len(calls), 5)
        writing_args, writing_kwargs = calls[1]
        self.assertEqual(writing_args[6], "t_1")
        self.assertEqual(writing_kwargs, {"assignee": "geminiwriter"})
        pipeline_args, pipeline_kwargs = calls[2]
        self.assertEqual(pipeline_args[6], "t_2")
        self.assertEqual(
            pipeline_kwargs,
            {"model": "gpt-5.6-sol", "provider": "openai-codex"},
        )

    def test_tuesday_targets_following_wednesday(self):
        now = datetime(2026, 8, 4, 14, tzinfo=timezone.utc)
        self.assertEqual(MODULE.next_wednesday(now), "2026-08-05")

    def test_wednesday_targets_next_week(self):
        now = datetime(2026, 8, 5, 14, tzinfo=timezone.utc)
        self.assertEqual(MODULE.next_wednesday(now), "2026-08-12")

    def test_queue_acknowledgement_is_recoverable_and_idempotent(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            repo = root / "repo"
            workspace = root / "workspace"
            archive = root / "archive"
            source = repo / "data/newsletter_workspace/link_queue.md"
            copy = workspace / "data/newsletter_workspace/link_queue.md"
            source.parent.mkdir(parents=True)
            copy.parent.mkdir(parents=True)
            source.write_text("## queued\n", encoding="utf-8")
            copy.write_text("## queued\n", encoding="utf-8")

            self.assertTrue(MODULE.acknowledge_shared_queue(workspace, "2026-08-05", repo, archive))
            self.assertFalse(source.exists())
            self.assertEqual((archive / "link_queue_2026-08-05.md").read_text(), "## queued\n")
            self.assertFalse(MODULE.acknowledge_shared_queue(workspace, "2026-08-05", repo, archive))

    def test_queue_acknowledgement_rejects_changed_input(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            repo = root / "repo"
            workspace = root / "workspace"
            source = repo / "data/newsletter_workspace/link_queue.md"
            copy = workspace / "data/newsletter_workspace/link_queue.md"
            source.parent.mkdir(parents=True)
            copy.parent.mkdir(parents=True)
            source.write_text("new input\n", encoding="utf-8")
            copy.write_text("old input\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "changed"):
                MODULE.acknowledge_shared_queue(workspace, "2026-08-05", repo, root / "archive")


if __name__ == "__main__":
    unittest.main()
