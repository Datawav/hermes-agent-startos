#!/usr/bin/env python3
"""Conservative self-healing for Hermes Kanban and cron workflows.

Detection reads durable stores. Mutations use official Hermes recovery/CLI
surfaces and only apply when the evidence proves an owner process is gone.
Healthy runs are silent; every decision is appended to a JSONL audit trail.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path("/opt/data")
HERMES = ROOT / ".local/bin/hermes"
STATE = ROOT / "cron/state/hermes_workflow_reconciler.json"
AUDIT = ROOT / "cron/audit/hermes_workflow_reconciler.jsonl"
LOCK = ROOT / "cron/state/hermes_workflow_reconciler.lock"
GHOST_GRACE = 900
WORKER_CHURN_WINDOW = 900
WORKER_CHURN_LIMIT = 4
CRON_CLAIM_GRACE = 900
TICK_GRACE = 180
MAX_REPAIRS = 3
COOLDOWN = 3600
RETRY_RECOVERY_COOLDOWN = 120
COMPASS_STAGE_RE = re.compile(
    r"^compass-(research|writing|pipeline|translate|podcast)-(\d{4}-\d{2}-\d{2})$"
)
COMPASS_REQUIRED_STAGES = frozenset({"research", "writing", "pipeline", "translate", "podcast"})
COMPASS_FOCUS_REASON = "paused by cross-board focus control"
SILENT_OBSERVE_KINDS = frozenset({"compass_weekly_outcome_duplicate"})
CIRCUIT_LIMIT = 3
MAX_RETRY_BACKOFF = 6 * 3600
REQUIRED = {
    "kanban-ghost-watchdog": "kanban_ghost_watchdog.py",
    "kanban-continuation-bridge": "kanban_continuation_bridge.py",
    "safeguard-health-watchdog": "safeguard_health_watchdog.py",
    "hermes-workflow-reconciler": "hermes_workflow_reconciler.py",
}
COMPLETION_PROOF_MARKERS = (
    "automated verification passed; no user decision is required",
    "independent verification is complete; no user decision is required",
    "focused verification passed. continue automated",
)
GITHUB_CAPABILITY_MARKERS = (
    "no gh auth",
    "gh auth status`: not logged in",
    "no github credentials",
    "github authentication is unavailable",
    "github api returns 401",
)
CIRCUIT_BREAKER_MARKERS = (
    "dispatcher circuit breaker stopped retries",
    "active-session-limit worker failures",
)
RETRYABLE_WORKER_ERRORS = (
    "not alive", "active-session-limit", "worker process", "spawn failed",
    "spawn_failed", "timed out", "timeout", "connection reset",
    "not inside a git repo", "does not point at a git repo root",
)

_PREPARE_PATH = Path(__file__).with_name("compass_tuesday_prepare.py")
_PREPARE_SPEC = importlib.util.spec_from_file_location(
    "_workflow_reconciler_compass_tuesday_prepare", _PREPARE_PATH
)
if _PREPARE_SPEC is None or _PREPARE_SPEC.loader is None:
    raise RuntimeError(f"cannot load Compass target resolver from {_PREPARE_PATH}")
_PREPARE_MODULE = importlib.util.module_from_spec(_PREPARE_SPEC)
_PREPARE_SPEC.loader.exec_module(_PREPARE_MODULE)
compass_prepare_current_working_target = _PREPARE_MODULE.current_working_target


def compass_current_working_target(root: Path) -> str | None:
    """Resolve the target from the same origin/main source used by Tuesday intake."""
    return compass_prepare_current_working_target(root / "compass")


@dataclass(frozen=True)
class Finding:
    kind: str
    subject: str
    board: str = "default"
    repair: str | None = None
    detail: str = ""

    @property
    def fingerprint(self) -> str:
        # Detail may contain changing age/error context; incident identity must
        # remain stable so cooldowns and circuit breakers cannot be bypassed.
        raw = f"{self.kind}\0{self.board}\0{self.subject}"
        return hashlib.sha256(raw.encode()).hexdigest()[:20]


def load_json(path: Path, default: dict) -> dict:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def boards(root: Path) -> list[tuple[str, Path]]:
    found = [("default", root / "kanban.db")]
    found += [(p.parent.name, p) for p in sorted((root / "kanban/boards").glob("*/kanban.db"))]
    return [(name, path) for name, path in found if path.is_file()]


def github_auth_available(root: Path) -> bool:
    gh = root / "bin/gh"
    if not gh.is_file():
        return False
    try:
        result = subprocess.run(
            [str(gh), "auth", "status"], capture_output=True, text=True,
            timeout=15, check=False,
        )
        return result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def unattended_commands_available(root: Path) -> bool:
    try:
        text = (root / "config.yaml").read_text()
    except OSError:
        return False
    return bool(re.search(r"(?m)^\s*hooks_auto_accept:\s*true\s*$", text))


def dispatcher_is_healthy(root: Path) -> bool:
    snapshot = load_json(root / "kanban/capacity-scheduler-snapshot.json", {})
    if snapshot:
        return snapshot.get("mode") == "healthy" and int(snapshot.get("target", 0)) > 0
    try:
        text = (root / ".hermes/state/kanban_adaptive_capacity.txt").read_text()
    except OSError:
        return False
    return "pressure=healthy" in text and "mode=high-performance" in text


def latest_block_reason(conn: sqlite3.Connection, task_id: str, *, allow_comments: bool = True) -> str:
    row = conn.execute(
        "SELECT payload FROM task_events WHERE task_id=? AND kind='blocked' "
        "ORDER BY id DESC LIMIT 1", (task_id,),
    ).fetchone()
    if row:
        try:
            return str(json.loads(row[0] or "{}").get("reason") or "").strip()
        except (json.JSONDecodeError, TypeError):
            pass
    if not allow_comments:
        return ""
    try:
        row = conn.execute(
            "SELECT body FROM task_comments WHERE task_id=? ORDER BY id DESC LIMIT 1",
            (task_id,),
        ).fetchone()
    except sqlite3.Error:
        return ""
    return str(row[0] or "").strip() if row else ""


def _pid_exists(pid: int | None) -> bool:
    """Return positive liveness only when this namespace can see the PID."""
    if not pid:
        return False
    try:
        return (Path("/proc") / str(int(pid)) / "stat").is_file()
    except (OSError, ValueError):
        return False


def _dependency_cycles(conn: sqlite3.Connection) -> set[str]:
    """Return task ids participating in a dependency cycle."""
    graph: dict[str, list[str]] = {}
    for parent, child in conn.execute("SELECT parent_id,child_id FROM task_links"):
        graph.setdefault(str(parent), []).append(str(child))
    visiting: set[str] = set()
    visited: set[str] = set()
    cycles: set[str] = set()

    def walk(node: str, path: list[str]) -> None:
        if node in visiting:
            try:
                cycles.update(path[path.index(node):])
            except ValueError:
                cycles.add(node)
            return
        if node in visited:
            return
        visiting.add(node)
        path.append(node)
        for child in graph.get(node, []):
            walk(child, path)
        path.pop()
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        walk(node, [])
    return cycles


OWNER_MARKER_RE = re.compile(r"(?m)^IMPLEMENTATION_OWNER_KEY:\s*(\S+)\s*$")
AUTHORITY_TASK_RE = re.compile(r"(?m)^IMPLEMENTATION_AUTHORITY_TASK:\s*(\S+)\s*$")
AUTHORITY_GRANT_RE = re.compile(r"(?m)^IMPLEMENTATION_AUTHORITY_GRANTED:\s*(\S+)\s*$")
IMPLEMENTATION_TITLE_RE = re.compile(
    r"^\s*(?:Implement|Implementation|Fix|Repair)(?:\s|:|#)", re.I
)
IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH = 1787297000


def _normalize_owner_key(value: str) -> str:
    return unicodedata.normalize("NFC", value.strip()).casefold()


def _implementation_owner_key(body: str) -> str | None:
    """Return only an explicit owner key; incidental issue URLs never grant authority."""
    match = OWNER_MARKER_RE.search(body)
    return _normalize_owner_key(match.group(1)) if match else None


def _authority_task_id(body: str) -> str | None:
    match = AUTHORITY_TASK_RE.search(body)
    return match.group(1) if match else None


def _authority_granted(root: Path, task_id: str, owner_key: str) -> bool:
    for _board, db in boards(root):
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            columns = {item[1] for item in conn.execute("PRAGMA table_info(tasks)")}
            if not {"id", "body", "status"}.issubset(columns):
                continue
            result_expr = "result" if "result" in columns else "''"
            row = conn.execute(
                f"SELECT body,{result_expr},status FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if not row or row[2] not in ("done", "archived"):
                continue
            text = f"{row[0] or ''}\n{row[1] or ''}"
            try:
                text += "\n" + "\n".join(
                    str(comment[0] or "") for comment in conn.execute(
                        "SELECT body FROM task_comments WHERE task_id=?", (task_id,)
                    )
                )
            except sqlite3.Error:
                pass
            grants = {_normalize_owner_key(value) for value in AUTHORITY_GRANT_RE.findall(text)}
            return owner_key in grants and "ISSUE_ONLY_WORKFLOW" not in text
        finally:
            conn.close()
    return False


def _linked(edges: dict[tuple[str, str], set[str]], board: str, left: str, right: str) -> bool:
    def reaches(start: str, target: str) -> bool:
        pending = [start]; seen: set[str] = set()
        while pending:
            node = pending.pop()
            if node in seen:
                continue
            seen.add(node)
            children = edges.get((board, node), set())
            if target in children:
                return True
            pending.extend(children)
        return False
    return reaches(left, right) or reaches(right, left)


def _implementation_owner_findings(root: Path) -> list[Finding]:
    """Enforce explicit authority and one owner key across every Kanban board."""
    active: list[dict] = []
    edges: dict[tuple[str, str], set[str]] = {}
    for board, db in boards(root):
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            columns = {item[1] for item in conn.execute("PRAGMA table_info(tasks)")}
            required = {"id", "title", "body", "status", "created_at"}
            if not required.issubset(columns):
                continue
            started_expr = "started_at" if "started_at" in columns else "NULL AS started_at"
            for row in conn.execute(
                f"SELECT id,title,body,status,created_at,{started_expr} FROM tasks "
                "WHERE status IN ('todo','ready','running','blocked','done') ORDER BY created_at,id"
            ):
                item = dict(row); item["board"] = board; active.append(item)
            tables = {item[0] for item in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            if "task_links" in tables:
                for parent, child in conn.execute("SELECT parent_id,child_id FROM task_links"):
                    edges.setdefault((board, str(parent)), set()).add(str(child))
        finally:
            conn.close()

    findings: list[Finding] = []
    groups: dict[str, list[dict]] = {}
    for row in active:
        body = str(row.get("body") or "")
        title = str(row.get("title") or "")
        if "ISSUE_ONLY_WORKFLOW" in body:
            continue
        key = _implementation_owner_key(body)
        authority_task = _authority_task_id(body)
        looks_like_writer = bool(
            IMPLEMENTATION_TITLE_RE.match(title)
            or "IMPLEMENTATION_MUTATION_BOUNDARY: code-writing" in body
        )
        created = int(row.get("created_at") or 0)
        authorized = bool(
            key and authority_task and _authority_granted(root, authority_task, key)
        )
        if looks_like_writer and created >= IMPLEMENTATION_OWNERSHIP_POLICY_EPOCH and not authorized:
            if row["status"] in ("todo", "ready"):
                findings.append(Finding(
                    "kanban_missing_implementation_authority",
                    str(row["id"]), str(row["board"]), "block_dependency",
                    "code-writing card lacks a separate completed owner-selection record for its exact owner key",
                ))
            elif row["status"] == "running":
                findings.append(Finding(
                    "kanban_running_without_implementation_authority",
                    str(row["id"]), str(row["board"]),
                    detail="running writer crossed the boundary without a valid authority record; do not kill or replay",
                ))
            continue
        if authorized and key:
            groups.setdefault(key, []).append(row)

    for key, candidates in groups.items():
        if len(candidates) < 2:
            continue
        def rank(row: dict) -> tuple[int, int, str]:
            if row["status"] == "running":
                tier = 0
            elif row["status"] == "blocked" and row.get("started_at"):
                tier = 1
            elif row["status"] == "done":
                tier = 2
            elif row["status"] in ("ready", "todo"):
                tier = 3
            else:
                tier = 4
            return tier, int(row.get("created_at") or 0), str(row["id"])
        done_anchors = sorted(
            [row for row in candidates if row["status"] == "done"],
            key=lambda row: (int(row.get("created_at") or 0), str(row["id"])),
        )
        running = [row for row in candidates if row["status"] == "running"]
        started_blocked = [
            row for row in candidates
            if row["status"] == "blocked" and row.get("started_at")
        ]
        lineage_successors = [
            row for row in candidates
            if row["status"] != "done" and any(
                row["board"] == anchor["board"] and _linked(
                    edges, str(row["board"]), str(anchor["id"]), str(row["id"])
                )
                for anchor in done_anchors
            )
        ]
        pool = running or started_blocked or lineage_successors or candidates
        canonical = sorted(pool, key=rank)[0]
        for row in candidates:
            if row["id"] == canonical["id"] or row["status"] == "done":
                continue
            if row["status"] == "running":
                findings.append(Finding(
                    "kanban_duplicate_running_implementation_owner",
                    str(row["id"]), str(row["board"]),
                    detail=f"ownership key {key} also has running owner {canonical['id']}; outcome uncertain",
                ))
                continue
            if row["status"] == "todo" and row["board"] == canonical["board"] and _linked(
                edges, str(row["board"]), str(canonical["id"]), str(row["id"])
            ):
                continue
            if row["status"] in ("todo", "ready"):
                findings.append(Finding(
                    "kanban_duplicate_implementation_owner",
                    str(row["id"]), str(row["board"]), "block_dependency",
                    f"ownership key {key} is already held by {canonical['id']} on "
                    f"{canonical['board']}; reconcile that owner before code, branch, PR, review, build, or delivery",
                ))
    return findings


def detect_kanban(root: Path, now: int) -> list[Finding]:
    owner_findings = _implementation_owner_findings(root)
    findings: list[Finding] = list(owner_findings)
    duplicate_owner_ids_by_board: dict[str, set[str]] = {}
    for item in owner_findings:
        if item.repair == "block_dependency":
            duplicate_owner_ids_by_board.setdefault(item.board, set()).add(item.subject)
    github_ready = github_auth_available(root)
    commands_ready = unattended_commands_available(root)
    capacity_ready = dispatcher_is_healthy(root)
    lifecycle = load_json(root / "state/gateway.lifecycle.json", {})
    heartbeat = root / "state/gateway.heartbeat"
    gateway_ready = bool(
        lifecycle.get("phase") == "running"
        and heartbeat.is_file()
        and now - int(heartbeat.stat().st_mtime) <= 180
    )
    compass_recovery_ready = capacity_ready and gateway_ready
    for board, db in boards(root):
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                findings.append(Finding("kanban_integrity", str(db), board, detail="quick_check failed"))
                conn.close(); continue
            event_columns = {item[1] for item in conn.execute("PRAGMA table_info(task_events)")}
            if "created_at" in event_columns:
                churn_counts: dict[str, int] = {}
                for row in conn.execute(
                    "SELECT e.task_id,e.created_at FROM task_events e JOIN tasks t ON t.id=e.task_id "
                    "WHERE t.status='ready' AND e.kind IN ('reclaimed','crashed','spawn_failed')"
                ):
                    try:
                        stamp = int(row["created_at"])
                    except (TypeError, ValueError):
                        continue
                    if now - WORKER_CHURN_WINDOW <= stamp <= now:
                        task_id = str(row["task_id"])
                        churn_counts[task_id] = churn_counts.get(task_id, 0) + 1
                for task_id, attempts in churn_counts.items():
                    if attempts < WORKER_CHURN_LIMIT:
                        continue
                    findings.append(Finding(
                        "kanban_worker_churn_circuit",
                        task_id,
                        board,
                        "block_transient",
                        f"{attempts} crash/reclaim events in the last "
                        f"{WORKER_CHURN_WINDOW // 60} minutes; pause respawn churn for bounded retry",
                    ))
            rows = conn.execute("SELECT id,started_at,worker_pid,last_heartbeat_at,claim_expires "
                                "FROM tasks WHERE status='running'").fetchall()
            for row in rows:
                try:
                    started = int(row["started_at"] or now)
                    claim_expires = int(row["claim_expires"]) if row["claim_expires"] is not None else None
                    heartbeat = int(row["last_heartbeat_at"]) if row["last_heartbeat_at"] is not None else None
                except (TypeError, ValueError):
                    continue
                expired = claim_expires is not None and claim_expires < now
                if (row["worker_pid"] is None and heartbeat is None
                        and expired and now - started >= GHOST_GRACE):
                    findings.append(Finding("kanban_never_started", row["id"], board,
                                            "reclaim", f"age={now-started}s"))
                elif (row["worker_pid"] is not None and expired
                      and (heartbeat is None or now - heartbeat >= GHOST_GRACE)
                      and not _pid_exists(row["worker_pid"])):
                    findings.append(Finding(
                        "kanban_dead_worker_unverified", row["id"], board,
                        detail=f"pid={row['worker_pid']} is not visible in this namespace; "
                               "claim and heartbeat are expired, but owner death is unproven",
                    ))
            task_columns = {item[1] for item in conn.execute("PRAGMA table_info(tasks)")}
            table_names = {
                item[0] for item in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if board == "compass" and "task_links" in table_names:
                compass_rows = conn.execute(
                    "SELECT id,status,priority,idempotency_key,block_kind FROM tasks "
                    "WHERE idempotency_key LIKE 'compass-%'"
                ).fetchall()
                by_target: dict[str, dict[str, sqlite3.Row]] = {}
                for task in compass_rows:
                    match = COMPASS_STAGE_RE.fullmatch(str(task["idempotency_key"] or ""))
                    if not match:
                        continue
                    stages = by_target.setdefault(match.group(2), {})
                    prior = stages.get(match.group(1))
                    # Prefer the live incarnation when a historical bug left an
                    # archived duplicate with the same idempotency key.
                    if prior is None or (
                        prior["status"] == "archived" and task["status"] != "archived"
                    ):
                        stages[match.group(1)] = task

                current_target = compass_current_working_target(root)
                live_by_target: dict[str, dict[str, sqlite3.Row]] = {}
                for target, stages in by_target.items():
                    live_stages = {
                        stage: task for stage, task in stages.items()
                        if task["status"] != "archived"
                    }
                    live_by_target[target] = live_stages
                    missing = COMPASS_REQUIRED_STAGES - set(live_stages)
                    terminal_legacy_week = all(
                        stage in stages and stages[stage]["status"] in ("done", "completed", "archived")
                        for stage in ("pipeline", "translate", "podcast")
                    )
                    if target == current_target and missing and not terminal_legacy_week:
                        findings.append(Finding(
                            "compass_weekly_graph_incomplete", target, board,
                            "reconcile_compass_weekly",
                            "missing live idempotent weekly stages: " + ", ".join(sorted(missing)),
                        ))

                for target, stages in live_by_target.items():
                    if target != current_target:
                        continue
                    for stage, task in stages.items():
                        task_id = str(task["id"])
                        if int(task["priority"] or 0) != 10000:
                            continue
                        parents_done = conn.execute(
                            "SELECT COUNT(*), SUM(CASE WHEN p.status IN ('done','completed','archived') "
                            "THEN 0 ELSE 1 END) FROM task_links l JOIN tasks p ON p.id=l.parent_id "
                            "WHERE l.child_id=?", (task_id,),
                        ).fetchone()
                        if not parents_done or int(parents_done[0] or 0) == 0 or int(parents_done[1] or 0) != 0:
                            continue
                        if task["status"] == "scheduled" and compass_recovery_ready:
                            scheduled = conn.execute(
                                "SELECT payload FROM task_events WHERE task_id=? AND kind='scheduled' "
                                "ORDER BY id DESC LIMIT 1", (task_id,),
                            ).fetchone()
                            try:
                                reason = str(json.loads(scheduled[0] or "{}").get("reason") or "").lower() if scheduled else ""
                            except (json.JSONDecodeError, TypeError):
                                reason = ""
                            if COMPASS_FOCUS_REASON in reason:
                                findings.append(Finding(
                                    "compass_weekly_stage_focus_stall", task_id, board, "unblock",
                                    f"P0 {stage} stage for {target} remained focus-parked after its parents completed and capacity recovered",
                                ))
                        elif task["status"] == "blocked" and task["block_kind"] == "transient" and compass_recovery_ready:
                            gave_up = conn.execute(
                                "SELECT payload FROM task_events WHERE task_id=? AND kind='gave_up' "
                                "ORDER BY id DESC LIMIT 1", (task_id,),
                            ).fetchone()
                            try:
                                payload = json.loads(gave_up[0] or "{}") if gave_up else {}
                            except (json.JSONDecodeError, TypeError):
                                payload = {}
                            trigger = str(payload.get("trigger_outcome") or "").lower()
                            error = str(payload.get("error") or "").lower()
                            if trigger in {"crashed", "spawn_failed", "timed_out"} and any(
                                marker in error for marker in RETRYABLE_WORKER_ERRORS
                            ):
                                findings.append(Finding(
                                    "compass_weekly_stage_retry_stall", task_id, board, "unblock",
                                    f"P0 {stage} stage for {target} exhausted a transient worker retry while dispatcher health is restored",
                                ))

                if "task_comments" in table_names:
                    for task in compass_rows:
                        if task["status"] not in ("done", "archived"):
                            continue
                        count = conn.execute(
                            "SELECT COUNT(*) FROM task_comments WHERE task_id=? "
                            "AND body LIKE 'MARMOT_OUTCOME:%'", (task["id"],),
                        ).fetchone()[0]
                        if int(count or 0) > 1:
                            findings.append(Finding(
                                "compass_weekly_outcome_duplicate", str(task["id"]), board,
                                detail="multiple terminal MARMOT_OUTCOME markers exist; preserve them for delivery audit and never replay automatically",
                            ))
            if "task_comments" in table_names:
                completed_ready = conn.execute(
                    "SELECT t.id,c.body FROM tasks t JOIN task_comments c "
                    "ON c.id=(SELECT c2.id FROM task_comments c2 WHERE c2.task_id=t.id "
                    "AND c2.author IN ('default','operations') "
                    "AND c2.body LIKE '%HOST ROLLBACK COMPLETED%' "
                    "ORDER BY c2.id DESC LIMIT 1) WHERE t.status='ready'"
                ).fetchall()
                for ready in completed_ready:
                    if "HOST ROLLBACK COMPLETED" not in str(ready["body"] or ""):
                        continue
                    prior = latest_block_reason(conn, ready["id"]).lower()
                    if "host-root action required" in prior and "rollback" in prior:
                        findings.append(Finding(
                            "kanban_host_rollback_terminal_drift", ready["id"], board,
                            detail="a historical host rollback receipt conflicts with ready state; "
                                   "task-scoped reconciliation is required before terminalization",
                        ))
            blocked = (conn.execute(
                "SELECT id,block_kind FROM tasks WHERE status='blocked'"
            ).fetchall() if "block_kind" in task_columns else [])
            for row in blocked:
                diagnostic_reason = latest_block_reason(conn, row["id"])
                reason = latest_block_reason(conn, row["id"], allow_comments=False)
                # An empty owner gate cannot express a decision. Surface it as
                # malformed, but never infer permission to resume unknown work.
                if row["block_kind"] == "needs_input" and not diagnostic_reason:
                    findings.append(Finding(
                        "kanban_malformed_owner_gate", row["id"], board,
                        detail="needs_input had no observable durable reason; automatic unblock withheld",
                    ))
                    continue
                gave_up = (conn.execute(
                    "SELECT payload,created_at FROM task_events WHERE task_id=? "
                    "AND kind='gave_up' ORDER BY id DESC LIMIT 1", (row["id"],)
                ).fetchone() if "created_at" in event_columns else None)
                try:
                    gave_up_at = int(gave_up[1]) if gave_up and gave_up[1] is not None else None
                except (TypeError, ValueError):
                    gave_up_at = None
                if (gave_up is not None and gave_up_at is not None and 0 <= gave_up_at <= now
                        and now - gave_up_at <= 72 * 3600):
                    try:
                        payload = json.loads(gave_up[0] or "{}")
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                    error = str(payload.get("error") or "").lower()
                    trigger = str(payload.get("trigger_outcome") or "").lower()
                    if (row["block_kind"] == "transient"
                            and trigger in {"crashed", "spawn_failed", "timed_out"}
                            and any(marker in error for marker in RETRYABLE_WORKER_ERRORS)):
                        findings.append(Finding(
                            "kanban_worker_retry_exhausted", row["id"], board,
                            detail=f"automatic retry budget exhausted after {trigger}; "
                                   "dedicated transient-retry controller must prove recovery before unblocking",
                        ))
                        continue
                if row["block_kind"] != "capability":
                    continue
                low = reason.lower()
                if any(marker in low for marker in COMPLETION_PROOF_MARKERS):
                    findings.append(Finding(
                        "kanban_completed_work_parked", row["id"], board,
                        detail="block reason contains completion-like prose; structured completion proof is required",
                    ))
                elif github_ready and any(marker in low for marker in GITHUB_CAPABILITY_MARKERS):
                    findings.append(Finding(
                        "kanban_github_capability_restored", row["id"], board,
                        "unblock", "authenticated gh capability is now available",
                    ))
                elif commands_ready and "interactive terminal-consent" in low:
                    findings.append(Finding(
                        "kanban_command_capability_restored", row["id"], board,
                        "unblock", "unattended workers now accept configured command hooks",
                    ))
                elif capacity_ready and any(marker in low for marker in CIRCUIT_BREAKER_MARKERS):
                    findings.append(Finding(
                        "kanban_capacity_circuit_recovered", row["id"], board,
                        "unblock", "adaptive dispatcher is healthy; worker must reconcile prior side effects before retry",
                    ))
            duplicate_owner_ids = duplicate_owner_ids_by_board.get(board, set())
            if "task_links" in table_names:
                cycles = _dependency_cycles(conn)
                for task_id in sorted(cycles):
                    findings.append(Finding(
                        "kanban_dependency_cycle", task_id, board,
                        detail="dependency graph contains a cycle; automatic mutation withheld",
                    ))
                eligible = conn.execute(
                    "SELECT t.id, COUNT(l.parent_id) AS parent_count, "
                    "SUM(CASE WHEN p.id IS NULL THEN 1 ELSE 0 END) AS missing_count, "
                    "SUM(CASE WHEN p.status IN ('done','completed','archived') THEN 0 ELSE 1 END) AS open_count "
                    "FROM tasks t JOIN task_links l ON l.child_id=t.id "
                    "LEFT JOIN tasks p ON p.id=l.parent_id WHERE t.status='todo' "
                    "GROUP BY t.id"
                ).fetchall()
                for row in eligible:
                    if (str(row["id"]) not in cycles
                            and str(row["id"]) not in duplicate_owner_ids
                            and int(row["parent_count"] or 0) > 0
                            and int(row["missing_count"] or 0) == 0
                            and int(row["open_count"] or 0) == 0):
                        findings.append(Finding(
                            "kanban_dependencies_satisfied", row["id"], board, "promote",
                            "all recorded parent dependencies are terminal",
                        ))
            # A completion-protocol failure followed by repeated decomposition is
            # not evidence that the engineering work is incomplete.  It is a
            # bookkeeping incident and must never be "fixed" by blindly
            # unblocking the root (which caused the original retry loop).  Route
            # it to a human/safe reconciliation path as an observe-only finding.
            if _has_duplicate_decomposition_loop(conn):
                for task_id in _duplicate_decomposition_roots(conn):
                    status_row = conn.execute(
                        "SELECT status FROM tasks WHERE id=?", (task_id,)
                    ).fetchone()
                    # Terminal work, intentional schedules/holds, and active
                    # workers are not stalled completion loops. Only a root
                    # that is actually parked blocked needs reconciliation.
                    if not status_row or status_row[0] != "blocked":
                        continue
                    findings.append(Finding(
                        "kanban_completion_decomposition_loop",
                        task_id,
                        board,
                        detail="completion protocol failure followed by duplicate decomposition; reconcile evidence, do not unblock",
                    ))
            conn.close()
        except sqlite3.Error as exc:
            findings.append(Finding("kanban_unreadable", str(db), board, detail=str(exc)[:160]))
    return findings


def _duplicate_decomposition_roots(conn: sqlite3.Connection) -> list[str]:
    """Return roots with completion-protocol trouble and 2+ decompositions."""
    roots: list[str] = []
    task_ids = [r[0] for r in conn.execute(
        "SELECT task_id FROM task_events GROUP BY task_id "
        "HAVING SUM(kind='decomposed') >= 2"
    )]
    for task_id in task_ids:
        events = conn.execute(
            "SELECT kind, payload FROM task_events WHERE task_id=? ORDER BY id",
            (task_id,),
        ).fetchall()
        first_failure = next((i for i, (kind, payload) in enumerate(events)
                              if kind in {"protocol_violation", "block_loop_detected"}
                              and (kind == "protocol_violation" or
                                   "completion" in (payload or "").lower())), None)
        if first_failure is not None and sum(
                kind == "decomposed" for kind, _ in events[first_failure + 1:]) >= 2:
            roots.append(task_id)
    return roots


def _has_duplicate_decomposition_loop(conn: sqlite3.Connection) -> bool:
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    return "task_events" in tables and bool(_duplicate_decomposition_roots(conn))


def _epoch(value: str | None) -> float | None:
    if not value: return None
    try: return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError: return None


def _owner_matches(pid: int | None, expected_start: int | None) -> bool:
    """Return true only when /proc proves this exact scheduler owner is live."""
    if not pid or expected_start is None:
        return False
    try:
        raw = (Path("/proc") / str(int(pid)) / "stat").read_text()
        close = raw.rfind(")")
        if close < 0:
            return False
        # starttime is field 22 overall: token 20 after the closing comm field.
        tail = raw[close + 1:].split()
        return len(tail) > 19 and int(tail[19]) == int(expected_start)
    except (FileNotFoundError, PermissionError, OSError, ValueError):
        return False


def detect_cron(root: Path, now: int) -> list[Finding]:
    findings: list[Finding] = []
    jobs_path = root / "cron/jobs.json"
    jobs = load_json(jobs_path, {"jobs": []}).get("jobs", [])
    by_name = {j.get("name"): j for j in jobs}
    for name, script in REQUIRED.items():
        job = by_name.get(name)
        if not job:
            findings.append(Finding("required_cron_missing", name, detail=script))
        elif not job.get("enabled"):
            findings.append(Finding("required_cron_paused", job["id"], repair="resume", detail=name))
        if not (root / "scripts" / script).is_file():
            findings.append(Finding("required_script_missing", script))
    ticker = root / "cron/ticker_last_success"
    if not ticker.exists() or now - int(ticker.stat().st_mtime) > TICK_GRACE:
        findings.append(Finding("scheduler_ticker_stale", str(ticker), detail="host scheduler may be stalled"))
    db = root / "cron/executions.db"
    if db.is_file():
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True); conn.row_factory = sqlite3.Row
        # A ``claimed`` record is persisted before executor submission.  In
        # particular, workdir jobs share the scheduler's sequential executor,
        # so a healthy long-running job can leave many later attempts claimed
        # while they wait in that bounded queue.  No side effect has started for
        # those rows and treating their queue age as an uncertain execution
        # produced recurring false recovery alerts.  Only ``running`` has
        # crossed the durable start boundary and can have an uncertain outcome.
        for row in conn.execute("SELECT id,job_id,status,claimed_at,started_at,pid,process_started_at FROM executions "
                                "WHERE status='running'"):
            stamp = _epoch(row["started_at"] or row["claimed_at"])
            if stamp and now - stamp > CRON_CLAIM_GRACE:
                # Long-running jobs are not stale while the durable owner PID
                # and process start time still match. This is positive liveness
                # proof and prevents legitimate reviews/builds from generating
                # a recovery alert merely because they exceed fifteen minutes.
                if _owner_matches(row["pid"], row["process_started_at"]):
                    continue
                # Observe only.  This script runs outside the gateway's PID
                # namespace, so even an exact pid/start-time mismatch here is
                # not proof that the gateway-owned process exited.  In
                # particular, a live external fire may legitimately exceed the
                # claim TTL.  The gateway calls recover_interrupted_executions
                # in its own namespace on startup; that is the authoritative
                # place to prove owner death and mark the outcome unknown.
                findings.append(Finding("cron_execution_stale", row["id"],
                                        detail=f"job={row['job_id']} status={row['status']} outcome uncertain"))
        conn.close()
    return findings


def mutate(finding: Finding, hermes: Path, runner=subprocess.run,
           root: Path = ROOT) -> tuple[bool, str]:
    subject = str(finding.subject)
    if finding.repair == "reclaim":
        cmd = [str(hermes), "kanban"]
        if finding.board != "default": cmd += ["--board", finding.board]
        cmd += ["reclaim", subject, "--reason", "self-healer: expired claim, no PID, no heartbeat"]
    elif finding.repair == "resume":
        cmd = [str(hermes), "cron", "resume", subject]
    elif finding.repair in {"unblock", "complete"}:
        cmd = [str(hermes), "kanban"]
        if finding.board != "default": cmd += ["--board", finding.board]
        if finding.repair == "unblock":
            cmd += ["unblock", subject, "--reason", f"self-healer: {finding.detail}"]
        else:
            cmd += ["complete", subject, "--result",
                    "Reconciled as complete: durable verification was already recorded and no user decision remains."]
    elif finding.repair == "promote":
        cmd = [str(hermes), "kanban"]
        if finding.board != "default": cmd += ["--board", finding.board]
        cmd += ["promote", subject,
                f"self-healer: {finding.detail}", "--json"]
    elif finding.repair == "reconcile_compass_weekly":
        cmd = [sys.executable, str(root / "scripts/compass_tuesday_prepare.py"),
               "--reconcile-target", subject]
    elif finding.repair in {"block_transient", "block_dependency"}:
        cmd = [str(hermes), "kanban"]
        if finding.board != "default": cmd += ["--board", finding.board]
        kind = "transient" if finding.repair == "block_transient" else "dependency"
        cmd += ["block", subject, "--kind", kind,
                f"self-healer: {finding.detail}"]
    else:
        return False, "not automatable"
    env = os.environ.copy()
    env.pop("HERMES_KANBAN_DB", None)
    env.pop("HERMES_KANBAN_BOARD", None)
    result = runner(
        cmd, capture_output=True, text=True, timeout=60, check=False, env=env
    )
    return result.returncode == 0, (result.stdout or result.stderr).strip()[:300]


def reconcile(root: Path, now: int, state_path: Path, audit_path: Path,
              hermes: Path = HERMES, dry_run: bool = False,
              runner=subprocess.run) -> tuple[list[Finding], list[dict]]:
    findings = detect_kanban(root, now) + detect_cron(root, now)
    state = load_json(state_path, {"incidents": {}}); incidents = state.setdefault("incidents", {})
    actions: list[dict] = []; repairs = 0; active = set()
    for finding in findings:
        fp = finding.fingerprint; active.add(fp)
        new_incident = fp not in incidents
        incident = incidents.setdefault(fp, {"first_seen": now, "failures": 0})
        incident["last_seen"] = now; incident["finding"] = asdict(finding)
        action = "observe"; ok = None; output = ""
        base_cooldown = (RETRY_RECOVERY_COOLDOWN
                         if finding.kind == "kanban_worker_retry_exhausted" else COOLDOWN)
        # The circuit limits rapid retries, not eventual recovery. A previous
        # hard failure ceiling silently stranded tasks after three unsuccessful
        # repair calls. Keep probing forever with capped exponential backoff.
        failures = int(incident.get("failures", 0))
        cooldown = min(MAX_RETRY_BACKOFF, base_cooldown * (2 ** min(failures, CIRCUIT_LIMIT)))
        eligible = (finding.repair and repairs < MAX_REPAIRS
                    and now - int(incident.get("last_attempt", 0)) >= cooldown)
        if eligible:
            action = "dry_run" if dry_run else finding.repair
            if not dry_run:
                incident["last_attempt"] = now; repairs += 1
                ok, output = mutate(finding, hermes, runner, root)
                if ok: incident["repaired_at"] = now; incident["failures"] = 0
                else: incident["failures"] = int(incident.get("failures", 0)) + 1
        actions.append({"at": now, "fingerprint": fp, "action": action,
                        "ok": ok, "output": output, "new": new_incident,
                        "finding": asdict(finding)})
    state["incidents"] = {k: v for k, v in incidents.items() if k in active}
    state["updated_at"] = now
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    if actions:
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a") as stream:
            for action in actions: stream.write(json.dumps(action, sort_keys=True) + "\n")
    return findings, actions


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT); parser.add_argument("--now", type=int, default=int(time.time()))
    args = parser.parse_args()
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: return 0
        findings, actions = reconcile(args.root, args.now, STATE, AUDIT, dry_run=args.dry_run)
    failed = [a for a in actions if a["ok"] is False]
    # Observe-only findings are not execution failures: the cron completed its
    # detection job successfully. Announce each new incident once, then stay
    # silent while its fingerprint remains unchanged. Only a failed automatic
    # mutation makes this script exit non-zero.
    human_new = [
        a for a in actions
        if a["finding"]["repair"] is None and a["new"]
        and a["finding"]["kind"] not in SILENT_OBSERVE_KINDS
    ]
    if failed or human_new:
        names = ", ".join(sorted(
            {a["finding"]["kind"] for a in human_new}
            | {a["finding"]["kind"] for a in failed}
        ))
        print(f"⚠️ Hermes workflow recovery needs review: {names}. Automatic safe repairs were exhausted or intentionally withheld because execution outcome is uncertain.")
    if failed:
        return 1
    return 0


if __name__ == "__main__": raise SystemExit(main())
