#!/usr/bin/env python3
"""Prepare Compass's weekly worktree and dispatch its durable task graph."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import argparse
import json
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys


REPO = Path("/opt/data/compass")
INBOX_ARCHIVE = Path("/opt/data/compass-inbox-archive")
HELPER = Path("/opt/data/scripts/compass_weekly_workspace.py")
JOBS = Path("/opt/data/cron/jobs.json")
JOB_ID = "70f7c5dffb9f"
BOARD = "compass"
BOARD_DB = Path("/opt/data/kanban/boards/compass/kanban.db")
CHAT_ID = "b7f9698a4c6a8513a9e056e26d6d0e87"


def next_wednesday(now: datetime) -> str:
    days = (2 - now.weekday()) % 7
    if days == 0:
        days = 7
    return (now + timedelta(days=days)).date().isoformat()


def current_working_target(repo: Path = REPO) -> str:
    """Return the one unpublished weekly target after origin/main's latest published issue."""
    tracked = run([
        "git", "ls-tree", "-r", "--name-only", "origin/main", "--",
        "content/en/newsletters",
    ], cwd=repo)
    issue_dates = sorted(set(re.findall(
        r"content/en/newsletters/(\d{4}-\d{2}-\d{2})-newsletter\.md$", tracked,
        re.MULTILINE,
    )), reverse=True)
    for issue_date in issue_dates:
        source = run([
            "git", "show",
            f"origin/main:content/en/newsletters/{issue_date}-newsletter.md",
        ], cwd=repo)
        frontmatter = source.split("---", 2)
        if len(frontmatter) >= 3 and re.search(
            r"(?m)^draft:\s*false\s*$", frontmatter[1]
        ):
            return (
                datetime.fromisoformat(issue_date) + timedelta(days=7)
            ).date().isoformat()
    raise RuntimeError("origin/main contains no published newsletter")


def run(args: list[str], cwd: Path = REPO, timeout: int = 180) -> str:
    result = subprocess.run(args, cwd=cwd, text=True, capture_output=True, timeout=timeout, check=False)
    if result.returncode:
        detail = (result.stderr or result.stdout or "unknown failure").strip()
        raise RuntimeError(f"{' '.join(args[:4])} failed: {detail}")
    return result.stdout.strip()


def task_bodies(prompt: str, target: str, queue: str) -> list[str]:
    titles = ("Compass Newsletter Pipeline", "Compass Translation", "Compass Podcast Prep")
    templates: dict[str, str] = {}
    for title in titles:
        pattern = rf'create "{re.escape(title)} \$\{{TARGET\}}" \\\n\s+--body "(.*?)" \\\n\s+--assignee'
        match = re.search(pattern, prompt, re.DOTALL)
        if not match:
            raise RuntimeError(f"cannot extract task template for {title}")
        body = match.group(1).replace("${TARGET}", target)
        templates[title] = body

    queue_text = queue or "none"
    research = (
        f"Run only Stages 0-4 of skills/_COMPASS/agents/OrchestratorAgent.md for target {target}. "
        "This task is pinned to openai-codex/gpt-5.6-sol: all discovery, source research, intake, "
        "fetch evaluation, triage, selection, and selection reviewers must remain Sol-owned. "
        f"Use this link_queue.md content verbatim as intake: {queue_text}. "
        f"Persist preflight_{target}.md, intake_{target}.md, fetch_{target}.md, triage_{target}.md, "
        f"and selection_review_{target}.md with evidence-bearing gates. Follow the queue archival "
        "and shared-inbox acknowledgement rules in the Tuesday workflow. If human selection input is "
        "required, use a typed needs_input block. Complete only after Stage 4 passes; do not draft prose."
    )
    writing = (
        f"Run only Stages 5-6 of skills/_COMPASS/agents/OrchestratorAgent.md for target {target}. "
        f"First require every Stage 0-4 artifact and selection_review_{target}.md to end in GATE: PASS. "
        "This durable goal is Sol-primary under the geminiwriter profile: keep orchestration, source "
        "control, factual verification, and final synthesis on Sol. Delegate only bounded section-prose "
        "drafting through the profile's reset-aware Cursor lane, then record the actual returned model in "
        "every section artifact: Gemini 3.1 Pro when available, otherwise Grok 4.6. Write section prose in "
        "parallel where safe, validate every returned section against the source artifacts, assemble the "
        "draft with draft: true, synchronize section artifacts, and "
        "complete only after all Stage 5 section gates and Stage 6 assembly checks pass. Do not perform "
        "final review, open a PR, merge, publish, sign, or broadcast."
    )
    draft = (
        f"Run only Stages 7-8 of skills/_COMPASS/agents/OrchestratorAgent.md for target {target}. "
        "This task is pinned to openai-codex/gpt-5.6-sol: the ReviewSwarm coordinator, all five "
        "reviewers, correction verification, factual control, and final handoff must remain Sol-owned. "
        f"Require the assembled draft and section artifacts for {target}, run every evidence-bearing "
        "review/build/link/continuity gate, apply corrections through the writing artifacts, create or "
        "update the draft newsletter PR, and preserve draft: true. Tuesday is not complete until the "
        "exact draft PR exists and all Stage 7 evidence plus verified-recipient outreach receipts are "
        "durable. Automatically omit researched-unresolved outreach identities with evidence; do not "
        "create a needs_input gate for them or for the ordinary READY FOR HUMAN REVIEW handoff. Surface "
        "the PR and handoff once. Copy the final English Markdown to "
        "/opt/data/blog/handoffs/deliverables/ as a regular non-symlink file, run the deterministic "
        "Marmot delivery preflight, and record exactly one origin-bound MARMOT_OUTCOME with one MEDIA: "
        "line. Complete only after the canonical Marmot delivery acknowledgement is durable. Do not "
        "create a needs_input gate for the ordinary READY FOR HUMAN REVIEW handoff, and do not merge, "
        "deploy, sign, or broadcast. On success call kanban_complete so the separate Wednesday "
        "publication card can advance."
    )
    publication = (
        f"Own only Wednesday publication for Compass {target}; Tuesday draft finalization is a parent "
        "and must already be complete. Before Wednesday's authorized clock gates, park with a typed "
        "dependency block rather than needs_input. At the scheduled refresh and publication windows, "
        "follow the authoritative Compass Wednesday crons and PublishAgent gates. Complete only after "
        "deployment and both Nostr events are independently verified. Do not treat silence at the "
        "ordinary draft-review handoff as a publication hold, but obey any explicit owner hold."
    )
    return [
        research,
        writing,
        draft,
        publication,
        templates["Compass Translation"],
        templates["Compass Podcast Prep"],
    ]


def verify_previous_publication(
    repo: Path = REPO, worktree_root: Path = Path("/opt/data/compass-worktrees")
) -> str:
    tracked = run([
        "git", "ls-tree", "-r", "--name-only", "origin/main", "--",
        "content/en/newsletters",
    ], cwd=repo)
    issue_dates = sorted(set(re.findall(
        r"content/en/newsletters/(\d{4}-\d{2}-\d{2})-newsletter\.md$", tracked,
        re.MULTILINE,
    )))
    if not issue_dates:
        raise RuntimeError("origin/main contains no published newsletter")
    previous = issue_dates[-1]

    workspace = repo / "data/newsletter_workspace"
    logs = [
        workspace / f"publish_log_{previous}.md",
        worktree_root / previous / "data/newsletter_workspace" / f"publish_log_{previous}.md",
    ]
    logs = [path for path in logs if path.is_file()]
    if logs:
        latest = logs[-1]
        text = latest.read_text(encoding="utf-8")
        if "GATE: PASS" not in text or "kind 30023" not in text or "kind 1" not in text:
            raise RuntimeError(f"prior publication proof is incomplete: {latest}")
    else:
        published = workspace / "published"
        article_path = published / f"{previous}_30023.json"
        if not article_path.is_file():
            raise RuntimeError(f"no publication proof exists for latest issue {previous}")
        note_path = published / f"{previous}_1.json"
        if not note_path.is_file():
            raise RuntimeError(f"prior publication proof is incomplete: missing {note_path}")
        try:
            article = json.loads(article_path.read_text(encoding="utf-8"))
            note = json.loads(note_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"prior publication proof is invalid JSON: {exc}") from exc
        for path, event, expected_kind in (
            (article_path, article, 30023),
            (note_path, note, 1),
        ):
            if event.get("kind") != expected_kind or not event.get("id") or not event.get("sig"):
                raise RuntimeError(f"prior publication proof is incomplete: {path}")
    open_prs = json.loads(
        run([
            "gh", "pr", "list", "-R", "andotherstuff/nostr-compass", "--state", "open",
            "--json", "headRefName,url",
        ]) or "[]"
    )
    blockers = [item["url"] for item in open_prs if item.get("headRefName", "").startswith("newsletter/")]
    if blockers:
        raise RuntimeError("publication/update PR still open: " + ", ".join(blockers))
    return previous


def _guard_archived_idempotency_key(key: str, board_db: Path) -> dict[str, object] | None:
    """Fail closed when an archived key could be recreated by Hermes 0.20.x."""
    if not board_db.is_file():
        return None
    uri = f"file:{board_db}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        rows = connection.execute(
            "SELECT id,status FROM tasks WHERE idempotency_key=?", (key,)
        ).fetchall()
    if not rows or not any(status == "archived" for _, status in rows):
        return None
    live = [(task_id, status) for task_id, status in rows if status != "archived"]
    if len(live) == 1:
        return {"id": live[0][0], "status": live[0][1], "idempotency_key": key}
    if live:
        raise RuntimeError(f"multiple live tasks share archived idempotency key {key}")
    raise RuntimeError(f"archived idempotency key {key} cannot mint a replacement task")


def create_task(hermes: str, title: str, body: str, workspace: str, key: str, runtime: str,
                parent: str | None = None, assignee: str = "default",
                model: str | None = None, provider: str | None = None,
                board_db: Path = BOARD_DB) -> dict[str, object]:
    existing = _guard_archived_idempotency_key(key, board_db)
    if existing is not None:
        return existing
    args = [
        hermes, "kanban", "--board", BOARD, "create", title, "--body", body,
        "--assignee", assignee, "--workspace", f"dir:{workspace}", "--tenant", "newsletter",
        "--idempotency-key", key, "--priority", "10000", "--max-runtime", runtime, "--max-retries", "3", "--json",
        "--goal", "--goal-max-turns", "50", "--skill", "compass-newsletter",
    ]
    if parent:
        args.extend(["--parent", parent])
    if model:
        args.extend(["--model", model])
    if provider:
        if not model:
            raise RuntimeError("provider override requires a model override")
        args.extend(["--provider", provider])
    return json.loads(run(args, cwd=Path(workspace)))


def dispatch_graph(hermes: str, target: str, workspace: str,
                   bodies: list[str]) -> dict[str, dict[str, object]]:
    """Idempotently create every remaining stage, even when research already exists."""
    research = create_task(
        hermes, f"Compass Research and Preparation {target}", bodies[0], workspace,
        f"compass-research-{target}", "6h", model="gpt-5.6-sol", provider="openai-codex",
    )
    writing = create_task(
        hermes, f"Compass Section Writing {target}", bodies[1], workspace,
        f"compass-writing-{target}", "5h", str(research["id"]), assignee="geminiwriter",
    )
    draft = create_task(
        hermes, f"Compass Draft Finalization {target}", bodies[2], workspace,
        f"compass-draft-finalization-{target}", "5h", str(writing["id"]),
        model="gpt-5.6-sol", provider="openai-codex",
    )
    pipeline = create_task(
        hermes, f"Compass Newsletter Pipeline {target}", bodies[3], workspace,
        f"compass-pipeline-{target}", "5h", str(draft["id"]),
        model="gpt-5.6-sol", provider="openai-codex",
    )
    pipeline_id = str(pipeline["id"])
    translation = create_task(
        hermes, f"Compass Translation {target}", bodies[4], workspace,
        f"compass-translate-{target}", "4h", pipeline_id,
    )
    podcast = create_task(
        hermes, f"Compass Podcast Prep {target}", bodies[5], workspace,
        f"compass-podcast-{target}", "3h", pipeline_id,
    )
    return {
        "research": research,
        "writing": writing,
        "draft": draft,
        "pipeline": pipeline,
        "translation": translation,
        "podcast": podcast,
    }


def acknowledge_shared_queue(workspace: Path, target: str, repo: Path = REPO,
                             archive_root: Path = INBOX_ARCHIVE) -> bool:
    """Recoverably consume the shared queue after durable dispatch succeeds."""
    source = repo / "data/newsletter_workspace/link_queue.md"
    workspace_queue = workspace / "data/newsletter_workspace/link_queue.md"
    destination = archive_root / f"link_queue_{target}.md"
    if not source.exists():
        return False
    if not workspace_queue.is_file() or source.read_bytes() != workspace_queue.read_bytes():
        raise RuntimeError("shared link queue changed after weekly workspace initialization")
    archive_root.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.read_bytes() != source.read_bytes():
            raise RuntimeError(f"queue archive collision: {destination}")
        source.unlink()
    else:
        source.replace(destination)
    return True


def reconcile_target(target: str,
                     worktree_root: Path = Path("/opt/data/compass-worktrees"),
                     repo: Path = REPO) -> dict[str, dict[str, object]]:
    """Idempotently restore a partial weekly graph without consuming shared intake."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", target):
        raise RuntimeError(f"invalid Compass target: {target}")
    expected = current_working_target(repo)
    if target != expected:
        raise RuntimeError(
            f"refusing historical or future Compass reconciliation for {target}; "
            f"current working target is {expected}"
        )
    workspace = (worktree_root / target).resolve()
    if not workspace.is_dir():
        raise RuntimeError(f"weekly workspace is missing: {workspace}")
    jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
    prompt = next(item["prompt"] for item in jobs if item["id"] == JOB_ID)
    queue_path = workspace / "data/newsletter_workspace/link_queue.md"
    queue = queue_path.read_text(encoding="utf-8") if queue_path.is_file() else ""
    bodies = task_bodies(prompt, target, queue)
    hermes = shutil.which("hermes") or "/opt/hermes/.venv/bin/hermes"
    return dispatch_graph(hermes, target, str(workspace), bodies)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reconcile-target")
    args = parser.parse_args()
    try:
        if args.reconcile_target:
            result = reconcile_target(args.reconcile_target)
            print(json.dumps(result, sort_keys=True))
            return 0
        target = next_wednesday(datetime.now(timezone.utc))
        previous = verify_previous_publication()
        prepared = json.loads(run([sys.executable, str(HELPER), target]))
        workspace = prepared["workspace"]
        if prepared["branch"] != f"newsletter/{target}":
            raise RuntimeError("workspace helper returned the wrong branch")

        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        prompt = next(item["prompt"] for item in jobs if item["id"] == JOB_ID)
        queue_path = Path(workspace) / "data/newsletter_workspace/link_queue.md"
        queue = queue_path.read_text(encoding="utf-8") if queue_path.is_file() else ""
        bodies = task_bodies(prompt, target, queue)
        hermes = shutil.which("hermes") or "/opt/hermes/.venv/bin/hermes"

        dispatch_graph(hermes, target, workspace, bodies)
        acknowledge_shared_queue(Path(workspace), target)
        # Durable kickoff is reaction-only. The finalizer owns the one
        # user-visible outcome after the full delivery contract succeeds.
        return 0
    except (OSError, RuntimeError, KeyError, StopIteration, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        print(f"Compass Tuesday dispatch failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
