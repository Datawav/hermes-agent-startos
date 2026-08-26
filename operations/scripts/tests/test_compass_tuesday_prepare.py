import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

P = Path(__file__).parents[1] / "compass_tuesday_prepare.py"
spec = importlib.util.spec_from_file_location("compass_tuesday_prepare", P)
assert spec is not None and spec.loader is not None
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)


class CompassTuesdayReconcileTests(unittest.TestCase):
    def test_draft_handoff_finishes_without_avoidable_human_gate(self):
        prompt = """create "Compass Newsletter Pipeline ${TARGET}" \\
  --body "legacy pipeline" \\
  --assignee default
create "Compass Translation ${TARGET}" \\
  --body "translation" \\
  --assignee default
create "Compass Podcast Prep ${TARGET}" \\
  --body "podcast" \\
  --assignee default
"""
        draft = m.task_bodies(prompt, "2033-05-18", "none")[2]
        publication = m.task_bodies(prompt, "2033-05-18", "none")[3]
        self.assertIn("Tuesday is not complete until the exact draft PR exists", draft)
        self.assertIn("Automatically omit researched-unresolved outreach identities", draft)
        self.assertIn("MARMOT_OUTCOME", draft)
        self.assertIn("call kanban_complete", draft)
        self.assertNotIn("use the existing typed needs_input handoff", draft)
        self.assertIn("Wednesday publication", publication)
        self.assertIn("dependency block", publication)

    def test_current_working_target_ignores_merged_draft(self):
        listing = (
            "content/en/newsletters/2033-05-11-newsletter.md\n"
            "content/en/newsletters/2033-05-18-newsletter.md\n"
        )
        with patch.object(m, "run", side_effect=[
            listing,
            "---\ndraft: true\n---\n",
            "---\ndraft: false\n---\n",
        ]):
            self.assertEqual(m.current_working_target(Path("/repo")), "2033-05-18")

    def test_reconcile_existing_workspace_dispatches_complete_graph_without_consuming_queue(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "worktrees" / "2033-05-18"
            (workspace / "data/newsletter_workspace").mkdir(parents=True)
            (workspace / "data/newsletter_workspace/link_queue.md").write_text("## item\n")
            jobs = root / "jobs.json"
            jobs.write_text(json.dumps({"jobs": [{"id": m.JOB_ID, "prompt": "weekly prompt"}]}))
            bodies = ["research", "writing", "draft", "pipeline", "translation", "podcast"]
            expected = {name: {"id": name} for name in
                        ("research", "writing", "draft", "pipeline", "translation", "podcast")}
            with (
                patch.object(m, "JOBS", jobs),
                patch.object(m, "shutil") as shutil_mock,
                patch.object(m, "task_bodies", return_value=bodies) as body_mock,
                patch.object(m, "dispatch_graph", return_value=expected) as dispatch_mock,
                patch.object(m, "acknowledge_shared_queue") as acknowledge_mock,
                patch.object(m, "current_working_target", return_value="2033-05-18"),
            ):
                shutil_mock.which.return_value = "/hermes"
                result = m.reconcile_target("2033-05-18", root / "worktrees")
            self.assertEqual(result, expected)
            body_mock.assert_called_once_with("weekly prompt", "2033-05-18", "## item\n")
            dispatch_mock.assert_called_once_with("/hermes", "2033-05-18", str(workspace), bodies)
            acknowledge_mock.assert_not_called()

    def test_reconcile_requires_existing_weekly_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(m, "current_working_target", return_value="2033-05-18"):
                with self.assertRaisesRegex(RuntimeError, "weekly workspace is missing"):
                    m.reconcile_target("2033-05-18", Path(tmp))

    def test_reconcile_target_rejects_non_date_input(self):
        with self.assertRaisesRegex(RuntimeError, "invalid Compass target"):
            m.reconcile_target("latest", Path("/tmp"))

    def test_reconcile_target_rejects_published_historical_week(self):
        with patch.object(m, "current_working_target", return_value="2033-05-18"):
            with self.assertRaisesRegex(RuntimeError, "refusing historical"):
                m.reconcile_target("2033-05-11", Path("/tmp"))


if __name__ == "__main__":
    unittest.main()
