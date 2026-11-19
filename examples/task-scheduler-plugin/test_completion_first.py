import json
import tempfile
import unittest
from pathlib import Path

from completion_first import Policy, decide, load_cursor, requires_split, rotate_after, store_cursor


class CompletionFirstTests(unittest.TestCase):
    def policy(self, root: Path, **overrides) -> Policy:
        values = dict(
            max_active_tasks=1,
            max_wakes_per_cycle=1,
            split_enabled=True,
            max_task_chars=100,
            compound_marker_threshold=2,
            bullet_threshold=3,
            compound_markers=(" and ", " then "),
            state_file=root / "state.json",
        )
        values.update(overrides)
        return Policy(**values)

    def candidate(self, agent="a", task="1", title="small", description="one change"):
        return {"agent_id": agent, "task_id": task, "title": title, "description": description}

    def request(self, candidates, active=0):
        return {"protocol_version": 1, "status_counts": {"in_progress": active}, "candidates": candidates}

    def test_active_limit_suppresses_new_wake(self):
        with tempfile.TemporaryDirectory() as root:
            result = decide(self.request([self.candidate()], active=1), self.policy(Path(root)), None)
            self.assertEqual(result, [])

    def test_wake_selected_when_slot_available(self):
        with tempfile.TemporaryDirectory() as root:
            result = decide(self.request([self.candidate()]), self.policy(Path(root)), None)
            self.assertEqual(result[0]["action"], "wake")

    def test_long_task_is_split(self):
        with tempfile.TemporaryDirectory() as root:
            item = self.candidate(description="x" * 101)
            self.assertTrue(requires_split(item, self.policy(Path(root))))

    def test_compound_task_is_split(self):
        with tempfile.TemporaryDirectory() as root:
            item = self.candidate(description="do a and b then c")
            self.assertTrue(requires_split(item, self.policy(Path(root))))

    def test_bulleted_task_is_split(self):
        with tempfile.TemporaryDirectory() as root:
            item = self.candidate(description="- a\n- b\n- c")
            self.assertTrue(requires_split(item, self.policy(Path(root))))

    def test_split_can_be_disabled(self):
        with tempfile.TemporaryDirectory() as root:
            policy = self.policy(Path(root), split_enabled=False)
            self.assertFalse(requires_split(self.candidate(description="x" * 1000), policy))

    def test_round_robin_rotates_after_last_agent(self):
        items = [self.candidate("a", "1"), self.candidate("b", "2"), self.candidate("c", "3")]
        self.assertEqual([item["agent_id"] for item in rotate_after(items, "b")], ["c", "a", "b"])

    def test_round_robin_wraps(self):
        items = [self.candidate("a", "1"), self.candidate("b", "2")]
        self.assertEqual([item["agent_id"] for item in rotate_after(items, "z")], ["a", "b"])

    def test_cursor_round_trip(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "nested" / "state.json"
            store_cursor(path, "agent-b")
            self.assertEqual(load_cursor(path), "agent-b")

    def test_corrupt_cursor_is_ignored(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "state.json"
            path.write_text("not json", encoding="utf-8")
            self.assertIsNone(load_cursor(path))

    def test_protocol_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(ValueError):
                decide({"protocol_version": 2, "candidates": []}, self.policy(Path(root)), None)


if __name__ == "__main__":
    unittest.main()
