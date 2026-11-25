#!/usr/bin/env python3
"""Reference completion-first scheduler plugin for LibreFang."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Policy:
    max_active_tasks: int
    max_wakes_per_cycle: int
    unclaimed_wake_hold_secs: int
    split_enabled: bool
    max_task_chars: int
    compound_marker_threshold: int
    bullet_threshold: int
    compound_markers: tuple[str, ...]
    state_file: Path


@dataclass(frozen=True)
class SchedulerState:
    last_agent_id: str | None = None
    last_task_id: str | None = None
    last_wake_epoch: int | None = None
    pending_count_at_wake: int | None = None


def load_policy(path: Path) -> Policy:
    with path.open("r", encoding="utf-8") as stream:
        raw = json.load(stream)
    policy = Policy(
        max_active_tasks=int(raw["max_active_tasks"]),
        max_wakes_per_cycle=int(raw["max_wakes_per_cycle"]),
        unclaimed_wake_hold_secs=int(raw.get("unclaimed_wake_hold_secs", 120)),
        split_enabled=bool(raw["split_enabled"]),
        max_task_chars=int(raw["max_task_chars"]),
        compound_marker_threshold=int(raw["compound_marker_threshold"]),
        bullet_threshold=int(raw["bullet_threshold"]),
        compound_markers=tuple(str(item).lower() for item in raw["compound_markers"]),
        state_file=(path.parent / raw["state_file"]).resolve(),
    )
    for name in (
        "max_active_tasks",
        "max_wakes_per_cycle",
        "unclaimed_wake_hold_secs",
        "max_task_chars",
        "compound_marker_threshold",
        "bullet_threshold",
    ):
        if getattr(policy, name) < 1:
            raise ValueError(f"{name} must be at least 1")
    if not policy.compound_markers:
        raise ValueError("compound_markers must not be empty")
    return policy


def requires_split(candidate: dict[str, Any], policy: Policy) -> bool:
    if not policy.split_enabled:
        return False
    text = f"{candidate.get('title', '')}\n{candidate.get('description', '')}".lower()
    if len(text) > policy.max_task_chars:
        return True
    marker_count = sum(text.count(marker) for marker in policy.compound_markers)
    bullet_count = sum(
        1 for line in text.splitlines() if line.lstrip().startswith(("- ", "* ", "+ "))
    )
    return (
        marker_count >= policy.compound_marker_threshold
        or bullet_count >= policy.bullet_threshold
    )


def load_state(path: Path) -> SchedulerState:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return SchedulerState(
            last_agent_id=value.get("last_agent_id"),
            last_task_id=value.get("last_task_id"),
            last_wake_epoch=value.get("last_wake_epoch"),
            pending_count_at_wake=value.get("pending_count_at_wake"),
        )
    except (FileNotFoundError, OSError, json.JSONDecodeError, AttributeError):
        return SchedulerState()


def store_state(path: Path, state: SchedulerState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "last_agent_id": state.last_agent_id,
                    "last_task_id": state.last_task_id,
                    "last_wake_epoch": state.last_wake_epoch,
                    "pending_count_at_wake": state.pending_count_at_wake,
                },
                stream,
            )
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def rotate_after(candidates: list[dict[str, Any]], last_agent_id: str | None) -> list[dict[str, Any]]:
    ordered = sorted(candidates, key=lambda item: (item["agent_id"], item["task_id"]))
    if not last_agent_id:
        return ordered
    for index, candidate in enumerate(ordered):
        if candidate["agent_id"] > last_agent_id:
            return ordered[index:] + ordered[:index]
    return ordered


def decide(
    request: dict[str, Any],
    policy: Policy,
    state: SchedulerState,
    now_epoch: int,
) -> list[dict[str, str]]:
    if request.get("protocol_version") != 1:
        raise ValueError("unsupported protocol_version")
    active = int(request.get("status_counts", {}).get("in_progress", 0))
    slots = max(0, policy.max_active_tasks - active)
    limit = min(slots, policy.max_wakes_per_cycle)
    candidates = list(request.get("candidates", []))
    pending_count = int(request.get("status_counts", {}).get("pending", 0))
    pending_has_not_decreased = (
        state.pending_count_at_wake is not None
        and pending_count >= state.pending_count_at_wake
    )
    if pending_has_not_decreased and state.last_wake_epoch is not None:
        elapsed = max(0, now_epoch - state.last_wake_epoch)
        if elapsed < policy.unclaimed_wake_hold_secs:
            return []
    ordered = rotate_after(candidates, state.last_agent_id)
    selected = ordered[:limit]
    return [
        {
            "agent_id": item["agent_id"],
            "task_id": item["task_id"],
            "action": "split" if requires_split(item, policy) else "wake",
        }
        for item in selected
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    policy = load_policy(args.config.resolve())
    request = json.load(sys.stdin)
    state = load_state(policy.state_file)
    now_epoch = int(time.time())
    pending_count = int(request.get("status_counts", {}).get("pending", 0))
    if state.last_wake_epoch is not None and state.pending_count_at_wake is None:
        state = SchedulerState(
            last_agent_id=state.last_agent_id,
            last_task_id=state.last_task_id,
            last_wake_epoch=state.last_wake_epoch,
            pending_count_at_wake=pending_count,
        )
        store_state(policy.state_file, state)
    decisions = decide(request, policy, state, now_epoch)
    if decisions:
        decision = decisions[-1]
        store_state(
            policy.state_file,
            SchedulerState(
                last_agent_id=decision["agent_id"],
                last_task_id=decision["task_id"],
                last_wake_epoch=now_epoch,
                pending_count_at_wake=pending_count,
            ),
        )
    json.dump({"protocol_version": 1, "decisions": decisions}, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
