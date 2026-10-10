"""Tests for api/runtime_panel.py Execution Flow and Reliability metrics."""
import pytest
from pathlib import Path
from api.run_journal import append_run_event
from api.runtime_panel import build_run_timeline, discover_all_runs, normalize_event


def test_normalize_event_categorization():
    # 1. Shell command
    tool_bash = {
        "event": "tool",
        "created_at": 1000.0,
        "seq": 1,
        "payload": {
            "name": "bash",
            "arguments": {"command": "npm install"},
            "tool_call_id": "call_1",
        },
    }
    norm_bash = normalize_event(tool_bash)
    assert norm_bash["category"] == "shell"
    assert norm_bash["icon"] == "🔧"
    assert norm_bash["title"] == "Execute Shell"
    assert norm_bash["target"] == "npm install"

    # 2. Write file
    tool_write = {
        "event": "tool",
        "created_at": 1001.0,
        "seq": 2,
        "payload": {
            "name": "write_file",
            "arguments": {"target_file": "src/App.tsx"},
            "tool_call_id": "call_2",
        },
    }
    norm_write = normalize_event(tool_write)
    assert norm_write["category"] == "write"
    assert norm_write["icon"] == "✍️"
    assert norm_write["title"] == "Write File"
    assert norm_write["target"] == "src/App.tsx"

    # 3. Excluded noise events (metering, token, title, etc.)
    assert normalize_event({"event": "metering"}) is None
    assert normalize_event({"event": "token"}) is None
    assert normalize_event({"event": "state_saved"}) is None
    assert normalize_event({"event": "title"}) is None


def test_build_run_timeline_execution_flow(tmp_path: Path):
    sid = "session-test-1"
    rid = "run-test-1"

    # Write events to journal
    events = [
        {"event": "reasoning", "payload": {"text": "Planning to install dependencies and view App.tsx"}},
        {"event": "tool", "payload": {"name": "bash", "arguments": {"command": "npm install"}, "tool_call_id": "c1"}},
        {"event": "tool_complete", "payload": {"name": "bash", "result": "added 100 packages", "tool_call_id": "c1", "exit_code": 0}},
        {"event": "tool", "payload": {"name": "read_file", "arguments": {"path": "src/App.tsx"}, "tool_call_id": "c2"}},
        {"event": "tool_complete", "payload": {"name": "read_file", "result": "import React from 'react';", "tool_call_id": "c2"}},
        {"event": "done", "payload": {"terminal_state": "completed"}},
    ]

    for idx, evt in enumerate(events, start=1):
        append_run_event(sid, rid, evt["event"], evt["payload"], session_dir=tmp_path)

    from unittest.mock import patch
    with patch("api.models.SESSION_DIR", str(tmp_path)):
        res = build_run_timeline(rid, session_id=sid)
        assert res is not None
        assert res["run_id"] == rid
        assert res["session_id"] == sid

        # Check Reliability Summary
        rel = res["reliability"]
        assert rel["total_steps"] > 0
        assert rel["failed_steps"] == 0
        assert rel["status"] == "completed"

        # Check Timeline Flow
        timeline = res["timeline"]
        # Step 1: Task Started
        assert timeline[0]["type"] == "task_start"
        assert timeline[0]["icon"] == "🚀"

        # Step 2: Thinking
        thinking_step = [s for s in timeline if s["type"] == "thinking"][0]
        assert thinking_step["icon"] == "🧠"
        assert "Planning" in thinking_step["target"]

        # Step 3: Shell
        shell_step = [s for s in timeline if s.get("category") == "shell"][0]
        assert shell_step["icon"] == "🔧"
        assert shell_step["title"] == "Execute Shell"
        assert shell_step["target"] == "npm install"
        assert shell_step["status"] == "success"

        # Step 4: Read File
        read_step = [s for s in timeline if s.get("category") == "read"][0]
        assert read_step["icon"] == "📄"
        assert read_step["title"] == "Read File"
        assert read_step["target"] == "src/App.tsx"
