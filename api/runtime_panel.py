"""Read-only Runtime Panel adapter for Agent execution observability.

This module provides a thin, read-only layer over the existing Run Journal
(api/run_journal.py) to power the Runtime Panel UI.  It never modifies agent
execution, streaming, or journal-writing logic.

Data sources:
    - api/run_journal: read_run_events, latest_run_summary, find_run_summary

Timeline transformation:
    Raw SSE events (token, tool, tool_complete, done, etc.) are converted
    into a simplified timeline suitable for display, with token events
    aggregated and tool calls paired.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.run_journal import (
    RUN_JOURNAL_DIR_NAME,
    _read_jsonl,
    _run_path,
    _validate_id,
    find_run_summary,
    latest_run_summary,
    read_run_events,
)

logger = logging.getLogger(__name__)

# Events to exclude from the timeline (too verbose for display)
_TIMELINE_EXCLUDED_EVENTS = {"token", "reasoning", "state_saved", "context_status"}

# Events to include in the timeline
_TIMELINE_INCLUDED_EVENTS = {
    "tool",
    "tool_complete",
    "done",
    "apperror",
    "error",
    "cancel",
    "approval",
    "clarify",
    "warning",
    "stream_end",
}

# Status mapping from raw SSE event names to display status
_STATUS_MAP = {
    "done": "completed",
    "cancel": "interrupted",
    "apperror": "failed",
    "error": "failed",
    "stream_end": "completed",
}


def _map_run_status(summary: dict) -> str:
    """Map run journal terminal_state to a display-friendly status string."""
    terminal_state = summary.get("terminal_state")
    if not terminal_state or terminal_state == "completed":
        return "completed"
    if terminal_state.startswith("interrupted"):
        return "interrupted"
    if terminal_state in ("errored", "tool_limit_reached"):
        return "failed"
    if not summary.get("terminal"):
        return "running"
    return terminal_state or "unknown"


def _extract_tool_name(payload: dict) -> str:
    """Extract tool name from a tool/tool_complete event payload."""
    if not isinstance(payload, dict):
        return "unknown"
    # tool events have name in payload
    return str(payload.get("name") or payload.get("tool_name") or "unknown").strip() or "unknown"


def _extract_tool_call_id(payload: dict) -> str:
    """Extract tool call ID for pairing tool and tool_complete events."""
    if not isinstance(payload, dict):
        return ""
    return str(
        payload.get("tool_call_id")
        or payload.get("id")
        or payload.get("call_id")
        or ""
    ).strip()


def _extract_tool_args(payload: dict) -> dict:
    """Extract tool arguments from payload for display."""
    if not isinstance(payload, dict):
        return {}
    return payload.get("arguments") or payload.get("args") or payload.get("input") or {}


def _extract_tool_result(payload: dict) -> str:
    """Extract tool result/output from tool_complete payload."""
    if not isinstance(payload, dict):
        return ""
    result = payload.get("result") or payload.get("output") or payload.get("content") or ""
    return str(result)[:500]  # Cap result length for display


def _extract_command(payload: dict) -> str:
    """Extract command from tool payload for display."""
    args = _extract_tool_args(payload)
    if isinstance(args, dict):
        # Common patterns for bash/command tools
        cmd = args.get("command") or args.get("cmd") or args.get("input") or args.get("query") or ""
        return str(cmd)[:200]
    if isinstance(args, str):
        return args[:200]
    return ""


def _format_duration(started_at: float, finished_at: float) -> float:
    """Calculate duration in seconds, rounded to 2 decimal places."""
    duration = finished_at - started_at
    return round(max(0.0, duration), 2)


def discover_all_runs(session_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover all runs from the Run Journal directory structure.

    Scans {SESSION_DIR}/_run_journal/{session_id}/{run_id}.jsonl
    and builds a summary for each run.

    Returns:
        List of run summaries sorted by most recent first.
    """
    root = Path(session_dir) if session_dir is not None else None
    try:
        from api.models import SESSION_DIR
        if root is None:
            root = Path(SESSION_DIR)
    except ImportError:
        root = root or Path.cwd() / "sessions"

    journal_root = root / RUN_JOURNAL_DIR_NAME
    if not journal_root.exists():
        return []

    runs = []
    for session_dir_path in journal_root.iterdir():
        if not session_dir_path.is_dir():
            continue
        session_id = session_dir_path.name
        for run_file in session_dir_path.glob("*.jsonl"):
            run_id = run_file.stem
            try:
                _validate_id(run_id, "run_id")
            except ValueError:
                continue

            summary = latest_run_summary(session_id, run_id, session_dir=root)
            if not summary:
                continue

            events = summary.get("events", [])
            if not events:
                # Try reading events directly
                try:
                    result = read_run_events(session_id, run_id, session_dir=root)
                    events = result.get("events", [])
                except Exception:
                    events = []

            if not events:
                continue

            # Calculate timing
            first_event = events[0]
            last_event = events[-1]
            started_at = float(first_event.get("created_at") or 0)
            finished_at = float(last_event.get("created_at") or started_at)

            # Count tool calls (paired tool + tool_complete)
            tool_call_ids = set()
            for event in events:
                if event.get("event") == "tool" and isinstance(event.get("payload"), dict):
                    tid = event["payload"].get("tool_call_id") or event["payload"].get("id")
                    if tid:
                        tool_call_ids.add(tid)

            runs.append({
                "run_id": run_id,
                "session_id": session_id,
                "status": _map_run_status(summary),
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": _format_duration(started_at, finished_at),
                "event_count": len(events),
                "tool_calls": len(tool_call_ids),
                "terminal_state": summary.get("terminal_state"),
            })

    # Sort by started_at descending (most recent first)
    runs.sort(key=lambda r: r["started_at"], reverse=True)
    return runs


def _aggregate_token_events(events: List[dict]) -> Optional[dict]:
    """Aggregate consecutive token events into a single 'streaming' event.

    Returns the aggregated event or None if no tokens to aggregate.
    """
    token_events = [e for e in events if e.get("event") == "token"]
    if not token_events:
        return None

    total_chars = sum(len(e.get("payload", {}).get("text", "")) for e in token_events)
    first_ts = float(token_events[0].get("created_at", 0))
    last_ts = float(token_events[-1].get("created_at", 0))

    return {
        "type": "streaming",
        "title": "Assistant response",
        "status": "completed",
        "seq": token_events[0].get("seq"),
        "timestamp": first_ts,
        "end_seq": token_events[-1].get("seq"),
        "end_timestamp": last_ts,
        "details": {
            "token_count": len(token_events),
            "total_chars": total_chars,
        },
    }


def normalize_event(event: dict) -> Optional[dict]:
    """Convert a raw run journal event into a timeline-friendly format.

    Returns None for excluded events (token, reasoning, etc.).
    """
    event_name = event.get("event") or event.get("type") or "unknown"

    # Skip excluded events
    if event_name in _TIMELINE_EXCLUDED_EVENTS:
        return None

    seq = event.get("seq", 0)
    timestamp = float(event.get("created_at", 0))
    payload = event.get("payload") or {}

    if event_name in ("tool",):
        tool_name = _extract_tool_name(payload)
        return {
            "type": "tool",
            "title": tool_name,
            "status": "running",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "tool_call_id": _extract_tool_call_id(payload),
                "command": _extract_command(payload),
            },
        }

    if event_name in ("tool_complete",):
        tool_name = _extract_tool_name(payload)
        status = "success"
        if isinstance(payload, dict):
            if payload.get("error") or payload.get("exception"):
                status = "error"
            elif payload.get("status") == "error":
                status = "error"

        return {
            "type": "tool_complete",
            "title": tool_name,
            "status": status,
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "tool_call_id": _extract_tool_call_id(payload),
                "result": _extract_tool_result(payload),
            },
        }

    if event_name in ("done", "stream_end"):
        terminal_state = event.get("terminal_state") or payload.get("terminal_state")
        status = "success"
        if terminal_state or event_name == "done":
            if terminal_state and "error" in str(terminal_state).lower():
                status = "failed"
            elif terminal_state and "interrupt" in str(terminal_state).lower():
                status = "interrupted"
        return {
            "type": "done",
            "title": "Completed",
            "status": status,
            "seq": seq,
            "timestamp": timestamp,
        }

    if event_name in ("apperror", "error"):
        error_type = payload.get("type") or payload.get("error_type") or "error"
        return {
            "type": "error",
            "title": "Error",
            "status": "failed",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "error_type": str(error_type)[:100],
                "message": str(payload.get("message") or error_type)[:200],
            },
        }

    if event_name == "cancel":
        return {
            "type": "cancel",
            "title": "Cancelled",
            "status": "interrupted",
            "seq": seq,
            "timestamp": timestamp,
        }

    if event_name == "warning":
        return {
            "type": "warning",
            "title": "Warning",
            "status": "warning",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "message": str(payload.get("message") or "")[:200],
            },
        }

    if event_name == "approval":
        return {
            "type": "approval",
            "title": "Approval requested",
            "status": "pending",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "question": str(payload.get("question") or payload.get("text") or "")[:200],
            },
        }

    if event_name == "clarify":
        return {
            "type": "clarify",
            "title": "Clarification requested",
            "status": "pending",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "question": str(payload.get("question") or payload.get("text") or "")[:200],
            },
        }

    # Fallback for any other event type
    return {
        "type": event_name,
        "title": event_name.capitalize(),
        "status": "info",
        "seq": seq,
        "timestamp": timestamp,
    }


def build_run_timeline(run_id: str, session_id: Optional[str] = None) -> Optional[dict]:
    """Build a complete timeline for a single run.

    Reads the run journal, normalizes events, pairs tool calls,
    and returns a structured timeline suitable for UI display.

    Args:
        run_id: The run/stream ID to build timeline for.
        session_id: Optional session ID to scope the search.

    Returns:
        Dictionary with run metadata, summary, and timeline events.
        None if run not found.
    """
    # First, try to find the run summary
    summary = find_run_summary(run_id, session_dir=None)
    if not summary:
        return None

    actual_session_id = session_id or summary.get("session_id")
    if not actual_session_id:
        return None

    # Read all events
    events_result = read_run_events(
        actual_session_id,
        run_id,
        session_dir=None,
    )
    raw_events = events_result.get("events", [])

    if not raw_events:
        return {
            "run_id": run_id,
            "session_id": actual_session_id,
            "summary": {
                "status": "unknown",
                "duration_seconds": 0,
                "tool_calls": 0,
                "event_count": 0,
            },
            "timeline": [],
        }

    # Normalize events
    timeline_items = []
    for event in raw_events:
        normalized = normalize_event(event)
        if normalized:
            timeline_items.append(normalized)

    # Pair tool calls: merge tool + tool_complete into single entries
    paired_timeline = _pair_tool_calls(timeline_items)

    # Calculate summary
    first_event = raw_events[0]
    last_event = raw_events[-1]
    started_at = float(first_event.get("created_at", 0))
    finished_at = float(last_event.get("created_at", started_at))

    # Count unique tool calls
    tool_call_ids = set()
    for event in raw_events:
        payload = event.get("payload") or {}
        if event.get("event") == "tool":
            tid = payload.get("tool_call_id") or payload.get("id")
            if tid:
                tool_call_ids.add(tid)

    status = _map_run_status({
        "terminal_state": summary.get("terminal_state"),
        "terminal": summary.get("terminal"),
    })

    return {
        "run_id": run_id,
        "session_id": actual_session_id,
        "summary": {
            "status": status,
            "duration_seconds": _format_duration(started_at, finished_at),
            "tool_calls": len(tool_call_ids),
            "event_count": len(raw_events),
            "started_at": started_at,
            "finished_at": finished_at,
        },
        "timeline": paired_timeline,
    }


def _pair_tool_calls(timeline_items: List[dict]) -> List[dict]:
    """Pair tool and tool_complete events in the timeline.

    When a tool event is followed by its matching tool_complete,
    merge them into a single entry with duration.
    """
    if not timeline_items:
        return []

    result = []
    pending_tools = {}  # tool_call_id -> timeline item

    for item in timeline_items:
        if item["type"] == "tool":
            tool_call_id = item.get("details", {}).get("tool_call_id")
            if tool_call_id:
                pending_tools[tool_call_id] = item
            else:
                # No ID, add directly
                result.append(item)
        elif item["type"] == "tool_complete":
            tool_call_id = item.get("details", {}).get("tool_call_id")
            if tool_call_id and tool_call_id in pending_tools:
                # Pair with the pending tool event
                tool_event = pending_tools.pop(tool_call_id)
                duration = item["timestamp"] - tool_event["timestamp"]
                merged = {
                    "type": "tool_execution",
                    "title": tool_event["title"],
                    "status": item["status"],
                    "seq": tool_event["seq"],
                    "timestamp": tool_event["timestamp"],
                    "duration_seconds": round(max(0, duration), 3),
                    "details": {
                        **tool_event.get("details", {}),
                        **item.get("details", {}),
                    },
                }
                result.append(merged)
            else:
                # Orphan tool_complete, add as-is
                result.append(item)
        else:
            # Non-tool event, add directly
            result.append(item)

    # Any unpaired tool events (no completion received)
    for tool_call_id, tool_event in pending_tools.items():
        tool_event["status"] = "timeout"
        tool_event["type"] = "tool_pending"
        result.append(tool_event)

    return result


def get_run_list() -> dict:
    """Get a list of all runs with summary information.

    Returns:
        Dictionary with 'runs' list and 'total' count.
    """
    runs = discover_all_runs()
    return {
        "runs": runs,
        "total": len(runs),
    }
