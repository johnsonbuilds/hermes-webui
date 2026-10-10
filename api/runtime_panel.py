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

# Events to exclude from the timeline (internal noise events)
_TIMELINE_EXCLUDED_EVENTS = {
    "token",
    "metering",
    "state_saved",
    "context_status",
    "title",
    "terminal",
    "ping",
    "snapshot",
}

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
    "reasoning",
    "interim_assistant",
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
    args = payload.get("arguments") or payload.get("args") or payload.get("input") or {}
    if isinstance(args, str):
        try:
            return json.loads(args)
        except Exception:
            return {"raw": args}
    return args if isinstance(args, dict) else {}


def _extract_tool_result(payload: dict) -> str:
    """Extract tool result/output from tool_complete payload."""
    if not isinstance(payload, dict):
        return ""
    result = payload.get("result") or payload.get("output") or payload.get("content") or ""
    return str(result)[:1000]  # Cap result length for display


def _extract_command(payload: dict) -> str:
    """Extract command from tool payload for display."""
    args = _extract_tool_args(payload)
    if isinstance(args, dict):
        cmd = args.get("command") or args.get("cmd") or args.get("input") or args.get("query") or ""
        return str(cmd)[:300]
    if isinstance(args, str):
        return args[:300]
    return ""


def _format_duration(started_at: float, finished_at: float) -> float:
    """Calculate duration in seconds, rounded to 2 decimal places."""
    duration = finished_at - started_at
    return round(max(0.0, duration), 2)


def _categorize_tool(tool_name: str, payload: dict) -> dict:
    """Categorize a tool call into human-friendly execution flow metadata.
    
    Returns dict with keys: category, icon, title, target.
    """
    name_lower = tool_name.lower().strip()
    args = _extract_tool_args(payload)
    
    # 1. Shell / Terminal Command
    if name_lower in ("bash", "exec", "run_command", "terminal", "cmd", "shell"):
        cmd = args.get("command") or args.get("cmd") or args.get("input") or args.get("query") or _extract_command(payload)
        return {
            "category": "shell",
            "icon": "🔧",
            "title": "Execute Shell",
            "target": str(cmd).strip()[:300],
        }
        
    # 2. Write / Edit File
    if name_lower in ("write_file", "replace_file_content", "multi_replace_file_content", "create_file", "write_to_file", "edit_file"):
        path = args.get("target_file") or args.get("path") or args.get("filepath") or args.get("file") or ""
        title = "Write File" if name_lower in ("write_file", "create_file", "write_to_file") else "Edit File"
        return {
            "category": "write",
            "icon": "✍️",
            "title": title,
            "target": str(path).strip()[:300],
        }
        
    # 3. Read File
    if name_lower in ("read_file", "view_file", "cat", "read_run_events"):
        path = args.get("path") or args.get("absolute_path") or args.get("target_file") or args.get("file") or ""
        return {
            "category": "read",
            "icon": "📄",
            "title": "Read File",
            "target": str(path).strip()[:300],
        }
        
    # 4. Search Files
    if name_lower in ("grep_search", "file_search", "glob", "find", "grep", "search_files", "search"):
        query = args.get("query") or args.get("pattern") or args.get("search_path") or ""
        return {
            "category": "search",
            "icon": "🔍",
            "title": "Search Files",
            "target": str(query).strip()[:300],
        }
        
    # 5. Browser / Web Search
    if name_lower in ("browser", "read_url", "read_url_content", "read_browser_page", "web_search", "search_web", "navigate"):
        target = args.get("url") or args.get("query") or args.get("domain") or ""
        title = "Web Search" if "search" in name_lower else "Browser"
        return {
            "category": "browser",
            "icon": "🌐",
            "title": title,
            "target": str(target).strip()[:300],
        }
        
    # 6. Subagent / Delegation
    if name_lower in ("invoke_subagent", "send_message", "manage_task", "define_subagent"):
        subagents = args.get("subagents") or args.get("recipient") or args.get("name") or ""
        if isinstance(subagents, list) and subagents and isinstance(subagents[0], dict):
            sub_target = subagents[0].get("role") or subagents[0].get("type_name") or "Subagent"
        else:
            sub_target = str(subagents) or "Subagent"
        return {
            "category": "subagent",
            "icon": "🤖",
            "title": "Subagent Task",
            "target": str(sub_target).strip()[:300],
        }
        
    # 7. Memory / Knowledge
    if "wiki" in name_lower or "memory" in name_lower:
        return {
            "category": "memory",
            "icon": "🧠",
            "title": "Memory",
            "target": tool_name,
        }
        
    # 8. Custom / Fallback Tool
    clean_title = tool_name.replace("_", " ").title()
    target = ""
    for k in ("path", "query", "command", "name", "target", "file"):
        if args.get(k):
            target = str(args[k]).strip()
            break
    return {
        "category": "custom",
        "icon": "🛠️",
        "title": clean_title,
        "target": target[:300],
    }


def discover_all_runs(session_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Discover all runs from the Run Journal directory structure."""
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
                try:
                    result = read_run_events(session_id, run_id, session_dir=root)
                    events = result.get("events", [])
                except Exception:
                    events = []

            if not events:
                continue

            first_event = events[0]
            last_event = events[-1]
            started_at = float(first_event.get("created_at") or 0)
            finished_at = float(last_event.get("created_at") or started_at)

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

    runs.sort(key=lambda r: r["started_at"], reverse=True)
    return runs


def normalize_event(event: dict) -> Optional[dict]:
    """Convert a raw run journal event into an execution flow item.

    Returns None for excluded internal noise events.
    """
    event_name = event.get("event") or event.get("type") or "unknown"

    if event_name in _TIMELINE_EXCLUDED_EVENTS:
        return None

    seq = event.get("seq", 0)
    timestamp = float(event.get("created_at", 0))
    payload = event.get("payload") or {}

    if event_name in ("reasoning", "interim_assistant"):
        text = str(payload.get("text") or payload.get("content") or payload.get("reasoning") or "").strip()
        if not text:
            return None
        return {
            "type": "thinking_segment",
            "category": "thinking",
            "icon": "🧠",
            "title": "Thinking",
            "status": "completed",
            "seq": seq,
            "timestamp": timestamp,
            "details": {"text": text},
        }

    if event_name in ("tool",):
        tool_name = _extract_tool_name(payload)
        cat = _categorize_tool(tool_name, payload)
        return {
            "type": "tool",
            "category": cat["category"],
            "icon": cat["icon"],
            "title": cat["title"],
            "target": cat["target"],
            "status": "running",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "tool_name": tool_name,
                "tool_call_id": _extract_tool_call_id(payload),
                "command": _extract_command(payload),
                "args": _extract_tool_args(payload),
            },
        }

    if event_name in ("tool_complete",):
        tool_name = _extract_tool_name(payload)
        cat = _categorize_tool(tool_name, payload)
        status = "success"
        error_msg = ""
        exit_code = None

        if isinstance(payload, dict):
            if payload.get("error") or payload.get("exception"):
                status = "failed"
                error_msg = str(payload.get("error") or payload.get("exception"))
            elif payload.get("status") == "error":
                status = "failed"
                error_msg = str(payload.get("message") or payload.get("error") or "Tool execution error")
            if "exit_code" in payload:
                exit_code = payload["exit_code"]
                if exit_code != 0:
                    status = "failed"

        return {
            "type": "tool_complete",
            "category": cat["category"],
            "icon": cat["icon"],
            "title": cat["title"],
            "target": cat["target"],
            "status": status,
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "tool_name": tool_name,
                "tool_call_id": _extract_tool_call_id(payload),
                "result": _extract_tool_result(payload),
                "error": error_msg,
                "exit_code": exit_code,
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
            "category": "finished",
            "icon": "✅" if status == "success" else "❌" if status == "failed" else "⏸️",
            "title": "Finished",
            "status": status,
            "seq": seq,
            "timestamp": timestamp,
        }

    if event_name in ("apperror", "error"):
        error_type = payload.get("type") or payload.get("error_type") or "error"
        msg = str(payload.get("message") or error_type)[:300]
        return {
            "type": "error",
            "category": "error",
            "icon": "❌",
            "title": "Execution Error",
            "target": msg,
            "status": "failed",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "error_type": str(error_type)[:100],
                "message": msg,
            },
        }

    if event_name == "cancel":
        return {
            "type": "cancel",
            "category": "interrupted",
            "icon": "⏸️",
            "title": "Cancelled",
            "status": "interrupted",
            "seq": seq,
            "timestamp": timestamp,
        }

    if event_name == "warning":
        return {
            "type": "warning",
            "category": "warning",
            "icon": "⚠️",
            "title": "Warning",
            "target": str(payload.get("message") or "")[:300],
            "status": "warning",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "message": str(payload.get("message") or "")[:300],
            },
        }

    if event_name == "approval":
        return {
            "type": "approval",
            "category": "approval",
            "icon": "⏳",
            "title": "Approval Requested",
            "target": str(payload.get("question") or payload.get("text") or "")[:300],
            "status": "pending",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "question": str(payload.get("question") or payload.get("text") or "")[:300],
            },
        }

    if event_name == "clarify":
        return {
            "type": "clarify",
            "category": "clarify",
            "icon": "❓",
            "title": "Clarification Requested",
            "target": str(payload.get("question") or payload.get("text") or "")[:300],
            "status": "pending",
            "seq": seq,
            "timestamp": timestamp,
            "details": {
                "question": str(payload.get("question") or payload.get("text") or "")[:300],
            },
        }

    # Fallback for unexpected custom event
    return {
        "type": event_name,
        "category": "custom",
        "icon": "📌",
        "title": event_name.replace("_", " ").title(),
        "status": "info",
        "seq": seq,
        "timestamp": timestamp,
    }


def _group_thinking_segments(items: List[dict]) -> List[dict]:
    """Group consecutive thinking_segment items into a single Thinking step."""
    if not items:
        return []

    grouped = []
    thinking_buffer = []

    for item in items:
        if item.get("type") == "thinking_segment":
            thinking_buffer.append(item)
        else:
            if thinking_buffer:
                grouped.append(_flush_thinking_buffer(thinking_buffer))
                thinking_buffer = []
            grouped.append(item)

    if thinking_buffer:
        grouped.append(_flush_thinking_buffer(thinking_buffer))

    return grouped


def _flush_thinking_buffer(buffer: List[dict]) -> dict:
    first = buffer[0]
    last = buffer[-1]
    texts = [b.get("details", {}).get("text", "") for b in buffer if b.get("details", {}).get("text")]
    combined_text = "\n".join(texts)
    summary_line = (combined_text.splitlines()[0] if combined_text else "Planning execution steps...")[:120]

    start_ts = first.get("timestamp", 0)
    end_ts = last.get("timestamp", start_ts)
    duration = round(max(0.0, end_ts - start_ts), 2)

    return {
        "type": "thinking",
        "category": "thinking",
        "icon": "🧠",
        "title": "Thinking",
        "target": summary_line,
        "status": "completed",
        "seq": first.get("seq", 0),
        "timestamp": start_ts,
        "duration_seconds": duration,
        "details": {
            "text": combined_text,
            "segment_count": len(buffer),
        },
    }


def _pair_tool_calls(timeline_items: List[dict]) -> List[dict]:
    """Pair tool and tool_complete events in the timeline and decorate step performance."""
    if not timeline_items:
        return []

    items = _group_thinking_segments(timeline_items)

    result = []
    pending_tools = {}

    for item in items:
        if item["type"] == "tool":
            tool_call_id = item.get("details", {}).get("tool_call_id")
            if tool_call_id:
                pending_tools[tool_call_id] = item
            else:
                result.append(item)
        elif item["type"] == "tool_complete":
            tool_call_id = item.get("details", {}).get("tool_call_id")
            if tool_call_id and tool_call_id in pending_tools:
                tool_event = pending_tools.pop(tool_call_id)
                duration = _format_duration(tool_event["timestamp"], item["timestamp"])
                is_slow = duration >= 30.0

                merged_status = item["status"]
                if is_slow and merged_status == "success":
                    merged_status = "slow"

                merged = {
                    "type": "tool_execution",
                    "category": tool_event.get("category", "custom"),
                    "icon": tool_event.get("icon", "🛠️"),
                    "title": tool_event.get("title", "Tool Execution"),
                    "target": tool_event.get("target") or item.get("target") or "",
                    "status": merged_status,
                    "seq": tool_event["seq"],
                    "timestamp": tool_event["timestamp"],
                    "duration_seconds": duration,
                    "is_slow": is_slow,
                    "details": {
                        **tool_event.get("details", {}),
                        **item.get("details", {}),
                    },
                }
                result.append(merged)
            else:
                result.append(item)
        else:
            result.append(item)

    for tool_call_id, tool_event in pending_tools.items():
        tool_event["status"] = "running"
        tool_event["type"] = "tool_pending"
        result.append(tool_event)

    return result


def build_run_timeline(run_id: str, session_id: Optional[str] = None) -> Optional[dict]:
    """Build a complete Execution Flow for a single run with reliability metrics."""
    summary = find_run_summary(run_id, session_dir=None)
    if not summary:
        return None

    actual_session_id = session_id or summary.get("session_id")
    if not actual_session_id:
        return None

    events_result = read_run_events(actual_session_id, run_id, session_dir=None)
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
            "reliability": {
                "total_steps": 0,
                "failed_steps": 0,
                "slow_steps": 0,
                "status": "unknown",
            },
            "timeline": [],
        }

    raw_items = []
    for event in raw_events:
        normalized = normalize_event(event)
        if normalized:
            raw_items.append(normalized)

    paired_timeline = _pair_tool_calls(raw_items)

    first_event = raw_events[0]
    last_event = raw_events[-1]
    started_at = float(first_event.get("created_at", 0))
    finished_at = float(last_event.get("created_at", started_at))
    total_duration = _format_duration(started_at, finished_at)

    flow_steps = []
    if paired_timeline:
        flow_steps.append({
            "type": "task_start",
            "category": "start",
            "icon": "🚀",
            "title": "Task Started",
            "status": "completed",
            "timestamp": started_at,
            "duration_seconds": 0,
        })
        flow_steps.extend(paired_timeline)

    failed_count = sum(1 for step in flow_steps if step.get("status") in ("failed", "error"))
    slow_count = sum(1 for step in flow_steps if step.get("is_slow") or step.get("duration_seconds", 0) >= 30.0)

    slowest_step = None
    max_dur = 0.0
    for step in flow_steps:
        dur = float(step.get("duration_seconds") or 0.0)
        if dur > max_dur and step.get("category") != "start":
            max_dur = dur
            slowest_step = {
                "title": step.get("title"),
                "target": step.get("target"),
                "duration_seconds": dur,
            }

    tool_call_ids = set()
    for event in raw_events:
        payload = event.get("payload") or {}
        if event.get("event") == "tool":
            tid = payload.get("tool_call_id") or payload.get("id")
            if tid:
                tool_call_ids.add(tid)

    overall_status = _map_run_status({
        "terminal_state": summary.get("terminal_state"),
        "terminal": summary.get("terminal"),
    })

    return {
        "run_id": run_id,
        "session_id": actual_session_id,
        "summary": {
            "status": overall_status,
            "duration_seconds": total_duration,
            "tool_calls": len(tool_call_ids),
            "event_count": len(raw_events),
            "started_at": started_at,
            "finished_at": finished_at,
        },
        "reliability": {
            "total_steps": len(flow_steps),
            "failed_steps": failed_count,
            "slow_steps": slow_count,
            "slowest_step": slowest_step,
            "status": "failed" if failed_count > 0 else "completed",
        },
        "timeline": flow_steps,
    }


def get_run_list() -> dict:
    """Get a list of all runs with summary information."""
    runs = discover_all_runs()
    return {
        "runs": runs,
        "total": len(runs),
    }

