"""Shared structured output helpers for discord-cli.

Design principles (Agentic CLI):
- Default output is compact & line-oriented — easy for AI agents to parse.
- --json / --yaml emit clean data, NO wrapper by default.
- --full restores the old wrapped {"ok": true, "data": ...} envelope.
- --compact forces ultra-compact line output.
- Progress / errors go to stderr. Stdout is the API contract.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Callable

import click
import yaml

_SCHEMA_VERSION = "1"
_OUTPUT_ENV = "OUTPUT"


def _output_mode_from_env() -> str:
    """OUTPUT=rich|json|yaml|compact|full (default: compact)."""
    mode = os.getenv(_OUTPUT_ENV, "auto").strip().lower()
    if mode in ("json", "yaml", "compact", "rich", "full"):
        return mode
    return "compact"


def resolve_output_mode(
    *,
    as_json: bool,
    as_yaml: bool,
    as_compact: bool,
    as_full: bool,
) -> str:
    """Resolve explicit flags → env → TTY default."""
    # Explicit flags take highest priority
    flags = [(as_json, "json"), (as_yaml, "yaml"), (as_compact, "compact"), (as_full, "full")]
    active = [mode for active, mode in flags if active]
    if len(active) > 1:
        raise click.UsageError(f"Use only one output flag. Got: {', '.join(active)}")
    if active:
        return active[0]

    env = _output_mode_from_env()
    if env != "auto":
        return env

    if sys.stdout.isatty():
        return "rich"
    return "compact"


def structured_output_options(command: Callable) -> Callable:
    """Add --json/--yaml/--compact/--full to a Click command."""
    command = click.option("--full", "as_full", is_flag=True, help="Verbose wrapped output with schema_version envelope")(command)
    command = click.option("--compact", "as_compact", is_flag=True, help="Ultra-compact line-oriented output")(command)
    command = click.option("--yaml", "as_yaml", is_flag=True, help="Output as YAML")(command)
    command = click.option("--json", "as_json", is_flag=True, help="Output as JSON")(command)
    return command


def dump_json(data: Any, *, indent: int | None = 2) -> str:
    return json.dumps(data, ensure_ascii=False, indent=indent, default=str)


def dump_yaml(data: Any) -> str:
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)


def wrap_full(data: Any) -> dict[str, Any]:
    """Old-style envelope for --full / backward compat."""
    if isinstance(data, dict) and data.get("schema_version") == _SCHEMA_VERSION and "ok" in data:
        return data
    return {"ok": True, "schema_version": _SCHEMA_VERSION, "data": data}


def wrap_error(code: str, message: str, *, details: Any | None = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        err["details"] = details
    return {"ok": False, "schema_version": _SCHEMA_VERSION, "error": err}


# ---------------------------------------------------------------------------
# Emit helpers
# ---------------------------------------------------------------------------

def emit(
    data: Any,
    *,
    as_json: bool = False,
    as_yaml: bool = False,
    as_compact: bool = False,
    as_full: bool = False,
) -> str | None:
    """Serialize data and return the string (or None for rich mode).

    The caller should print the returned string to stdout.
    """
    mode = resolve_output_mode(
        as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full
    )

    if mode == "json":
        return dump_json(data)
    if mode == "yaml":
        return dump_yaml(data)
    if mode == "full":
        return dump_json(wrap_full(data))
    if mode == "compact":
        return _to_compact(data)
    # rich → caller handles with tables/colors
    return None


def emit_and_exit(data: Any, *, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool) -> bool:
    """Emit structured/compact output and return True when handled.

    Returns False only when mode == 'rich' (caller must render).
    """
    out = emit(data, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full)
    if out is not None:
        click.echo(out)
        return True
    return False


def emit_error(
    code: str,
    message: str,
    *,
    as_json: bool | None = None,
    as_yaml: bool | None = None,
    as_compact: bool | None = None,
    as_full: bool | None = None,
    details: Any | None = None,
) -> bool:
    """Emit a structured error when the active output mode is machine-readable.

    Returns True if an error payload was emitted (structured modes).
    Returns False for rich mode (caller should print to stderr).
    """
    ctx = click.get_current_context(silent=True)
    params = ctx.params if ctx is not None else {}

    as_json = bool(params.get("as_json", False)) if as_json is None else as_json
    as_yaml = bool(params.get("as_yaml", False)) if as_yaml is None else as_yaml
    as_compact = bool(params.get("as_compact", False)) if as_compact is None else as_compact
    as_full = bool(params.get("as_full", False)) if as_full is None else as_full

    mode = resolve_output_mode(
        as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full
    )

    if mode == "full":
        payload = wrap_error(code, message, details=details)
        click.echo(dump_json(payload))
        return True

    if mode == "json":
        err: dict[str, Any] = {"error": code, "message": message}
        if details is not None:
            err["details"] = details
        click.echo(dump_json(err))
        return True

    if mode == "yaml":
        err = {"error": code, "message": message}
        if details is not None:
            err["details"] = details
        click.echo(dump_yaml(err))
        return True

    if mode == "compact":
        click.echo(f"ERR {code}: {message}")
        return True

    return False


# ---------------------------------------------------------------------------
# Compact formatters
# ---------------------------------------------------------------------------

def _to_compact(data: Any) -> str:
    """Convert any data structure to ultra-compact lines."""
    if data is None:
        return ""
    if isinstance(data, list):
        if not data:
            return "(empty)"
        lines = []
        for item in data:
            lines.append(_compact_item(item))
        return "\n".join(lines)
    if isinstance(data, dict):
        return _compact_item(data)
    return str(data)


def _compact_item(item: Any) -> str:
    """Format a single record compactly."""
    if isinstance(item, dict):
        # Message record
        if "msg_id" in item or "content" in item:
            return _compact_message(item)
        # Guild record
        if "name" in item and "owner" in item and "id" in item:
            return _compact_guild(item)
        # Channel record
        if "name" in item and "topic" in item:
            return _compact_channel(item)
        # Member record
        if "username" in item:
            return _compact_member(item)
        # Stats / generic dict
        return "  ".join(f"{k}={_compact_val(v)}" for k, v in item.items())
    return str(item)


def _compact_message(m: dict) -> str:
    ts = str(m.get("timestamp", ""))[:19]
    sender = m.get("sender_name") or m.get("username") or "?"
    content = (m.get("content") or "").replace("\n", " ")
    ch = m.get("channel_name", "")
    prefix = f"#{ch} " if ch else ""
    return f"{ts} {prefix}{sender}: {content}"


def _compact_guild(g: dict) -> str:
    owner = "(owner)" if g.get("owner") else ""
    return f"{g['id']}  {g['name']} {owner}".rstrip()


def _compact_channel(c: dict) -> str:
    topic = (c.get("topic") or "")[:40]
    extra = f"  {topic}" if topic else ""
    return f"{c['id']}  #{c['name']}{extra}"


def _compact_member(m: dict) -> str:
    name = m.get("global_name") or m.get("username") or "?"
    bot = " [bot]" if m.get("bot") else ""
    nick = f" (nick: {m['nick']})" if m.get("nick") else ""
    return f"{m['id']}  @{m.get('username', '?')}  {name}{nick}{bot}"


def _compact_val(v: Any) -> str:
    if v is None:
        return "-"
    s = str(v).replace("\n", " ")
    return s[:80] if len(s) > 80 else s
