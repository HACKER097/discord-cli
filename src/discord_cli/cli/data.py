"""Data commands — export, purge."""

import json
import os
import sys

import click
from rich.console import Console
import yaml

from ._channels import resolve_channel_id_or_raise
from ._output import dump_json, dump_yaml, emit_error
from ..db import MessageDB

console = Console(stderr=True)


@click.group("data", invoke_without_command=True)
def data_group():
    """Data management commands (registered at top-level)."""
    pass


@data_group.command("export")
@click.argument("channel")
@click.option("-f", "--format", "fmt", type=click.Choice(["text", "json", "yaml"]), default="text")
@click.option("-o", "--output", "output_file", help="Output file path")
@click.option("--hours", type=int, help="Only export last N hours")
def export(channel: str, fmt: str, output_file: str | None, hours: int | None):
    """Export messages from CHANNEL to text or JSON."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel)
        msgs = db.get_recent(channel_id=channel_id, hours=hours, limit=100000)

    if not msgs:
        if fmt in {"json", "yaml"} and output_file is None:
            click.echo(dump_json({"error": "no_messages", "message": f"No messages found for '{channel}'."}))
            raise SystemExit(1) from None
        console.print(f"No messages found for '{channel}'.")
        return

    auto_yaml = (
        fmt == "text"
        and output_file is None
        and os.getenv("OUTPUT", "auto").strip().lower() != "rich"
        and not sys.stdout.isatty()
    )
    if fmt == "json":
        content = dump_json(msgs)
    elif fmt == "yaml" or auto_yaml:
        content = dump_yaml(msgs)
    else:
        lines = []
        for msg in msgs:
            ts = str(msg.get("timestamp", ""))[:19]
            sender = msg.get("sender_name") or "Unknown"
            text = msg.get("content") or ""
            lines.append(f"[{ts}] {sender}: {text}")
        content = "\n".join(lines)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content)
        console.print(f"Exported {len(msgs)} messages to {output_file}")
    else:
        click.echo(content)


@data_group.command("purge")
@click.argument("channel")
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation")
def purge(channel: str, yes: bool):
    """Delete all stored messages for CHANNEL."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel)
        if not yes:
            count = db.count(channel_id)
            if not click.confirm(f"Delete {count} messages from channel {channel_id}?"):
                return

        deleted = db.delete_channel(channel_id)

    console.print(f"Deleted {deleted} messages")
