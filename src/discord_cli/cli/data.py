"""Data commands — export, purge."""

import json
import os
import sys

import click
from rich.console import Console
import yaml

from ._channels import resolve_channel_id_or_raise
from ._output import default_structured_format, error_payload
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
        structured_fmt = fmt if fmt in {"json", "yaml"} else default_structured_format(as_json=False, as_yaml=False)
        if structured_fmt in {"json", "yaml"} and output_file is None:
            click.echo(
                (
                    json.dumps(error_payload("no_messages", f"No messages found for '{channel}'."), ensure_ascii=False, indent=2, default=str)
                    if structured_fmt == "json"
                    else yaml.safe_dump(error_payload("no_messages", f"No messages found for '{channel}'."), allow_unicode=True, sort_keys=False, default_flow_style=False)
                )
            )
            raise SystemExit(1) from None
        console.print(f"[yellow]No messages found for '{channel}'.[/yellow]")
        return

    auto_yaml = fmt == "text" and output_file is None and os.getenv("OUTPUT", "auto").strip().lower() != "rich" and not sys.stdout.isatty()
    if fmt == "json":
        content = json.dumps(msgs, ensure_ascii=False, indent=2, default=str)
    elif fmt == "yaml" or auto_yaml:
        content = yaml.safe_dump(msgs, allow_unicode=True, sort_keys=False, default_flow_style=False)
    else:
        lines = []
        for msg in msgs:
            ts = (msg.get("timestamp") or "")[:19]
            sender = msg.get("sender_name") or "Unknown"
            text = msg.get("content") or ""
            lines.append(f"[{ts}] {sender}: {text}")
        content = "\n".join(lines)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content)
        console.print(f"[green]✓[/green] Exported {len(msgs)} messages to {output_file}")
    else:
        console.print(content)


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

    console.print(f"[green]✓[/green] Deleted {deleted} messages")
