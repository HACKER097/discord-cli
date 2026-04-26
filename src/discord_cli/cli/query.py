"""Query commands — search, stats, today, top, timeline."""

from collections import defaultdict

import click
from rich.console import Console

from ._channels import resolve_channel_id_or_raise
from ._output import emit_and_exit, structured_output_options
from ..db import MessageDB

console = Console(stderr=True)


@click.group("query", invoke_without_command=True)
def query_group():
    """Query and analysis commands (registered at top-level)."""
    pass


@query_group.command("search")
@click.argument("keyword")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("-n", "--limit", default=50, help="Max results")
@structured_output_options
def search(keyword: str, channel: str | None, limit: int,
           as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Search stored messages by KEYWORD."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.search(keyword, channel_id=channel_id, limit=limit)

    if emit_and_exit(results, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    if not results:
        console.print("No messages found.")
        return

    for msg in results:
        ts = str(msg.get("timestamp", ""))[:19]
        sender = msg.get("sender_name") or "Unknown"
        content = (msg.get("content") or "").replace("\n", " ")
        ch = msg.get("channel_name", "")
        prefix = f"#{ch} " if ch else ""
        click.echo(f"{ts} {prefix}{sender}: {content}")
    click.echo(f"\n{len(results)} messages")


@query_group.command("recent")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("--hours", type=int, help="Only show messages from last N hours")
@click.option("-n", "--limit", default=50, help="Show last N messages")
@structured_output_options
def recent(channel: str | None, hours: int | None, limit: int,
           as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show the most recent stored messages."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.get_latest(channel_id=channel_id, hours=hours, limit=limit)

    if emit_and_exit(results, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    if not results:
        console.print("No recent messages found.")
        return

    show_channel = channel_id is None
    for msg in results:
        ts = str(msg.get("timestamp", ""))[:19]
        sender = msg.get("sender_name") or "Unknown"
        content = (msg.get("content") or "").replace("\n", " ")
        ch = msg.get("channel_name", "")
        prefix = f"#{ch} " if show_channel and ch else ""
        click.echo(f"{ts} {prefix}{sender}: {content}")
    click.echo(f"\n{len(results)} messages")


@query_group.command("stats")
@structured_output_options
def stats(as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show message statistics per channel."""
    with MessageDB() as db:
        channels = db.get_channels()
        total = db.count()

    payload = {"total": total, "channels": channels}
    if emit_and_exit(payload, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    click.echo(f"Total messages: {total}")
    for c in channels:
        ch_id = str(c["channel_id"])
        short_id = (ch_id[-6:] + "…") if len(ch_id) > 6 else ch_id
        name = f"#{c['channel_name']}" if c["channel_name"] else "—"
        guild = c.get("guild_name") or "—"
        first = (c["first_msg"] or "")[:10]
        last = (c["last_msg"] or "")[:10]
        click.echo(f"{short_id}  {name}  {guild}  count={c['msg_count']}  {first}..{last}")


@query_group.command("today")
@click.option("-c", "--channel", help="Filter by channel name")
@structured_output_options
def today(channel: str | None, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show today's messages, grouped by channel."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        msgs = db.get_today(channel_id=channel_id)

    if emit_and_exit(msgs, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    if not msgs:
        console.print("No messages today.")
        return

    grouped: dict[str, list[dict]] = defaultdict(list)
    for m in msgs:
        key = f"#{m.get('channel_name') or 'unknown'}"
        if m.get("guild_name"):
            key = f"{m['guild_name']} > {key}"
        grouped[key].append(m)

    for ch_label, ch_msgs in sorted(grouped.items(), key=lambda x: -len(x[1])):
        click.echo(f"\n--- {ch_label} ({len(ch_msgs)}) ---")
        for m in ch_msgs:
            ts = str(m.get("timestamp", ""))[11:19]
            sender = m.get("sender_name") or "Unknown"
            content = (m.get("content") or "").replace("\n", " ")
            click.echo(f"  {ts} {sender}: {content}")
    click.echo(f"\n{len(msgs)} messages today")


@query_group.command("top")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("--hours", type=int, help="Only count messages within N hours")
@click.option("-n", "--limit", default=20, help="Top N senders")
@structured_output_options
def top(channel: str | None, hours: int | None, limit: int,
        as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show most active senders."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.top_senders(channel_id=channel_id, hours=hours, limit=limit)

    if emit_and_exit(results, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    if not results:
        console.print("No sender data found.")
        return

    for i, r in enumerate(results, 1):
        first = (r["first_msg"] or "")[:10]
        last = (r["last_msg"] or "")[:10]
        click.echo(f"{i}. {r['sender_name']}  {r['msg_count']} msgs  {first}..{last}")


@query_group.command("timeline")
@click.option("-c", "--channel", help="Filter by channel name")
@click.option("--hours", type=int, help="Only show last N hours")
@click.option("--by", "granularity", type=click.Choice(["day", "hour"]), default="day")
@structured_output_options
def timeline(channel: str | None, hours: int | None, granularity: str,
             as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show message activity over time as a bar chart."""
    with MessageDB() as db:
        channel_id = resolve_channel_id_or_raise(db, channel) if channel else None
        results = db.timeline(channel_id=channel_id, hours=hours, granularity=granularity)

    if emit_and_exit(results, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    if not results:
        console.print("No timeline data.")
        return

    max_count = max(r["msg_count"] for r in results)
    bar_width = 30

    for r in results:
        period = r["period"]
        count = r["msg_count"]
        bar_len = int(count / max_count * bar_width) if max_count > 0 else 0
        bar = "█" * bar_len
        click.echo(f"{period} {bar} {count}")
