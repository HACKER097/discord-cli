"""Discord subcommands — guilds, channels, history, sync, sync-all, search, members."""

import asyncio
from contextlib import suppress

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..client import (
    fetch_messages,
    get_client,
    get_dm_channel,
    get_guild_info,
    list_channels,
    list_guilds,
    list_members,
    resolve_guild_id,
    search_guild_messages,
)
from ..db import MessageDB
from ._output import emit_and_exit, emit_error, structured_output_options

console = Console(stderr=True)


async def _fetch_channel_context(client, channel_id: str) -> dict[str, str | None]:
    """Resolve channel and guild names for a channel."""
    channel_name = None
    guild_name = None
    guild_id = None

    with suppress(Exception):
        response = await client.get(f"/channels/{channel_id}")
        if response.status_code == 200:
            data = response.json()
            channel_name = data.get("name")
            guild_id = data.get("guild_id")
            if guild_id:
                guild = await get_guild_info(client, guild_id)
                if guild:
                    guild_name = guild.get("name")

    return {
        "channel_name": channel_name,
        "guild_name": guild_name,
        "guild_id": guild_id,
    }


def _annotate_messages(messages: list[dict], context: dict[str, str | None]) -> list[dict]:
    """Attach channel and guild metadata to fetched messages."""
    for msg in messages:
        msg["guild_id"] = context.get("guild_id")
        msg["guild_name"] = context.get("guild_name")
        msg["channel_name"] = context.get("channel_name")
    return messages


async def _tail_fetch_once(
    client,
    db: MessageDB,
    channel_id: str,
    *,
    after: str | None,
    fetch_limit: int,
    context: dict[str, str | None],
    store: bool,
) -> tuple[list[dict], str | None, int]:
    """Fetch a single incremental batch for tail mode."""
    messages = await fetch_messages(client, channel_id, limit=fetch_limit, after=after)
    if not messages:
        return [], after, 0

    _annotate_messages(messages, context)
    inserted = db.insert_batch(messages) if store else 0
    return messages, messages[-1]["msg_id"], inserted


@click.group("dc")
def discord_group():
    """Discord operations — list servers, fetch history, sync."""
    pass


# ---------------------------------------------------------------------------
# guilds
# ---------------------------------------------------------------------------

@discord_group.command("guilds")
@structured_output_options
def dc_guilds(as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """List joined Discord servers."""

    async def _run():
        async with get_client() as client:
            return await list_guilds(client)

    guilds = asyncio.run(_run())

    if emit_and_exit(guilds, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for g in guilds:
        owner = "(owner)" if g["owner"] else ""
        click.echo(f"{g['id']}  {g['name']} {owner}".rstrip())
    click.echo(f"\n{len(guilds)} servers")


# ---------------------------------------------------------------------------
# channels
# ---------------------------------------------------------------------------

@discord_group.command("channels")
@click.argument("guild")
@structured_output_options
def dc_channels(guild: str, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """List text channels in a GUILD (server ID or name)."""

    async def _run():
        async with get_client() as client:
            guild_id = await resolve_guild_id(client, guild)
            if not guild_id:
                if emit_error("guild_not_found", f"Guild '{guild}' not found."):
                    raise SystemExit(1) from None
                console.print(f"[red]Guild '{guild}' not found.[/red]")
                return []
            return await list_channels(client, guild_id)

    channels = asyncio.run(_run())
    if not channels:
        return

    if emit_and_exit(channels, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for ch in channels:
        topic = (ch.get("topic") or "")[:40]
        extra = f"  {topic}" if topic else ""
        click.echo(f"{ch['id']}  #{ch['name']}{extra}")
    click.echo(f"\n{len(channels)} channels")


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------

@discord_group.command("history")
@click.argument("channel")
@click.option("-n", "--limit", default=1000, help="Max messages to fetch")
@click.option("--guild-name", help="Guild name to store with messages")
@click.option("--channel-name", help="Channel name to store with messages")
@structured_output_options
def dc_history(channel: str, limit: int, guild_name: str | None, channel_name: str | None,
               as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Fetch historical messages from CHANNEL (channel ID)."""

    async def _run():
        with MessageDB() as db:
            async with get_client() as client:
                context = await _fetch_channel_context(client, channel)
                if channel_name:
                    context["channel_name"] = channel_name
                elif context.get("channel_name") is None:
                    context["channel_name"] = channel

                if guild_name:
                    context["guild_name"] = guild_name

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console,
                ) as progress:
                    task = progress.add_task(
                        f"Fetching messages from {context.get('channel_name') or channel}...",
                        total=None,
                    )
                    messages = await fetch_messages(client, channel, limit=limit)
                    progress.update(task, description=f"Fetched {len(messages)} messages")

                _annotate_messages(messages, context)
                inserted = db.insert_batch(messages)
                return messages, len(messages), inserted

    messages, total, inserted = asyncio.run(_run())

    if emit_and_exit(messages, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for msg in messages:
        ts = str(msg.get("timestamp", ""))[:19]
        sender = msg.get("sender_name") or "Unknown"
        content = (msg.get("content") or "").replace("\n", " ")
        click.echo(f"{ts} {sender}: {content}")
    click.echo(f"\n{total} fetched, {inserted} new stored")


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------

@discord_group.command("sync")
@click.argument("channel")
@click.option("-n", "--limit", default=5000, help="Max messages per sync")
@structured_output_options
def dc_sync(channel: str, limit: int, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Incremental sync — fetch only new messages from CHANNEL."""

    async def _run():
        with MessageDB() as db:
            last_id = db.get_last_msg_id(channel)
            if last_id:
                console.print(f"Syncing from msg_id > {last_id}...")

            async with get_client() as client:
                context = await _fetch_channel_context(client, channel)

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console,
                ) as progress:
                    task_id = progress.add_task(
                        f"Syncing {context.get('channel_name') or channel}...",
                        total=None,
                    )
                    messages = await fetch_messages(client, channel, limit=limit, after=last_id)
                    progress.update(task_id, description=f"Fetched {len(messages)} new messages")

                _annotate_messages(messages, context)
                inserted = db.insert_batch(messages)
                return messages, len(messages), inserted

    messages, total, inserted = asyncio.run(_run())

    if emit_and_exit(messages, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for msg in messages:
        ts = str(msg.get("timestamp", ""))[:19]
        sender = msg.get("sender_name") or "Unknown"
        content = (msg.get("content") or "").replace("\n", " ")
        click.echo(f"{ts} {sender}: {content}")
    click.echo(f"\nSynced {total}, stored {inserted} new")


# ---------------------------------------------------------------------------
# tail
# ---------------------------------------------------------------------------

@discord_group.command("tail")
@click.argument("channel")
@click.option("-n", "--limit", default=20, help="Show last N messages before following")
@click.option("--interval", default=5.0, type=click.FloatRange(min=0.5), help="Polling interval in seconds")
@click.option("--poll-limit", default=100, type=click.IntRange(1, 100), help="Max new messages fetched per poll")
@click.option("--store/--no-store", default=True, help="Store tailed messages in local SQLite")
@click.option("--once", is_flag=True, help="Show initial snapshot and exit")
def dc_tail(channel: str, limit: int, interval: float, poll_limit: int, store: bool, once: bool):
    """Tail a channel and follow new messages."""

    async def _run():
        with MessageDB() as db:
            async with get_client() as client:
                context = await _fetch_channel_context(client, channel)
                channel_label = context.get("channel_name") or channel
                guild_label = context.get("guild_name")
                scope = f"{guild_label} > #{channel_label}" if guild_label else f"#{channel_label}"
                last_id = db.get_last_msg_id(channel)

                if limit > 0:
                    initial = await fetch_messages(client, channel, limit=limit)
                    _annotate_messages(initial, context)
                    if store and initial:
                        db.insert_batch(initial)
                    for msg in initial:
                        ts = str(msg.get("timestamp", ""))[:19]
                        sender = msg.get("sender_name") or "Unknown"
                        content = (msg.get("content") or "").replace("\n", " ")
                        click.echo(f"{ts} {sender}: {content}")
                    if initial:
                        last_id = initial[-1]["msg_id"]
                elif last_id is None:
                    latest = await fetch_messages(client, channel, limit=1)
                    if latest:
                        _annotate_messages(latest, context)
                        if store:
                            db.insert_batch(latest)
                        last_id = latest[-1]["msg_id"]

                if once:
                    return

                console.print(
                    f"\nWatching {scope} (poll every {interval:g}s, Ctrl-C to stop)"
                )

                while True:
                    messages, last_id, inserted = await _tail_fetch_once(
                        client,
                        db,
                        channel,
                        after=last_id,
                        fetch_limit=poll_limit,
                        context=context,
                        store=store,
                    )
                    for msg in messages:
                        ts = str(msg.get("timestamp", ""))[:19]
                        sender = msg.get("sender_name") or "Unknown"
                        content = (msg.get("content") or "").replace("\n", " ")
                        click.echo(f"{ts} {sender}: {content}")
                    if messages and store:
                        console.print(f"+{inserted} stored")
                    await asyncio.sleep(interval)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\nStopped tailing.")


# ---------------------------------------------------------------------------
# sync-all
# ---------------------------------------------------------------------------

@discord_group.command("sync-all")
@click.option("-n", "--limit", default=5000, help="Max messages per channel")
def dc_sync_all(limit: int):
    """Sync ALL channels in the database."""

    async def _run():
        with MessageDB() as db:
            async with get_client() as client:
                guilds = await list_guilds(client)
                channels: list[dict[str, str | None]] = []
                for guild in guilds:
                    guild_channels = await list_channels(client, guild["id"])
                    for channel in guild_channels:
                        channels.append(
                            {
                                "guild_id": guild["id"],
                                "guild_name": guild["name"],
                                "channel_id": channel["id"],
                                "channel_name": channel["name"],
                            }
                        )

                if not channels:
                    console.print("No text channels found for this account.")
                    return {}

                console.print(f"Discovered {len(channels)} channels across {len(guilds)} guilds. Syncing...")

                results: dict[str, int] = {}
                for ch in channels:
                    ch_id = ch["channel_id"]
                    ch_name = ch.get("channel_name") or ch_id
                    last_id = db.get_last_msg_id(ch_id)
                    try:
                        messages = await fetch_messages(client, ch_id, limit=limit, after=last_id)
                        for msg in messages:
                            msg["guild_name"] = ch.get("guild_name")
                            msg["channel_name"] = ch.get("channel_name")
                        inserted = db.insert_batch(messages)
                        results[ch_name] = inserted
                        if inserted > 0:
                            console.print(f"  + {ch_name}: {inserted}")
                        else:
                            console.print(f"  = {ch_name}: no new messages")
                    except Exception as e:
                        console.print(f"  ! {ch_name}: {e}")
                        results[ch_name] = 0
                return results

    results = asyncio.run(_run())
    total_new = sum(results.values())
    console.print(f"\nSynced {total_new} new messages across {len(results)} channels")


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

@discord_group.command("search")
@click.argument("guild")
@click.argument("keyword")
@click.option("-c", "--channel", help="Filter by channel ID")
@click.option("-n", "--limit", default=25, help="Max results")
@structured_output_options
def dc_search(guild: str, keyword: str, channel: str | None, limit: int,
              as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Search messages in a GUILD by KEYWORD (Discord native search)."""

    async def _run():
        async with get_client() as client:
            guild_id = await resolve_guild_id(client, guild)
            if not guild_id:
                if emit_error("guild_not_found", f"Guild '{guild}' not found."):
                    raise SystemExit(1) from None
                console.print(f"[red]Guild '{guild}' not found.[/red]")
                return []
            return await search_guild_messages(client, guild_id, keyword, channel_id=channel, limit=limit)

    results = asyncio.run(_run())

    if not results:
        if emit_and_exit([], as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
            return
        console.print("No messages found.")
        return

    if emit_and_exit(results, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for msg in results:
        ts = str(msg.get("timestamp", ""))[:19]
        sender = msg.get("sender_name") or "Unknown"
        content = (msg.get("content") or "").replace("\n", " ")
        ch = msg.get("channel_name", "")
        prefix = f"#{ch} " if ch else ""
        click.echo(f"{ts} {prefix}{sender}: {content}")
    click.echo(f"\n{len(results)} messages")


# ---------------------------------------------------------------------------
# members
# ---------------------------------------------------------------------------

@discord_group.command("members")
@click.argument("guild")
@click.option("-n", "--max", "limit", default=50, help="Max members to list")
@structured_output_options
def dc_members(guild: str, limit: int, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """List members of a GUILD (server)."""

    async def _run():
        async with get_client() as client:
            guild_id = await resolve_guild_id(client, guild)
            if not guild_id:
                if emit_error("guild_not_found", f"Guild '{guild}' not found."):
                    raise SystemExit(1) from None
                console.print(f"[red]Guild '{guild}' not found.[/red]")
                return []
            return await list_members(client, guild_id, limit=limit)

    members = asyncio.run(_run())

    if not members:
        if emit_and_exit([], as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
            return
        console.print("No members found (may require Privileged Intents).")
        return

    if emit_and_exit(members, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for m in members:
        name = m.get("global_name") or m.get("username") or "?"
        bot = " [bot]" if m.get("bot") else ""
        nick = f" (nick: {m['nick']})" if m.get("nick") else ""
        click.echo(f"{m['id']}  @{m.get('username', '?')}  {name}{nick}{bot}")
    click.echo(f"\n{len(members)} members")


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------

@discord_group.command("info")
@click.argument("guild")
@structured_output_options
def dc_info(guild: str, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show detailed info about a GUILD (server)."""

    async def _run():
        async with get_client() as client:
            guild_id = await resolve_guild_id(client, guild)
            if not guild_id:
                if emit_error("guild_not_found", f"Could not find guild: {guild}"):
                    raise SystemExit(1) from None
                return None
            return await get_guild_info(client, guild_id)

    info = asyncio.run(_run())
    if not info:
        console.print(f"Could not find guild: {guild}")
        return

    if emit_and_exit(info, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    for k, v in info.items():
        click.echo(f"{k}: {v if v is not None else '-'}")


# ---------------------------------------------------------------------------
# dm
# ---------------------------------------------------------------------------

@discord_group.command("dm")
@click.argument("user_id")
@structured_output_options
def dc_dm(user_id: str, as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Get or create a DM channel ID for a USER (by user ID)."""

    async def _run():
        async with get_client() as client:
            return await get_dm_channel(client, user_id)

    channel = asyncio.run(_run())

    if not channel:
        if emit_error("dm_failed", f"Could not open DM channel with user '{user_id}'."):
            raise SystemExit(1) from None
        console.print(f"Could not open DM channel with user '{user_id}'.")
        raise SystemExit(1)

    if emit_and_exit(channel, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    click.echo(f"{channel['id']}  dm_with_user={user_id}")
