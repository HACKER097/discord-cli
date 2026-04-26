"""discord-cli — CLI entry point."""

import logging

import click
from rich.console import Console

from .data import data_group
from .discord_cmds import discord_group
from ._output import emit_and_exit, emit_error, structured_output_options, dump_json
from .query import query_group

console = Console(stderr=True)


def _discord_user_payload(user: dict) -> dict[str, object]:
    """Normalize Discord user info for structured agent output."""
    return {
        "id": user.get("id", ""),
        "name": user.get("global_name") or user.get("username", ""),
        "username": user.get("username", ""),
        "global_name": user.get("global_name") or "",
        "email": user.get("email") or "",
        "phone": user.get("phone") or "",
        "mfa_enabled": bool(user.get("mfa_enabled", False)),
        "premium_type": user.get("premium_type", 0),
        "created_at": user.get("created_at", ""),
    }


@click.group()
@click.version_option(package_name="kabi-discord-cli")
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def cli(verbose: bool):
    """discord — CLI for fetching Discord chat history and searching messages."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(name)s: %(message)s")


@cli.command("auth")
@click.option("--save", is_flag=True, help="Save found token to .env automatically")
def auth(save: bool):
    """Extract Discord token from local browser/Discord client."""
    import httpx

    from ..auth import find_tokens, save_token_to_env

    console.print(
        "Warning: discord-cli uses a Discord user token from your local "
        "session. This may violate Discord's terms or trigger account restrictions. "
        "Use it only on accounts you control and at your own risk."
    )
    console.print("Scanning for Discord tokens...")
    results = find_tokens()

    if not results:
        console.print("No tokens found. Make sure Discord desktop app or browser is logged in.")
        return

    console.print(f"Found {len(results)} candidate token(s), validating...")

    valid_token = None
    valid_source = None
    user_info = None

    for r in results:
        token = r["token"]
        try:
            resp = httpx.get(
                "https://discord.com/api/v10/users/@me",
                headers={"Authorization": token},
                timeout=10.0,
            )
            if resp.status_code == 200:
                user_info = resp.json()
                valid_token = token
                valid_source = r["source"]
                break
        except Exception:
            continue

    if not valid_token or not user_info:
        console.print("No valid token found. All tokens returned 401.")
        console.print("Try logging into Discord in your browser and retry.")
        return

    masked = f"{valid_token[:8]}...{valid_token[-8:]}"
    username = user_info.get("username", "?")
    global_name = user_info.get("global_name") or username
    click.echo(f"Valid token from {valid_source}: {masked}")
    click.echo(f"Logged in as: {global_name} (@{username})")

    if save:
        env_path = save_token_to_env(valid_token)
        click.echo(f"Saved to {env_path}")
    else:
        click.echo("Run with --save to auto-save to .env")


@cli.command("status")
@structured_output_options
def status(as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Check if Discord token is valid."""
    import sys

    import httpx

    from ..config import get_token
    from ..exceptions import NotAuthenticatedError

    try:
        token = get_token()
    except NotAuthenticatedError as e:
        payload = emit_error("not_authenticated", str(e))
        if payload:
            sys.exit(1)
        console.print(f"Not authenticated: {e}")
        sys.exit(1)

    try:
        resp = httpx.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": token},
            timeout=10.0,
        )
        if resp.status_code == 200:
            user = resp.json()
            payload = {
                "authenticated": True,
                "user": _discord_user_payload(user),
            }
            if emit_and_exit(payload, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
                sys.exit(0)
            name = user.get("global_name") or user.get("username", "?")
            click.echo(f"Authenticated as {name} (@{user.get('username')})")
            sys.exit(0)
        else:
            payload = {
                "authenticated": False,
                "error": f"Token invalid (HTTP {resp.status_code})",
                "status_code": resp.status_code,
            }
            if emit_and_exit(payload, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
                sys.exit(1)
            click.echo(f"Token invalid (HTTP {resp.status_code})")
            sys.exit(1)
    except Exception as e:
        payload = {"authenticated": False, "error": str(e)}
        if emit_and_exit(payload, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
            sys.exit(1)
        click.echo(f"Connection error: {e}")
        sys.exit(1)


@cli.command("whoami")
@structured_output_options
def whoami(as_json: bool, as_yaml: bool, as_compact: bool, as_full: bool):
    """Show detailed profile of the current user."""
    import asyncio

    from ..client import get_client, get_me

    async def _run():
        async with get_client() as client:
            return await get_me(client)

    try:
        info = asyncio.run(_run())
    except Exception as exc:
        if emit_error("auth_error", str(exc)):
            raise SystemExit(1) from None
        raise click.ClickException(str(exc)) from exc

    payload = {"user": _discord_user_payload(info)}
    if emit_and_exit(payload, as_json=as_json, as_yaml=as_yaml, as_compact=as_compact, as_full=as_full):
        return

    premium_names = {0: "None", 1: "Nitro Classic", 2: "Nitro", 3: "Nitro Basic"}
    click.echo(f"username: @{info['username']}")
    if info.get("global_name"):
        click.echo(f"display:  {info['global_name']}")
    click.echo(f"id:       {info['id']}")
    if info.get("email"):
        click.echo(f"email:    {info['email']}")
    if info.get("phone"):
        click.echo(f"phone:    {info['phone']}")
    click.echo(f"mfa:      {'yes' if info.get('mfa_enabled') else 'no'}")
    click.echo(f"nitro:    {premium_names.get(info.get('premium_type', 0), '?')}")
    click.echo(f"created:  {info.get('created_at', '?')[:10]}")


# Register sub-groups
cli.add_command(discord_group, "dc")

# Register top-level query commands
for name, cmd in query_group.commands.items():
    cli.add_command(cmd, name)

# Register top-level data commands
for name, cmd in data_group.commands.items():
    cli.add_command(cmd, name)
