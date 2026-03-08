"""discord-cli — CLI entry point."""

import click
from rich.console import Console
from rich.table import Table

from .data import data_group
from .discord_cmds import discord_group
from .query import query_group

console = Console()


@click.group()
@click.version_option(package_name="discord-cli")
def cli():
    """discord — CLI for fetching Discord chat history and searching messages."""
    pass


@cli.command("auth")
@click.option("--save", is_flag=True, help="Save found token to .env automatically")
def auth(save: bool):
    """Extract Discord token from local browser/Discord client."""
    from ..auth import find_tokens, save_token_to_env

    console.print("[dim]Scanning for Discord tokens...[/dim]")
    results = find_tokens()

    if not results:
        console.print("[red]No tokens found.[/red]")
        console.print(
            "[dim]Make sure Discord desktop app or browser is logged in.[/dim]"
        )
        return

    table = Table(title=f"Found {len(results)} token(s)")
    table.add_column("#", style="dim", justify="right")
    table.add_column("Source", style="cyan")
    table.add_column("Token", style="bold")

    for i, r in enumerate(results, 1):
        # Show only first/last 8 chars of token for safety
        token = r["token"]
        masked = f"{token[:8]}...{token[-8:]}" if len(token) > 20 else token
        table.add_row(str(i), r["source"], masked)

    console.print(table)

    if save:
        # Use the first token found (Discord App takes priority)
        token = results[0]["token"]
        env_path = save_token_to_env(token)
        console.print(
            f"\n[green]✓[/green] Saved token from {results[0]['source']} to {env_path}"
        )
    else:
        console.print(
            "\n[dim]Run with --save to auto-save to .env, "
            "or copy the token manually.[/dim]"
        )


# Register sub-groups
cli.add_command(discord_group, "dc")

# Register top-level query commands
for name, cmd in query_group.commands.items():
    cli.add_command(cmd, name)

# Register top-level data commands
for name, cmd in data_group.commands.items():
    cli.add_command(cmd, name)
