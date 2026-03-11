"""discord-cli — Discord CLI for fetching chat history."""

try:
    from importlib.metadata import version

    __version__ = version("kabi-discord-cli")
except Exception:
    __version__ = "0.0.0"
