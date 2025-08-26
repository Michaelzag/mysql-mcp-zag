"""MySQL MCP Server - A modern MySQL MCP server built with FastMCP."""

from .server import (
    create_db_config,
    create_parser,
    mcp,
    table_exists,
    validate_table_name,
)

__version__ = "0.1.0"
__all__ = [
    "mcp",
    "table_exists",
    "validate_table_name",
    "create_db_config",
    "create_parser"
]
