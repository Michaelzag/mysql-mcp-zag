# MySQL MCP Server

A modern MySQL Model Context Protocol (MCP) server built with FastMCP.

## Features

- Execute SQL queries via MCP tools
- Browse database tables and structure via MCP resources
- SSL certificate support
- Connection pooling and error handling
- Full type safety with Python 3.13+

## Installation

### Using uvx (Recommended)

The easiest way to use MySQL MCP Server is with `uvx`:

```bash
uvx mysql-mcp-zag
```

### From Source (Alternative)

If you prefer to install from source:

```bash
git clone https://github.com/MichaelZag/mysql_mcp.git
cd mysql_mcp
uv sync
```

## Configuration

Set environment variables:

```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=your_user
export MYSQL_PASSWORD=your_password
export MYSQL_DATABASE=your_database

# Optional
export MYSQL_CERT=/path/to/cert.crt  # Supports .crt, .pem, and other SSL certificate formats
export MYSQL_CHARSET=utf8mb4
```

## Usage

### As MCP Server

Configure in your MCP client (e.g., Claude Desktop):

#### Using uvx (Recommended)

```json
{
  "mcpServers": {
    "mysql": {
      "command": "uvx",
      "args": ["mysql-mcp-zag"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_USER": "user",
        "MYSQL_PASSWORD": "password",
        "MYSQL_DATABASE": "database"
      }
    }
  }
}
```

#### Using local installation

```json
{
  "mcpServers": {
    "mysql": {
      "command": "uv",
      "args": ["--directory", "/path/to/mysql-mcp", "run", "mysql-mcp"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_USER": "user",
        "MYSQL_PASSWORD": "password",
        "MYSQL_DATABASE": "database"
      }
    }
  }
}
```

### Direct Usage

#### Using uvx

```bash
uvx mysql-mcp-zag
```

#### Using local installation

```bash
uv run mysql-mcp
```

## Available Tools

- `execute_sql`: Execute SQL queries

## Available Resources

- `mysql://tables`: List all tables
- `mysql://tables/{table}`: Describe table structure

## Development

```bash
# Run tests
uv run pytest

# Lint and format
uv run ruff check --fix src tests
uv run black src tests
uv run mypy src

# Run server locally
uv run mysql-mcp
```

## Requirements

- Python 3.13+
- MySQL server
- UV package manager

---

Created by Michael Zag, Michael@MichaelZag.com