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
git clone https://github.com/Michaelzag/mysql-mcp-zag.git
cd mysql-mcp-zag
uv sync
```

## Configuration

The MySQL MCP server accepts the following command line arguments:

### Required Arguments
- `--user`: MySQL username
- `--password`: MySQL password
- `--database`: MySQL database name

### Optional Arguments
- `--host`: MySQL server host (default: localhost)
- `--port`: MySQL server port (default: 3306)
- `--ssl-ca`: Path to SSL CA certificate file
- `--ssl-cert`: Path to SSL client certificate file
- `--ssl-key`: Path to SSL client private key file
- `--ssl-disabled`: Disable SSL connection
- `--charset`: Character set (default: utf8mb4)
- `--collation`: Collation (default: utf8mb4_unicode_ci)
- `--sql-mode`: SQL mode (default: TRADITIONAL)

### SSL Configuration Examples

```bash
# Basic SSL with CA certificate
uvx mysql-mcp-zag --user admin --password secret --database mydb --ssl-ca /path/to/ca.pem

# Full SSL with client certificates
uvx mysql-mcp-zag --user admin --password secret --database mydb \
  --ssl-ca /path/to/ca.pem \
  --ssl-cert /path/to/client-cert.pem \
  --ssl-key /path/to/client-key.pem

# Disable SSL entirely
uvx mysql-mcp-zag --user admin --password secret --database mydb --ssl-disabled
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
      "args": [
        "mysql-mcp-zag",
        "--host", "localhost",
        "--user", "your_user",
        "--password", "your_password",
        "--database", "your_database"
      ]
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
      "args": [
        "--directory", "/path/to/mysql-mcp",
        "run", "mysql-mcp",
        "--host", "localhost",
        "--user", "your_user",
        "--password", "your_password",
        "--database", "your_database"
      ]
    }
  }
}
```

### Direct Usage

#### Using uvx

```bash
uvx mysql-mcp-zag --user your_user --password your_password --database your_database
```

#### Using local installation

```bash
uv run mysql-mcp --user your_user --password your_password --database your_database
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
uv run mysql-mcp --user your_user --password your_password --database your_database
```

## Requirements

- Python 3.13+
- MySQL server
- UV package manager

---

Created by Michael Zag, Michael@MichaelZag.com