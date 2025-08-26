"""Modern MySQL MCP Server using FastMCP."""

import os
import re
from typing import Any

from fastmcp import FastMCP
from mysql.connector import Error, connect


def get_db_config() -> dict[str, Any]:
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
        "charset": os.getenv("MYSQL_CHARSET", "utf8mb4"),
        "collation": os.getenv("MYSQL_COLLATION", "utf8mb4_unicode_ci"),
        "autocommit": True,
        "sql_mode": os.getenv("MYSQL_SQL_MODE", "TRADITIONAL"),
    }

    # Add SSL cert configuration if provided
    ssl_cert = os.getenv("MYSQL_CERT")
    if ssl_cert:
        config["ssl_ca"] = ssl_cert

    # Remove None values to let MySQL connector use defaults if not specified
    config = {k: v for k, v in config.items() if v is not None}

    if not all([config.get("user"), config.get("password"), config.get("database")]):
        raise ValueError(
            "Missing required database configuration. "
            "Please set MYSQL_USER, MYSQL_PASSWORD, and MYSQL_DATABASE "
            "environment variables."
        )

    return config


def validate_table_name(table_name: str) -> bool:
    """Validate that a table name is safe to use in SQL queries.
    
    Args:
        table_name: The table name to validate
        
    Returns:
        True if the table name is valid, False otherwise
    """
    # MySQL table names can contain letters, numbers, underscores, and dollar signs
    # They cannot start with a number and have length limits
    if not table_name or len(table_name) > 64:
        return False
    
    # Check for valid MySQL identifier pattern
    pattern = r'^[a-zA-Z_$][a-zA-Z0-9_$]*$'
    return bool(re.match(pattern, table_name))


def table_exists(table_name: str, config: dict[str, Any]) -> bool:
    """Check if a table exists in the database.
    
    Args:
        table_name: The table name to check
        config: Database configuration
        
    Returns:
        True if the table exists, False otherwise
    """
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                return cursor.fetchone() is not None
    except Error:
        return False


# Create the FastMCP server
mcp = FastMCP(
    name="MySQL MCP Server",
    instructions="""
    This server provides MySQL database access through the Model Context Protocol.

    Available tools:
    - execute_sql: Execute SQL queries on the MySQL database

    Available resources:
    - mysql://tables: List all available tables
    - mysql://tables/{table}: Get detailed information about a specific table

    Environment variables required:
    - MYSQL_HOST: MySQL server host (default: localhost)
    - MYSQL_PORT: MySQL server port (default: 3306)
    - MYSQL_USER: MySQL username
    - MYSQL_PASSWORD: MySQL password
    - MYSQL_DATABASE: MySQL database name

    Optional environment variables:
    - MYSQL_CERT: Path to SSL certificate file
    - MYSQL_CHARSET: Character set (default: utf8mb4)
    - MYSQL_COLLATION: Collation (default: utf8mb4_unicode_ci)
    - MYSQL_SQL_MODE: SQL mode (default: TRADITIONAL)
    """,
)


@mcp.tool
def execute_sql(query: str) -> str:
    """Execute an SQL query on the MySQL server.

Args:
        query: The SQL query to execute

Returns:
        Query results as formatted text or success message for non-SELECT queries
    """
    config = get_db_config()

    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

                # Handle different query types
                if cursor.description:
                    # Query returns results (SELECT, SHOW, DESCRIBE, etc.)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    if not rows:
                        return "Query executed successfully. No results returned."

                    # Format as CSV-like output
                    result_lines = [",".join(columns)]
                    result_lines.extend([",".join(map(str, row)) for row in rows])

                    return "\n".join(result_lines)
                else:
                    # Non-SELECT query (INSERT, UPDATE, DELETE, etc.)
                    conn.commit()
                    return (
                        f"Query executed successfully. "
                        f"{cursor.rowcount} rows affected."
                    )

    except Error as e:
        return f"MySQL error: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


@mcp.resource("mysql://tables")
def list_tables() -> str:
    """List all available tables in the database."""
    config = get_db_config()

    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()

                if not tables:
                    return "No tables found in the database."

                table_list = []
                for table in tables:
                    if table and len(table) > 0:
                        # table is a sequence/tuple from MySQL cursor
                        table_name = str(table[0])  # type: ignore[index]
                        table_list.append(f"- {table_name}")
                return "Available tables:\n" + "\n".join(table_list)

    except Error as e:
        return f"MySQL error: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


@mcp.resource("mysql://tables/{table}")
def describe_table(table: str) -> str:
    """Get detailed information about a specific table.

Args:
        table: The name of the table to describe

Returns:
        Table structure and information
    """
    # Validate table name to prevent SQL injection
    if not validate_table_name(table):
        return f"Invalid table name: '{table}'. Table names must contain only letters, numbers, underscores, and dollar signs."
    
    config = get_db_config()
    
    # Check if table exists before proceeding
    if not table_exists(table, config):
        return f"Table '{table}' not found in the database."

    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                # Get table structure - now safe since we validated the table name
                cursor.execute(f"DESCRIBE `{table}`")  # nosec B608
                columns = cursor.fetchall()

                if not columns:
                    return f"Table '{table}' not found or has no columns."

                # Format table structure
                result = [f"Table: {table}", "=" * 50, ""]
                result.append("Columns:")

                for col in columns:
                    field, type_, null, key, default, extra = col
                    null_str = "NULL" if null == "YES" else "NOT NULL"
                    key_str = f" ({key!s})" if key else ""
                    default_str = f" DEFAULT {default!s}" if default is not None else ""
                    extra_str = f" {extra!s}" if extra else ""

                    col_info = f"{null_str}{key_str}{default_str}{extra_str}"
                    result.append(f"  - {field!s}: {type_!s} {col_info}")

                # Get row count - now safe since we validated the table name
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")  # nosec B608
                count_result = cursor.fetchone()
                row_count = count_result[0] if count_result else 0  # type: ignore
                result.extend(["", f"Total rows: {row_count!s}"])

                return "\n".join(result)

    except Error as e:
        return f"MySQL error: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


def main() -> None:
    """Main entry point for running the MCP server."""
    try:
        # Test database connection on startup
        config = get_db_config()
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version_result = cursor.fetchone()
                version = version_result[0] if version_result else "Unknown"  # type: ignore
                print(f"Connected to MySQL {version!s}", flush=True)

        # Run the FastMCP server
        mcp.run()

    except ValueError as e:
        print(f"Configuration error: {e}", flush=True)
        exit(1)
    except Error as e:
        print(f"MySQL connection error: {e}", flush=True)
        exit(1)
    except KeyboardInterrupt:
        print("\nShutting down server...", flush=True)
    except Exception as e:
        print(f"Unexpected error: {e}", flush=True)
        exit(1)


if __name__ == "__main__":
    main()
