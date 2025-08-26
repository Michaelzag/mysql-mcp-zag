"""Modern MySQL MCP Server using FastMCP."""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any

from fastmcp import FastMCP
from mysql.connector import Error, connect

# Global configuration
_db_config: dict[str, Any] | None = None


def validate_ssl_file(filepath: str, arg_name: str) -> str:
    """Validate that SSL certificate file exists and is readable."""
    if not filepath:
        return filepath

    path = Path(filepath)
    if not path.exists():
        raise argparse.ArgumentTypeError(
            f"{arg_name}: File '{filepath}' does not exist"
        )

    if not path.is_file():
        raise argparse.ArgumentTypeError(f"{arg_name}: '{filepath}' is not a file")

    if not os.access(filepath, os.R_OK):
        raise argparse.ArgumentTypeError(
            f"{arg_name}: File '{filepath}' is not readable"
        )

    return str(path.resolve())


def ssl_cert_file(value: str) -> str:
    """Custom type for SSL certificate file validation."""
    return validate_ssl_file(value, "SSL certificate")


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for MySQL MCP server."""
    parser = argparse.ArgumentParser(
        prog='mysql-mcp',
        description='MySQL Model Context Protocol (MCP) server built with FastMCP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --host localhost --user admin --password secret --database mydb
  %(prog)s --host db.example.com --port 3306 --database prod --ssl-ca /path/to/ca.pem
  %(prog)s --user myuser --password mypass --database testdb --charset utf8mb4
        """
    )

    # Database connection group
    db_group = parser.add_argument_group('Database Connection')
    db_group.add_argument('--host',
                         default='localhost',
                         help='MySQL server host (default: %(default)s)')
    db_group.add_argument('--port',
                         type=int,
                         default=3306,
                         help='MySQL server port (default: %(default)s)')
    db_group.add_argument('--user',
                         required=True,
                         help='MySQL username (required)')
    db_group.add_argument('--password',
                         required=True,
                         help='MySQL password (required)')
    db_group.add_argument('--database',
                         required=True,
                         help='MySQL database name (required)')

    # SSL configuration group
    ssl_group = parser.add_argument_group('SSL Configuration')
    ssl_group.add_argument('--ssl-ca',
                          type=ssl_cert_file,
                          help='Path to SSL CA certificate file')
    ssl_group.add_argument('--ssl-cert',
                          type=ssl_cert_file,
                          help='Path to SSL client certificate file')
    ssl_group.add_argument('--ssl-key',
                          type=ssl_cert_file,
                          help='Path to SSL client private key file')
    ssl_group.add_argument('--ssl-disabled',
                          action='store_true',
                          help='Disable SSL connection')

    # Advanced options group
    advanced_group = parser.add_argument_group('Advanced Options')
    advanced_group.add_argument('--charset',
                               default='utf8mb4',
                               help='Character set (default: %(default)s)')
    advanced_group.add_argument('--collation',
                               default='utf8mb4_unicode_ci',
                               help='Collation (default: %(default)s)')
    advanced_group.add_argument('--sql-mode',
                               default='TRADITIONAL',
                               help='SQL mode (default: %(default)s)')

    return parser


def validate_ssl_configuration(args: argparse.Namespace) -> None:
    """Validate SSL configuration after argument parsing."""
    if args.ssl_disabled:
        return

    # Check for incomplete SSL client certificate configuration
    ssl_client_files = [args.ssl_cert, args.ssl_key]
    ssl_client_provided = sum(x is not None for x in ssl_client_files)

    if ssl_client_provided == 1:
        if args.ssl_cert and not args.ssl_key:
            raise argparse.ArgumentTypeError(
                "SSL client certificate provided but private key missing "
                "(--ssl-key required)"
            )
        if args.ssl_key and not args.ssl_cert:
            raise argparse.ArgumentTypeError(
                "SSL client private key provided but certificate missing "
                "(--ssl-cert required)"
            )


def create_db_config(args: argparse.Namespace) -> dict[str, Any]:
    """Create database configuration from parsed arguments."""
    config = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
        "charset": args.charset,
        "collation": args.collation,
        "autocommit": True,
        "sql_mode": args.sql_mode,
    }

    # Add SSL configuration if not disabled
    if not args.ssl_disabled:
        if args.ssl_ca:
            config["ssl_ca"] = args.ssl_ca
        if args.ssl_cert:
            config["ssl_cert"] = args.ssl_cert
        if args.ssl_key:
            config["ssl_key"] = args.ssl_key

    return config


def get_db_config() -> dict[str, Any]:
    """Get database configuration."""
    if _db_config is None:
        raise RuntimeError("Database configuration not initialized. Call main() first.")
    return _db_config


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

    Command line arguments:
    Required:
    - --user: MySQL username
    - --password: MySQL password
    - --database: MySQL database name

    Optional:
    - --host: MySQL server host (default: localhost)
    - --port: MySQL server port (default: 3306)
    - --ssl-ca: Path to SSL CA certificate file
    - --ssl-cert: Path to SSL client certificate file
    - --ssl-key: Path to SSL client private key file
    - --ssl-disabled: Disable SSL connection
    - --charset: Character set (default: utf8mb4)
    - --collation: Collation (default: utf8mb4_unicode_ci)
    - --sql-mode: SQL mode (default: TRADITIONAL)
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
        return (
            f"Invalid table name: '{table}'. Table names must contain only "
            "letters, numbers, underscores, and dollar signs."
        )

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
    global _db_config

    try:
        # Parse command line arguments
        parser = create_parser()
        args = parser.parse_args()

        # Validate SSL configuration
        validate_ssl_configuration(args)

        # Create database configuration
        _db_config = create_db_config(args)

        # Test database connection on startup
        with connect(**_db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version_result = cursor.fetchone()
                version = version_result[0] if version_result else "Unknown"  # type: ignore
                print(f"Connected to MySQL {version!s}", flush=True)

                # Show SSL status
                cursor.execute("SHOW STATUS LIKE 'Ssl_cipher'")
                ssl_result = cursor.fetchone()
                if ssl_result and len(ssl_result) > 1:
                    # ssl_result is a tuple from MySQL cursor
                    cipher_value = ssl_result[1]  # type: ignore[index]
                    if cipher_value:
                        cipher = str(cipher_value)
                        print(
                            f"SSL connection established with cipher: {cipher}",
                            flush=True
                        )
                    else:
                        print("Connection established without SSL", flush=True)
                else:
                    print("Connection established without SSL", flush=True)

        # Run the FastMCP server
        mcp.run()

    except argparse.ArgumentTypeError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Error as e:
        print(f"MySQL connection error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nShutting down server...", flush=True)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
