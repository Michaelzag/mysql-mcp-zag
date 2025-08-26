"""Tests for MySQL MCP Server."""

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client
from mcp.types import TextResourceContents
from mysql.connector import Error

import mysql_mcp.server as server_module
from mysql_mcp import create_db_config, create_parser
from mysql_mcp.server import get_db_config, mcp


def setup_test_db_config():
    """Helper function to set up database configuration for testing."""
    parser = create_parser()
    args = parser.parse_args([
        "--user", "test_user",
        "--password", "test_password",
        "--database", "test_db"
    ])
    server_module._db_config = create_db_config(args)


class TestDatabaseConfiguration:
    """Test database configuration functionality."""

    def test_create_db_config_with_all_args(self):
        """Test database configuration with all command line arguments."""
        parser = create_parser()
        args = parser.parse_args([
            "--host", "localhost",
            "--port", "3306",
            "--user", "test_user",
            "--password", "test_password",
            "--database", "test_db",
            "--charset", "utf8mb4",
            "--collation", "utf8mb4_unicode_ci"
        ])

        config = create_db_config(args)

        assert config["host"] == "localhost"
        assert config["port"] == 3306
        assert config["user"] == "test_user"
        assert config["password"] == "test_password"
        assert config["database"] == "test_db"
        assert config["charset"] == "utf8mb4"
        assert config["collation"] == "utf8mb4_unicode_ci"
        assert config["autocommit"] is True

    @patch("mysql_mcp.server.validate_ssl_file")
    def test_create_db_config_with_ssl_cert(self, mock_validate):
        """Test database configuration with SSL certificate."""
        mock_validate.return_value = "/path/to/cert.pem"

        parser = create_parser()
        args = parser.parse_args([
            "--user", "test_user",
            "--password", "test_password",
            "--database", "test_db",
            "--ssl-ca", "/path/to/cert.pem"
        ])

        config = create_db_config(args)
        assert config["ssl_ca"] == "/path/to/cert.pem"

    def test_create_db_config_missing_required_args(self):
        """Test database configuration with missing required arguments."""
        parser = create_parser()
        # argparse raises SystemExit for missing required args
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_create_db_config_defaults(self):
        """Test database configuration with default values."""
        parser = create_parser()
        args = parser.parse_args([
            "--user", "test_user",
            "--password", "test_password",
            "--database", "test_db"
        ])

        config = create_db_config(args)
        assert config["host"] == "localhost"
        assert config["port"] == 3306
        assert config["charset"] == "utf8mb4"

    def test_get_db_config_not_initialized(self):
        """Test that get_db_config raises error when not initialized."""
        with pytest.raises(
            RuntimeError, match="Database configuration not initialized"
        ):
            get_db_config()


class TestMCPIntegration:
    """Test FastMCP integration functionality."""

    @pytest.mark.asyncio
    async def test_mcp_server_tools(self):
        """Test that MCP server exposes the correct tools."""
        async with Client(mcp) as client:
            tools = await client.list_tools()

            # Verify execute_sql tool is available
            tool_names = [tool.name for tool in tools]
            assert "execute_sql" in tool_names

            # Get the execute_sql tool
            execute_tool = next(tool for tool in tools if tool.name == "execute_sql")
            assert execute_tool.description is not None
            assert "query" in str(execute_tool.inputSchema)

    @pytest.mark.asyncio
    async def test_mcp_server_resources(self):
        """Test that MCP server exposes the correct resources."""
        async with Client(mcp) as client:
            resources = await client.list_resources()

            # Verify resources are available
            resource_uris = [str(resource.uri) for resource in resources]
            assert "mysql://tables" in resource_uris

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_execute_sql_via_mcp(self, mock_connect):
        """Test executing SQL via MCP client."""
        # Setup database configuration
        setup_test_db_config()

        # Setup mock
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.get_server_info.return_value = "8.0.33"

        # Setup mock for SELECT query
        mock_cursor.description = [("count",)]
        mock_cursor.fetchall.return_value = [(5,)]

        # Call the tool via MCP
        async with Client(mcp) as client:
            result = await client.call_tool("execute_sql", {
                "query": "SELECT COUNT(*) as count FROM users"
            })

            # Verify the result
            assert "count" in result.data
            assert "5" in result.data

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_execute_sql_show_tables_via_mcp(self, mock_connect):
        """Test SHOW TABLES via MCP client."""
        # Setup database configuration
        setup_test_db_config()

        # Setup mock
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.get_server_info.return_value = "8.0.33"

        # Setup mock for SHOW TABLES
        mock_cursor.description = [("Tables_in_test_db",)]
        mock_cursor.fetchall.return_value = [("users",), ("products",)]

        # Call the tool via MCP
        async with Client(mcp) as client:
            result = await client.call_tool("execute_sql", {
                "query": "SHOW TABLES"
            })

            # Verify the result includes header and tables
            lines = result.data.split("\n")
            assert lines[0] == "Tables_in_test_db"
            assert "users" in lines[1]
            assert "products" in lines[2]

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_list_tables_via_mcp(self, mock_connect):
        """Test listing tables via MCP resources."""
        # Setup database configuration
        setup_test_db_config()

        # Setup mock
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.get_server_info.return_value = "8.0.33"

        mock_cursor.fetchall.return_value = [("users",), ("products",)]

        # Read the tables resource
        async with Client(mcp) as client:
            result = await client.read_resource("mysql://tables")

            # Verify the result
            resource = result[0]
            if isinstance(resource, TextResourceContents):
                content = resource.text
            else:
                content = resource.blob  # type: ignore
            assert "Available tables:" in content
            assert "- users" in content
            assert "- products" in content

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_describe_table_via_mcp(self, mock_connect):
        """Test describing a table via MCP resources."""
        # Setup database configuration
        setup_test_db_config()

        # Setup mock
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.get_server_info.return_value = "8.0.33"

        # Mock DESCRIBE and COUNT queries
        describe_result = [
            ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ("name", "varchar(255)", "NO", "", None, ""),
        ]
        count_result = [(50,)]

        mock_cursor.fetchall.return_value = describe_result
        mock_cursor.fetchone.return_value = count_result[0]

        # Read the table description resource
        async with Client(mcp) as client:
            result = await client.read_resource("mysql://tables/users")

            # Verify the result
            resource = result[0]
            if isinstance(resource, TextResourceContents):
                content = resource.text
            else:
                content = resource.blob  # type: ignore
            assert "Table: users" in content
            assert "id: int(11)" in content
            assert "Total rows: 50" in content


class TestErrorHandling:
    """Test comprehensive error handling."""

    def test_database_configuration_error_handling(self):
        """Test that configuration errors are properly handled."""
        # Reset the global configuration to test uninitialized state
        original_config = server_module._db_config
        server_module._db_config = None

        try:
            with pytest.raises(RuntimeError) as exc_info:
                get_db_config()

            assert "Database configuration not initialized" in str(exc_info.value)
        finally:
            # Restore the original configuration
            server_module._db_config = original_config

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_mcp_tool_error_handling(self, mock_connect):
        """Test error handling in MCP tools."""
        # Setup database configuration
        setup_test_db_config()

        mock_connect.side_effect = Error("Connection timeout")

        # This should not raise an exception, but return an error message
        async with Client(mcp) as client:
            result = await client.call_tool("execute_sql", {"query": "SELECT 1"})

            assert "MySQL error" in result.data

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_mcp_resource_error_handling(self, mock_connect):
        """Test error handling in MCP resources."""
        # Setup database configuration
        setup_test_db_config()

        mock_connect.side_effect = Error("Access denied")

        # This should not raise an exception, but return an error message
        async with Client(mcp) as client:
            result = await client.read_resource("mysql://tables")

            resource = result[0]
            if isinstance(resource, TextResourceContents):
                content = resource.text
            else:
                content = resource.blob  # type: ignore
            assert "MySQL error" in content

    @pytest.mark.asyncio
    @patch("mysql_mcp.server.connect")
    async def test_insert_query_via_mcp(self, mock_connect):
        """Test INSERT query via MCP client."""
        # Setup database configuration
        setup_test_db_config()

        # Setup mock
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.get_server_info.return_value = "8.0.33"

        # Setup mock for INSERT query (no description = non-SELECT)
        mock_cursor.description = None
        mock_cursor.rowcount = 1

        # Call the tool via MCP
        async with Client(mcp) as client:
            result = await client.call_tool("execute_sql", {
                "query": (
                    "INSERT INTO users (name, email) "
                    "VALUES ('Test User', 'test@example.com')"
                )
            })

            # Verify the result message
            assert "Query executed successfully. 1 rows affected." in result.data

            # Verify the connection was committed
            mock_connection.commit.assert_called_once()
