"""Tests for MySQL MCP Server."""

import os
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client
from mcp.types import TextResourceContents
from mysql.connector import Error

from mysql_mcp.server import get_db_config, mcp, validate_table_name, table_exists


class TestDatabaseConfiguration:
    """Test database configuration functionality."""

    @patch.dict(os.environ, {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db",
        "MYSQL_CHARSET": "utf8mb4",
        "MYSQL_COLLATION": "utf8mb4_unicode_ci"
    })
    def test_get_db_config_with_all_vars(self):
        """Test database configuration with all environment variables."""
        config = get_db_config()

        assert config["host"] == "localhost"
        assert config["port"] == 3306
        assert config["user"] == "test_user"
        assert config["password"] == "test_password"
        assert config["database"] == "test_db"
        assert config["charset"] == "utf8mb4"
        assert config["collation"] == "utf8mb4_unicode_ci"
        assert config["autocommit"] is True

    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db",
        "MYSQL_CERT": "/path/to/cert.pem"
    })
    def test_get_db_config_with_ssl_cert(self):
        """Test database configuration with SSL certificate."""
        config = get_db_config()
        assert config["ssl_ca"] == "/path/to/cert.pem"

    @patch.dict(os.environ, {}, clear=True)
    def test_get_db_config_missing_required_vars(self):
        """Test database configuration with missing required variables."""
        with pytest.raises(ValueError, match="Missing required database configuration"):
            get_db_config()

    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    }, clear=True)
    def test_get_db_config_defaults(self):
        """Test database configuration with default values."""
        config = get_db_config()
        assert config["host"] == "localhost"
        assert config["port"] == 3306
        assert config["charset"] == "utf8mb4"


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
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_execute_sql_via_mcp(self, mock_connect):
        """Test executing SQL via MCP client."""
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
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_execute_sql_show_tables_via_mcp(self, mock_connect):
        """Test SHOW TABLES via MCP client."""
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
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_list_tables_via_mcp(self, mock_connect):
        """Test listing tables via MCP resources."""
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
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_describe_table_via_mcp(self, mock_connect):
        """Test describing a table via MCP resources."""
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


class TestTableValidation:
    """Test table name validation and security features."""

    def test_validate_table_name_valid_names(self):
        """Test that valid table names pass validation."""
        valid_names = [
            "users",
            "user_profiles",
            "UserProfiles",
            "table123",
            "_private_table",
            "$system_table",
            "a",  # Single character
            "table_with_underscores_123",
        ]
        
        for name in valid_names:
            assert validate_table_name(name), f"Valid table name '{name}' should pass validation"

    def test_validate_table_name_invalid_names(self):
        """Test that invalid table names fail validation."""
        invalid_names = [
            "",  # Empty string
            "123table",  # Starts with number
            "table-name",  # Contains hyphen
            "table name",  # Contains space
            "table.name",  # Contains dot
            "table;DROP TABLE users;--",  # SQL injection attempt
            "table`",  # Contains backtick
            "table'",  # Contains single quote
            "table\"",  # Contains double quote
            "table()",  # Contains parentheses
            "a" * 65,  # Too long (over 64 characters)
            "table@name",  # Contains @ symbol
            "table#name",  # Contains # symbol
        ]
        
        for name in invalid_names:
            assert not validate_table_name(name), f"Invalid table name '{name}' should fail validation"

    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    def test_table_exists_true(self, mock_connect):
        """Test table_exists returns True when table exists."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock table exists
        mock_cursor.fetchone.return_value = ("users",)
        
        config = get_db_config()
        assert table_exists("users", config) is True
        
        # Verify the query was called with parameterized query
        mock_cursor.execute.assert_called_once_with("SHOW TABLES LIKE %s", ("users",))

    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    def test_table_exists_false(self, mock_connect):
        """Test table_exists returns False when table doesn't exist."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock table doesn't exist
        mock_cursor.fetchone.return_value = None
        
        config = get_db_config()
        assert table_exists("nonexistent", config) is False

    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    def test_table_exists_error_handling(self, mock_connect):
        """Test table_exists handles database errors gracefully."""
        mock_connect.side_effect = Error("Connection failed")
        
        config = get_db_config()
        assert table_exists("users", config) is False


class TestSecurityFeatures:
    """Test security-related functionality."""

    @pytest.mark.asyncio
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    async def test_describe_table_invalid_name_rejection(self):
        """Test that describe_table rejects invalid table names."""
        async with Client(mcp) as client:
            result = await client.read_resource("mysql://tables/invalid;DROP TABLE users;--")
            
            resource = result[0]
            if isinstance(resource, TextResourceContents):
                content = resource.text
            else:
                content = resource.blob  # type: ignore
            
            assert "Invalid table name" in content
            assert "DROP TABLE" in content  # Shows the rejected malicious input

    @pytest.mark.asyncio
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.table_exists")
    async def test_describe_table_nonexistent_table(self, mock_table_exists):
        """Test that describe_table handles nonexistent tables."""
        mock_table_exists.return_value = False
        
        async with Client(mcp) as client:
            result = await client.read_resource("mysql://tables/nonexistent")
            
            resource = result[0]
            if isinstance(resource, TextResourceContents):
                content = resource.text
            else:
                content = resource.blob  # type: ignore
            
            assert "Table 'nonexistent' not found in the database" in content

    @pytest.mark.asyncio
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.table_exists")
    @patch("mysql_mcp.server.connect")
    async def test_describe_table_with_valid_name(self, mock_connect, mock_table_exists):
        """Test that describe_table works with valid table names after security checks."""
        # Setup mocks
        mock_table_exists.return_value = True
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock DESCRIBE query result
        describe_result = [
            ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ("name", "varchar(255)", "NO", "", None, ""),
        ]
        count_result = (50,)
        
        mock_cursor.fetchall.return_value = describe_result
        mock_cursor.fetchone.return_value = count_result
        
        async with Client(mcp) as client:
            result = await client.read_resource("mysql://tables/users")
            
            resource = result[0]
            if isinstance(resource, TextResourceContents):
                content = resource.text
            else:
                content = resource.blob  # type: ignore
            
            assert "Table: users" in content
            assert "id: int(11)" in content
            assert "Total rows: 50" in content
            
            # Verify security checks were called
            mock_table_exists.assert_called_once()


class TestErrorHandling:
    """Test comprehensive error handling."""

    def test_database_configuration_error_handling(self):
        """Test that configuration errors are properly handled."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                get_db_config()

            assert "Missing required database configuration" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_mcp_tool_error_handling(self, mock_connect):
        """Test error handling in MCP tools."""
        mock_connect.side_effect = Error("Connection timeout")

        # This should not raise an exception, but return an error message
        async with Client(mcp) as client:
            result = await client.call_tool("execute_sql", {"query": "SELECT 1"})

            assert "MySQL error" in result.data

    @pytest.mark.asyncio
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_mcp_resource_error_handling(self, mock_connect):
        """Test error handling in MCP resources."""
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
    @patch.dict(os.environ, {
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db"
    })
    @patch("mysql_mcp.server.connect")
    async def test_insert_query_via_mcp(self, mock_connect):
        """Test INSERT query via MCP client."""
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
