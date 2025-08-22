"""Test configuration and fixtures for MySQL MCP Server tests."""

import os
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client

from mysql_mcp.server import mcp


@pytest.fixture
def mock_env_vars() -> Generator[None]:
    """Mock environment variables for testing."""
    with patch.dict(os.environ, {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_password",
        "MYSQL_DATABASE": "test_db",
        "MYSQL_CHARSET": "utf8mb4",
        "MYSQL_COLLATION": "utf8mb4_unicode_ci",
    }):
        yield


@pytest.fixture
def mock_mysql_connection():
    """Mock MySQL connection and cursor."""
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connection.cursor.return_value.__exit__.return_value = None

    with patch("mysql_mcp.server.connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connect.return_value.__exit__.return_value = None
        yield mock_connection, mock_cursor


@pytest.fixture
async def test_client():
    """Create a test client for the MCP server."""
    async with Client(mcp) as client:
        yield client


@pytest.fixture
def sample_table_data():
    """Sample table data for testing."""
    return [
        ("users",),
        ("products",),
        ("orders",),
    ]


@pytest.fixture
def sample_table_structure():
    """Sample table structure for testing."""
    return [
        ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
        ("name", "varchar(255)", "NO", "", None, ""),
        ("email", "varchar(255)", "YES", "UNI", None, ""),
        ("created_at", "timestamp", "NO", "", "CURRENT_TIMESTAMP", ""),
    ]


@pytest.fixture
def sample_query_results():
    """Sample query results for testing."""
    return [
        (1, "John Doe", "john@example.com", "2024-01-01 10:00:00"),
        (2, "Jane Smith", "jane@example.com", "2024-01-02 11:00:00"),
        (3, "Bob Johnson", "bob@example.com", "2024-01-03 12:00:00"),
    ]
