"""
FastMCP Server Example
This example demonstrates how to create a simple FastMCP server with a few tools and resources.
"""

from mcp.server.fastmcp import FastMCP

# from fastmcp import FastMCP

# Create an MCP server
mcp = FastMCP("Demo")


# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


@mcp.resource("echo://{message}")
def echo_resource(message: str) -> str:
    """Echo a message as a resource"""
    return f"Resource echo: {message}"


@mcp.resource("schema://main")
def get_schema() -> str:
    """Provide the database schema as a resource"""

    return "The schema for the database is: "


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"
