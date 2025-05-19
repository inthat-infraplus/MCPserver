import psycopg2
import json
from loguru import logger
#from mcp.server import FastMCP
from mcp.server.fastmcp import FastMCP
# Create MCP server
mcp = FastMCP("postgres-server")

@mcp.tool()  # decorator to register the function as a tool
def query_data(sql_query: str) -> str:
    """
    Execute Postgres SQL query for table inside the "menu" database
    and return the result as a JSON response.
    """
    logger.info(f"Executing SQL query: {sql_query}")

    DB_NAME = "menu"
    DB_USER = "postgres"
    DB_PASS = "1234"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    # --- Connection and Execution ---
    conn = None
    cursor = None

    try:
        # 1. Establish a connection to the database
        logger.info(f"Connecting to database: {DB_NAME}")  # Use logger
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
        )
        logger.info("Connection successful.")  # Use logger

        # 2. Create a cursor object
        cursor = conn.cursor()

        # 3. Execute the SQL query
        logger.info(f"Executing SQL query: {sql_query}")  # Use logger
        cursor.execute(sql_query)

        # 4. Fetch the results
        # fetchall() retrieves all rows from the result set of a query
        rows = cursor.fetchall()
        logger.debug(f"Query results: {rows}") # Use logger
        return json.dumps(rows, indent=2)  # Convert the result to a JSON string and return

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return json.dumps({"error": str(e)}, indent=2) # Return JSON error

    finally:
        # 5. Close the cursor and connection
        if cursor is not None:
            cursor.close()
            logger.info("Cursor closed.")  # Use logger
        if conn is not None:
            conn.close()
            logger.info("Connection closed.")  # Use logger

    logger.info("Function finished.") #Add logger
    return json.dumps([], indent=2) #Return empty list in case of error.

if __name__ == "__main__":
    logger.info("Starting server...")
    # initialize the server
    mcp.run(transport="stdio")  # run mcp tool
