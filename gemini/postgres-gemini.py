import psycopg2
import google.generativeai as genai
import json

# PostgreSQL connection details
PG_CONFIG = {
    "dbname": "DB_NAME",
    "user": "DB_USER",
    "password": "PASS",
    "host": "HOST",
    "port": "PORT",
}

# Gemini API configuration
genai.configure(api_key="YOUR_API_KEY")
model = genai.GenerativeModel("gemini-2.0-flash")

def get_database_schema(dbname, user, password, host, port):
    """
    Retrieves the database schema (tables, columns, and relationships) from PostgreSQL.
    """
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()

        # Query to get table names
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = [row[0] for row in cursor.fetchall()]

        schema_description = "Database Schema:\n\n"
        for table in tables:
            schema_description += f"Table: {table}\n"
            # Query to get column names and data types for each table
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table}'")
            columns = cursor.fetchall()
            schema_description += "  Columns:\n"
            for column_name, data_type in columns:
                schema_description += f"    {column_name} ({data_type})\n"

            # Query to get foreign key relationships
            cursor.execute(f"""
                SELECT
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table_name,
                    ccu.column_name AS referenced_column_name
                FROM
                    information_schema.table_constraints AS tc
                JOIN
                    information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_name = kcu.table_name
                JOIN
                    information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{table}';
                """)
            foreign_keys = cursor.fetchall()
            if foreign_keys:
                schema_description += "  Foreign Keys:\n"
                for fk in foreign_keys:
                    schema_description += f"    {fk[1]} references {fk[3]} in {fk[2]}\n"
            schema_description += "\n"
        return schema_description

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_data_from_postgres(query):
    """Executes a SQL query and returns the results."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def generate_response_with_gemini(prompt, chat_history=None):
    """Generates a response using the Gemini API, optionally with chat history."""
    if chat_history:
        response = model.generate_content(prompt, history=chat_history)
    else:
        response = model.generate_content(prompt)
    return response.text

def process_query(natural_language_query, chat_history=None):
    """
    Processes a natural language query:
    1.  Retrieves the database schema.
    2.  Translates the natural language query to SQL using Gemini, providing the schema and chat history.
    3.  Executes the SQL query on PostgreSQL.
    4.  Uses Gemini to generate a natural language response, providing the data and chat history.
    """
    # 1. Retrieve the database schema
    db_schema = get_database_schema(
        PG_CONFIG["dbname"],
        PG_CONFIG["user"],
        PG_CONFIG["password"],
        PG_CONFIG["host"],
        PG_CONFIG["port"],
    )

    if not db_schema:
        return "Error: Could not retrieve database schema."

    # 2. Generate SQL, providing the schema to Gemini
    sql_prompt = f"""
    You are a helpful assistant that can translate natural language queries into SQL queries.
    You should use the provided database schema and any relevant chat history to answer the user's question.
    
    Here is the schema of the PostgreSQL database:
    {db_schema}
    
    Here is the chat history:
    {chat_history}
    
    Translate the following natural language query into a SQL query that can be executed on the database.
    Do not include any explanation or context, only the SQL query.
    
    Natural language query: {natural_language_query}
    """
    sql_query = generate_response_with_gemini(sql_prompt)
    print(f"Generated SQL Query: {sql_query}")

    # Remove any markdown code block indicators from the generated SQL
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    # 3. Execute SQL on PostgreSQL
    data = get_data_from_postgres(sql_query)
    print(f"Data from PostgreSQL: {data}")

    # 4. Generate Natural Language Response, providing the data to Gemini
    if data:
        response_prompt = f"Generate a natural language response based on the following data: {json.dumps(data, indent=2)}.  Also use the chat history to make the response more contextual: {chat_history}"
        final_response = generate_response_with_gemini(response_prompt)
        return final_response
    else:
        return "I could not retrieve the information."


if __name__ == "__main__":
    chat_history = []  # Initialize chat history

    print("Welcome to the Chat-Enabled MCP Server!")
    print("You can now ask questions about the 'menu' database.")
    print("Type 'exit' to quit.")

    while True:
        # Get user input
        natural_language_query = input("Enter your natural language query: ")

        if natural_language_query.lower() == "exit":
            print("Exiting chat...")
            break

        response = process_query(natural_language_query, chat_history)
        print(f"Final Response: {response}")

        # Update chat history.  For simplicity, we'll just append the user query and the response.  A more robust implementation might have a maximum history length.
        chat_history.append({"role": "user", "parts": [{"text": natural_language_query}]})
        chat_history.append({"role": "model", "parts": [{"text": response}]})
