import streamlit as st
import duckdb
import pandas as pd

# IMPORTANT: Update this URL to the publicly accessible HTTPS URL 
# (e.g., https://yourdomain.com/my_duckdb.db)
DUCKDB_URL = "https://cs.wellesley.edu/~eni/duckdb/2023_wiki_views.duckdb"

# --- Functions to interact with DuckDB ---

@st.cache_resource
def get_db_connection():
    """Establishes and caches the DuckDB connection via HTTPS/HTTPFS."""
    try:
        # 1. Connect to an in-memory database first
        conn = duckdb.connect(database=':memory:', read_only=False) 
        
        # 2. Install and Load the HTTPFS extension
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # 3. Use the URL in a READ_ONLY command
        conn.execute(f"ATTACH '{DUCKDB_URL}' AS remote_db (READ_ONLY)")
        conn.execute("USE remote_db;") 
        
        return conn
    except Exception as e:
        st.error(f"Error connecting to DuckDB via HTTPFS: {e}")
        st.error("Make sure your database file is publicly accessible and the URL is correct.")
        st.stop()
        return None

def get_table_list(conn):
    """Retrieves a list of tables and their row counts."""
    query_tables = "SHOW ALL TABLES;"
    tables_df = conn.execute(query_tables).fetchdf()

    table_data = []
    for table_name in tables_df['name']:
        # Get the row count for each table
        row_count_query = f"SELECT count(*) FROM \"{table_name}\";"
        row_count = conn.execute(row_count_query).fetchone()[0]
        table_data.append({'Table Name': table_name, 'Row Count': row_count})

    return pd.DataFrame(table_data)

def get_sample_data(conn, table_name, limit=10):
    """Retrieves a sample of rows from the specified table."""
    try:
        query = f"SELECT * FROM \"{table_name}\" LIMIT {limit};"
        sample_df = conn.execute(query).fetchdf()
        return sample_df
    except Exception as e:
        st.error(f"Error fetching sample data: {e}")
        return pd.DataFrame()

def get_column_stats(conn, table_name, column_name):
    """Retrieves summary statistics for a selected column."""
    
    # Get column data type
    column_info_query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND column_name = '{column_name}';
    """
    column_type = conn.execute(column_info_query).fetchdf()['data_type'].iloc[0]

    # Decide which unique count query to run based on data type
    if 'VARCHAR' in column_type or 'CHAR' in column_type or 'STRING' in column_type:
        # FOR STRING/TEXT COLUMNS: Aggressive normalization to remove hidden characters
        # 1. REGEXP_REPLACE removes all non-printable/control characters (like \r, \n, \0).
        # 2. TRIM removes standard leading/trailing spaces.
        # 3. LOWER ensures case-insensitivity.
        distinct_count_expression = f"COUNT(DISTINCT TRIM(LOWER(REGEXP_REPLACE(\"{column_name}\", '[^\\x20-\\x7E]', '', 'g'))))"
        # The regex '[^\\x20-\\x7E]' matches any character outside the standard printable ASCII range.
    else:
        # FOR NUMERIC/DATE/BOOLEAN COLUMNS: Standard distinct count is sufficient.
        distinct_count_expression = f"COUNT(DISTINCT \"{column_name}\")"

    # Get unique value count and total non-NULL values
    stats_query = f"""
    SELECT 
        COUNT(*) AS total_rows, 
        {distinct_count_expression} AS unique_values,
        COUNT(\"{column_name}\") AS non_null_count
    FROM \"{table_name}\";
    """
    stats_df = conn.execute(stats_query).fetchdf()

    # --- DEBUGGING OUTPUT: Check the raw query result ---
    # We will move this output display outside of this function to keep data processing clean.
    # For now, we return the raw DataFrame as well for external display.
    
    # Calculate additional statistics
    total_rows = stats_df['total_rows'].iloc[0]
    unique_count = stats_df['unique_values'].iloc[0]
    non_null_count = stats_df['non_null_count'].iloc[0]
    null_count = total_rows - non_null_count
    
    stats = {
        "Data Type": column_type,
        "Total Rows in Table": total_rows,
        "Unique Values (Aggressively Normalized)": unique_count,
        "Non-NULL Values": non_null_count,
        "NULL Values": null_count,
        "Uniqueness Ratio": f"{unique_count / total_rows * 100:.2f}%" if total_rows > 0 else "N/A"
    }

    return stats, stats_df # Return both the formatted stats and the raw stats_df

# --- Streamlit App Layout ---

st.title("🦆 DuckDB Explorer")
st.markdown(f"**Database File:** `{DUCKDB_URL}`")

conn = get_db_connection()

if conn:
    
    # --- Section 1: Table List and Row Counts ---
    
    st.header("1. Database Tables & Row Counts")
    
    table_df = get_table_list(conn)
    st.dataframe(table_df, use_container_width=True, hide_index=True)
    
    st.divider()

    # --- Section 2: Column Selection and Summary Statistics ---
    
    st.header("2. Column Analysis")
    
    # 2a. Table Selection
    available_tables = table_df['Table Name'].tolist()
    if not available_tables:
        st.warning("No tables found in the database.")
        st.stop()

    selected_table = st.selectbox(
        "Select a table to analyze:",
        available_tables
    )

    # --- NEW SECTION: Sample Data View ---
    st.subheader(f"2A. First 10 Rows of Table: `{selected_table}`")
    sample_data = get_sample_data(conn, selected_table)
    st.dataframe(sample_data, use_container_width=True)
    
    st.divider()
    
    st.subheader(f"2B. Summary Statistics for Selected Column")
    
    # 2b. Column Selection
    # Fetch column names for the selected table
    columns_query = f"PRAGMA table_info('{selected_table}');"
    columns_df = conn.execute(columns_query).fetchdf()
    column_names = columns_df['name'].tolist()

    if column_names:
        selected_column = st.selectbox(
            f"Select a column from **{selected_table}**:",
            column_names
        )

        # 2c. Display Statistics
        st.subheader(f"Statistics for Column: `{selected_column}`")

        # Get and format the stats, and the raw stats_df
        stats, stats_df = get_column_stats(conn, selected_table, selected_column)
        
        # Display DEBUGGING output
        st.subheader("🔍 DEBUG: Raw Statistics DataFrame (`stats_df`)")
        st.markdown("This shows the direct result from the DuckDB query.")
        st.code(stats_df.to_string(), language='text')
        
        # Display final formatted stats
        st.subheader("Formatted Column Statistics")
        stats_df_display = pd.DataFrame(list(stats.items()), columns=['Metric', 'Value'])
        
        st.table(stats_df_display)