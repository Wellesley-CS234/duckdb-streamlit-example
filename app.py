import streamlit as st
import duckdb
import pandas as pd

# --- Configuration ---

DUCKDB_URL = "https://cs.wellesley.edu/~eni/duckdb/wiki_climate.duckdb"

# --- Functions to interact with DuckDB ---

@st.cache_resource
def get_db_connection():
    """Establishes and caches the DuckDB connection via HTTPS/HTTPFS."""
    try:
        # 1. Connect to an in-memory database first
        conn = duckdb.connect(database=':memory:', read_only=False) 
        
        # 2. Install and Load the HTTPFS extension
        # DuckDB requires the extension to read remote files
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # 3. Use the URL in a READ_ONLY command
        # This tells DuckDB to treat the remote URL as the database
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

def get_column_stats(conn, table_name, column_name):
    """Retrieves summary statistics for a selected column."""
    
    # Get column data type
    column_info_query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND column_name = '{column_name}';
    """
    column_type = conn.execute(column_info_query).fetchdf()['data_type'].iloc[0]

    # Get unique value count and total non-NULL values
    stats_query = f"""
    SELECT 
        COUNT(*) AS total_rows, 
        COUNT(DISTINCT \"{column_name}\") AS unique_values,
        COUNT(\"{column_name}\") AS non_null_count
    FROM \"{table_name}\";
    """
    stats_df = conn.execute(stats_query).fetchdf()

    # Calculate additional statistics
    total_rows = stats_df['total_rows'].iloc[0]
    unique_count = stats_df['unique_values'].iloc[0]
    non_null_count = stats_df['non_null_count'].iloc[0]
    null_count = total_rows - non_null_count
    
    stats = {
        "Data Type": column_type,
        "Total Rows in Table": total_rows,
        "Unique Values": unique_count,
        "Non-NULL Values": non_null_count,
        "NULL Values": null_count,
        "Uniqueness Ratio": f"{unique_count / total_rows * 100:.2f}%" if total_rows > 0 else "N/A"
    }

    return stats

# --- Streamlit App Layout ---

st.title("🦆 DuckDB Explorer")
st.markdown(f"**Database File:** `{DUCKDB_PATH}`")

conn = get_db_connection()

if conn:
    
    # --- Section 1: Table List and Row Counts ---
    
    st.header("1. Database Tables & Row Counts")
    
    table_df = get_table_list(conn)
    st.dataframe(table_df, use_container_width=True, hide_index=True)
    
    st.divider()

    # --- Section 2: Column Selection and Summary Statistics ---
    
    st.header("2. Column Summary Statistics")
    
    # 2a. Table Selection
    available_tables = table_df['Table Name'].tolist()
    if not available_tables:
        st.warning("No tables found in the database.")
        st.stop()

    selected_table = st.selectbox(
        "Select a table to analyze:",
        available_tables
    )

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

        # Get and format the stats
        stats = get_column_stats(conn, selected_table, selected_column)
        
        # Convert stats dict to a DataFrame for clean display
        stats_df_display = pd.DataFrame(list(stats.items()), columns=['Metric', 'Value'])
        
        st.table(stats_df_display)