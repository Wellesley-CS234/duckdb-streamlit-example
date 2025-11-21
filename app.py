import streamlit as st
import duckdb
import pandas as pd

DUCKDB_URL = "https://cs.wellesley.edu/~eni/duckdb/2024_wiki_views.duckdb"

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

def get_distinct_sample(conn, table_name, distinct_expression, limit=5):
    """Retrieves a sample of distinct values using the normalization expression."""
    try:
        # Build a query that applies the normalization and then gets distinct values
        query = f"""
        SELECT 
            {distinct_expression} AS normalized_value 
        FROM \"{table_name}\" 
        WHERE \"{distinct_expression.split('(')[0].strip('"')}\" IS NOT NULL -- Safely retrieve column name
        GROUP BY 1
        LIMIT {limit};
        """
        distinct_df = conn.execute(query).fetchdf()
        return distinct_df
    except Exception as e:
        st.warning(f"Could not run distinct sample query: {e}")
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
    
    # Define the expression for normalization and distinct counting
    distinct_expression_raw = f"\"{column_name}\""

    if 'VARCHAR' in column_type or 'CHAR' in column_type or 'STRING' in column_type:
        # Aggressive normalization
        normalized_column_expression = f"TRIM(LOWER(REGEXP_REPLACE(\"{column_name}\", '[^\\x20-\\x7E]', '', 'g')))"
        
        # --- NEW STRATEGY: Use a subquery with GROUP BY for accurate counting ---
        # The COUNT(DISTINCT) function might be relying on incomplete metadata via HTTPFS.
        # GROUP BY forces the engine to scan and group all unique, normalized values.
        
        # 1. Get total rows and non-null count (standard)
        base_stats_query = f"""
        SELECT 
            COUNT(*) AS total_rows, 
            COUNT(\"{column_name}\") AS non_null_count
        FROM \"{table_name}\";
        """
        base_stats_df = conn.execute(base_stats_query).fetchdf()
        
        # 2. Get accurate distinct count using GROUP BY
        distinct_count_query = f"""
        SELECT 
            COUNT(*) AS unique_values
        FROM 
            (SELECT {normalized_column_expression} AS distinct_group FROM \"{table_name}\" GROUP BY 1) AS subquery;
        """
        distinct_count = conn.execute(distinct_count_query).fetchone()[0]
        
        # Merge results into a single stats_df for consistency
        stats_df = base_stats_df.copy()
        stats_df['unique_values'] = distinct_count
        
        distinct_expression_normalized = normalized_column_expression
        
    else:
        # FOR NUMERIC/DATE/BOOLEAN COLUMNS: Standard distinct count is sufficient.
        distinct_count_expression = f"COUNT(DISTINCT {distinct_expression_raw})"
        stats_query = f"""
        SELECT 
            COUNT(*) AS total_rows, 
            {distinct_count_expression} AS unique_values,
            COUNT(\"{column_name}\") AS non_null_count
        FROM \"{table_name}\";
        """
        stats_df = conn.execute(stats_query).fetchdf()
        distinct_expression_normalized = distinct_expression_raw # Use raw name for sampling

    # Calculate additional statistics
    total_rows = stats_df['total_rows'].iloc[0]
    unique_count = stats_df['unique_values'].iloc[0]
    non_null_count = stats_df['non_null_count'].iloc[0]
    null_count = total_rows - non_null_count
    
    stats = {
        "Data Type": column_type,
        "Total Rows in Table": total_rows,
        "Unique Values (Robust Count)": unique_count, # Renamed metric for clarity
        "Non-NULL Values": non_null_count,
        "NULL Values": null_count,
        "Uniqueness Ratio": f"{unique_count / total_rows * 100:.2f}%" if total_rows > 0 else "N/A"
    }

    # Return formatted stats, raw stats_df, and the distinct expression for sampling outside
    return stats, stats_df, distinct_expression_normalized 

# --- Streamlit App Layout (Rest of the code remains the same) ---

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

    # --- Sample Data View ---
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

        # Get stats, raw df, and the normalized expression
        stats, stats_df, distinct_expression_normalized = get_column_stats(conn, selected_table, selected_column)
        
        # Display DEBUGGING output for raw stats
        st.subheader("🔍 DEBUG: Raw Statistics DataFrame (`stats_df`)")
        st.markdown("This shows the base count result (Total Rows, Non-Null Count) and the Robust Unique Count.")
        st.code(stats_df.to_string(), language='text')

        # --- NEW DEBUGGING STEP: Display Sample of Distinct Values ---
        if 'VARCHAR' in stats['Data Type']:
            st.subheader("🔍 DEBUG: Sample of Normalized Distinct Values (First 5)")
            st.markdown("This shows what the `GROUP BY` query actually sees as unique values.")
            
            distinct_sample_df = get_distinct_sample(conn, selected_table, selected_column, distinct_expression_normalized)
            
            if distinct_sample_df.empty:
                st.warning("Could not retrieve distinct value sample.")
            else:
                st.code(distinct_sample_df.to_string(), language='text')
            
        # Display final formatted stats
        st.subheader("Formatted Column Statistics")
        stats_df_display = pd.DataFrame(list(stats.items()), columns=['Metric', 'Value'])
        
        st.table(stats_df_display)