import streamlit as st
import duckdb
import pandas as pd
import altair as alt
import time
from functools import wraps

# --- Configuration ---
st.set_page_config(layout="wide", page_title="DuckDB Remote Analytics")

DATABASE_URL = "https://cs.wellesley.edu/~eni/duckdb/2023_wiki_views.duckdb"
"https://cs.wellesley.edu/~eni/duckdb/2024_wiki_views.duckdb"

# --- Utility Functions ---

# Function to handle connection and caching
@st.cache_resource
def get_duckdb_connection():
    try:
        # 1. Connect to an in-memory database first
        conn = duckdb.connect(database=':memory:', read_only=False) 
        
        # 2. Install and Load the HTTPFS extension
        # DuckDB requires the extension to read remote files
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # 3. Use the URL in a READ_ONLY command
        # This tells DuckDB to treat the remote URL as the database
        conn.execute(f"ATTACH '{DATABASE_URL}' AS remote_db (READ_ONLY)")
        conn.execute("USE remote_db;") 
        return conn
    
    except Exception as e:
        st.error(f"Error connecting to DuckDB via HTTPFS: {e}")
        st.error("Make sure your database file is publicly accessible and the URL is correct.")
        st.stop()
        return None

# Retry decorator for temporary network issues
def retry_query(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        st.warning(f"Query failed (Attempt {attempt + 1}/{max_retries}). Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        raise e
            return None
        return wrapper
    return decorator

@retry_query()
def run_duckdb_query(query):
    """Executes a SQL query on the cached connection and returns a DataFrame."""
    con = get_duckdb_connection()
    if con:
        # Use an f-string to ensure the table name is correctly referenced
        return con.execute(query).fetchdf()
    return pd.DataFrame()

# --- Page Definitions ---

def page_timeseries_analysis():
    st.markdown("## 📈 Pageview Time Series Analysis")

    # 1. Date Aggregation Selector
    agg_period = st.radio(
        "Aggregate by:",
        ('Day', 'Week', 'Month'),
        index=0,
        horizontal=True,
        key='timeseries_agg_period'
    )

    # Map radio choice to DuckDB date truncation function
    date_format_map = {
        'Day': 'DATE_TRUNC(\'day\', date)',
        'Week': 'DATE_TRUNC(\'week\', date)',
        'Month': 'DATE_TRUNC(\'month\', date)'
    }
    date_trunc_sql = date_format_map[agg_period]

    # 2. Data Fetching
    with st.spinner(f"Fetching data aggregated by {agg_period}..."):
        query = f"""
        SELECT
            {date_trunc_sql} AS period,
            SUM(pageviews) AS total_pageviews
        FROM data_table
        GROUP BY 1
        ORDER BY 1;
        """
        try:
            df_timeseries = run_duckdb_query(query)

            if df_timeseries.empty:
                st.info("No data returned for the time series analysis.")
                return

            # Ensure the 'period' column is datetime type for Altair
            df_timeseries['period'] = pd.to_datetime(df_timeseries['period'])

            # 3. Visualization using Altair
            chart = alt.Chart(df_timeseries).mark_line(point=True).encode(
                x=alt.X('period', title=agg_period),
                y=alt.Y('total_pageviews', title='Total Pageviews'),
                tooltip=['period', 'total_pageviews']
            ).properties(
                title=f'Total Pageviews by {agg_period}'
            ).interactive() # Allows zooming and panning

            st.altair_chart(chart, use_container_width=True)

        except Exception as e:
            st.error(f"An error occurred during Timeseries Analysis: {e}")


def page_project_breakdown():
    st.markdown("## 🌐 Project Pageview Breakdown by Country")
    st.markdown("Select a country to see how pageviews are distributed across different projects (e.g., `en.wikipedia`).")

    # --- 1. Get List of Unique Country Codes for the Selector (Cached) ---
    @st.cache_data(ttl=3600)
    @retry_query()
    def get_country_codes():
        """Fetches a sorted list of unique country_codes."""
        try:
            # Query only the distinct country codes
            df_countries = run_duckdb_query("SELECT DISTINCT country_code FROM data_table WHERE country_code IS NOT NULL ORDER BY country_code;")
            return df_countries['country_code'].tolist()
        except Exception as e:
            st.error(f"Failed to fetch country codes: {e}")
            return []

    country_codes = get_country_codes()

    if not country_codes:
        st.warning("Could not load country codes from the database. Please check the connection and table name.")
        return

    # 2. Interactive Selector
    selected_country = st.selectbox(
        "Select a Country Code:",
        options=country_codes,
        index=country_codes.index('US') if 'US' in country_codes else 0,
        key='country_selector'
    )

    # 3. Data Fetching
    if selected_country:
        with st.spinner(f"Fetching project data for {selected_country}..."):
            # Sanitize input (Streamlit handles some, but good practice for SQL)
            safe_country = selected_country.replace("'", "''")

            query = f"""
            SELECT
                project,
                SUM(pageviews) AS total_pageviews
            FROM data_table
            WHERE country_code = '{safe_country}'
            GROUP BY 1
            ORDER BY 2 DESC;
            """
            try:
                df_projects = run_duckdb_query(query)

                if df_projects.empty:
                    st.info(f"No project data found for country code: {selected_country}")
                    return

                # 4. Visualization using Altair
                chart = alt.Chart(df_projects).mark_bar().encode(
                    x=alt.X('project', sort='-y', title='Project'),
                    y=alt.Y('total_pageviews', title='Total Pageviews'),
                    tooltip=['project', 'total_pageviews'],
                    color=alt.Color('project', legend=None)
                ).properties(
                    title=f'Total Pageviews by Project in {selected_country}'
                ).interactive()

                st.altair_chart(chart, use_container_width=True)
                st.markdown("---")
                st.caption("Raw Data Preview")
                st.dataframe(df_projects, use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred during Project Breakdown Analysis: {e}")

# --- Main App Logic (Sidebar Navigation) ---

# Initialize session state for navigation
if 'page' not in st.session_state:
    st.session_state.page = 'timeseries'

st.sidebar.title("App Navigation")
selection = st.sidebar.radio(
    "Go to",
    options=['Pageview Time Series', 'Project Breakdown'],
    index=0
)

# Use the selection to route to the correct page function
if selection == 'Pageview Time Series':
    page_timeseries_analysis()
elif selection == 'Project Breakdown':
    page_project_breakdown()

st.sidebar.markdown("---")
