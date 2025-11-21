import streamlit as st
import duckdb
import pandas as pd
import altair as alt
import time
from functools import wraps

# --- Configuration ---
st.set_page_config(layout="wide", page_title="DuckDB Remote Analytics")

DATABASE_URL = "https://cs.wellesley.edu/~eni/duckdb/2023_wiki_views.duckdb"
#"https://cs.wellesley.edu/~eni/duckdb/2024_wiki_views.duckdb"

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


def page_article_analysis():
    st.markdown("## 📰 Top 10 Article Performance")
    st.markdown("Select a month to identify the top 10 articles by total pageviews and view their daily trends.")

    # --- 1. Get List of Available Months (Cached) ---
    @st.cache_data(ttl=3600)
    @retry_query()
    def get_available_months():
        """Fetches a sorted list of unique year-month strings (YYYY-MM)."""
        try:
            # Extract distinct year and month, format as YYYY-MM
            df_months = run_duckdb_query("""
                SELECT DISTINCT
                    STRFTIME(DATE_TRUNC('month', date), '%Y-%m') AS month_key
                FROM data_table
                ORDER BY month_key DESC;
            """)
            return df_months['month_key'].tolist()
        except Exception as e:
            st.error(f"Failed to fetch available months: {e}")
            return []

    available_months = get_available_months()

    if not available_months:
        st.warning("Could not load available months from the database. Please check the connection and table name.")
        return

    # 2. Interactive Selector
    selected_month = st.selectbox(
        "Select a Month (YYYY-MM):",
        options=available_months,
        index=0,
        key='month_selector'
    )

    # 3. Data Fetching and Top 10 Calculation
    if selected_month:
        # Start date as a string literal (e.g., '2024-01-01')
        start_date = f"'{selected_month}-01'"
        
        # FIX: Calculate the end date by casting the start date string to DATE (using ::DATE)
        # and adding 1 MONTH. This avoids the ambiguous DATE_TRUNC call.
        # This will result in the first day of the following month (e.g., '2024-02-01').
        end_date = f"{start_date}::DATE + INTERVAL 1 MONTH" 

        with st.spinner(f"Analyzing articles for {selected_month}..."):
            try:
                # 3a. Query to find the Top 10 articles by total views in the selected month
                top_articles_query = f"""
                WITH MonthlyTotals AS (
                    SELECT
                        article,
                        SUM(pageviews) AS total_monthly_pageviews
                    FROM data_table
                    -- FIX: Explicitly cast start_date string to DATE for comparison
                    WHERE date >= {start_date}::DATE AND date < {end_date}
                    GROUP BY 1
                )
                SELECT
                    article,
                    total_monthly_pageviews
                FROM MonthlyTotals
                ORDER BY total_monthly_pageviews DESC
                LIMIT 10;
                """
                df_top_articles = run_duckdb_query(top_articles_query)

                if df_top_articles.empty:
                    st.info(f"No article data found for {selected_month}.")
                    return

                # 4. Display Top 10 Table
                st.subheader(f"Top 10 Articles in {selected_month}")
                # Rename columns for display
                display_df = df_top_articles.rename(
                    columns={
                        'article': 'Article Title',
                        'total_monthly_pageviews': 'Total Pageviews'
                    }
                )
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                # 5. Get Daily Pageviews for the Top 10 Articles
                # Extract the names of the top 10 articles to use in the IN clause
                top_article_names = [name.replace("'", "''") for name in df_top_articles['article'].tolist()]
                
                # Check if we have articles to query for daily views
                if not top_article_names:
                    return

                articles_list_sql = ", ".join(f"'{name}'" for name in top_article_names)

                daily_views_query = f"""
                SELECT
                    date,
                    article,
                    SUM(pageviews) AS daily_pageviews
                FROM data_table
                WHERE article IN ({articles_list_sql})
                  -- FIX: Explicitly cast start_date string to DATE for comparison
                  AND date >= {start_date}::DATE AND date < {end_date}
                GROUP BY 1, 2
                ORDER BY date;
                """
                df_daily_views = run_duckdb_query(daily_views_query)
                
                if df_daily_views.empty:
                    st.warning(f"Could not retrieve daily view data for the top articles in {selected_month}.")
                    return

                df_daily_views['date'] = pd.to_datetime(df_daily_views['date'])

                # 6. Visualization using Altair
                st.subheader(f"Daily Pageview Trend for Top 10 Articles")

                chart = alt.Chart(df_daily_views).mark_line(point=True).encode(
                    x=alt.X('date', title='Day of the Month'),
                    y=alt.Y('daily_pageviews', title='Daily Pageviews'),
                    color=alt.Color('article', title='Article'),
                    tooltip=['date', 'article', 'daily_pageviews']
                ).properties(
                    height=500
                ).interactive()

                st.altair_chart(chart, use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred during Article Analysis: {e}")


# --- Main App Logic (Sidebar Navigation) ---

# Initialize session state for navigation
if 'page' not in st.session_state:
    st.session_state.page = 'timeseries'

st.sidebar.title("App Navigation")
selection = st.sidebar.radio(
    "Go to",
    options=['Pageview Time Series', 'Top Article Analysis'], # Updated option
    index=0
)

# Use the selection to route to the correct page function
if selection == 'Pageview Time Series':
    page_timeseries_analysis()
elif selection == 'Top Article Analysis': # Updated route
    page_article_analysis()

st.sidebar.markdown("---")