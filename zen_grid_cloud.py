import streamlit as st
import snowflake.connector
import pandas as pd

def create_connection():
    """Create a fresh Snowflake connection using Streamlit secrets"""
    try:
        conn = snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

def main():
    st.title("🔍 Zen Grid Diagnostic")
    st.write("Checking available tables and permissions...")
    
    conn = create_connection()
    if not conn:
        st.error("Cannot connect to Snowflake")
        return
    
    try:
        # Show current context
        st.subheader("Current Context")
        context_query = "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()"
        context_df = pd.read_sql(context_query, conn)
        st.dataframe(context_df)
        
        # Show available tables
        st.subheader("Available Tables")
        tables_query = "SHOW TABLES"
        tables_df = pd.read_sql(tables_query, conn)
        st.write(f"Found {len(tables_df)} tables:")
        st.dataframe(tables_df)
        
        # Look for market/forecast related tables
        st.subheader("Market/Forecast Tables")
        if len(tables_df) > 0:
            market_tables = tables_df[
                tables_df['name'].str.contains('FORECAST|MARKET|POST', case=False, na=False)
            ]
            if len(market_tables) > 0:
                st.write("Tables containing 'FORECAST', 'MARKET', or 'POST':")
                st.dataframe(market_tables[['name', 'database_name', 'schema_name']])
            else:
                st.write("No obvious market/forecast tables found")
        
        # Test specific table access
        st.subheader("Table Access Test")
        test_tables = [
            'FORECAST_POSTMORTEM',
            'DAILY_MARKET_DATA', 
            'FORECAST_SUMMARY',
            'FORECAST_DAILY_V2'
        ]
        
        for table in test_tables:
            try:
                test_query = f"SELECT COUNT(*) as count FROM {table}"
                result = pd.read_sql(test_query, conn)
                st.success(f"✅ {table}: {result.iloc[0]['COUNT']} rows")
            except Exception as e:
                st.error(f"❌ {table}: {str(e)}")
        
    except Exception as e:
        st.error(f"Diagnostic failed: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
