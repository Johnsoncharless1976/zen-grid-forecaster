import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

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

def load_data():
    """Load all data in one connection with proper context setting"""
    conn = create_connection()
    if not conn:
        return None, None, None
    
    try:
        cursor = conn.cursor()
        
        # Explicitly set the context
        cursor.execute("USE DATABASE ZEN_MARKET")
        cursor.execute("USE SCHEMA FORECASTING")
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        
        # Load forecast postmortem data with full qualification
        forecast_query = """
        SELECT 
            DATE,
            INDEX as SYMBOL,
            FORECAST_BIAS,
            ACTUAL_CLOSE,
            HIT,
            LOAD_TS
        FROM ZEN_MARKET.FORECASTING.FORECAST_POSTMORTEM 
        ORDER BY DATE DESC
        LIMIT 100
        """
        forecast_df = pd.read_sql(forecast_query, conn)
        
        # Load market data
        market_query = """
        SELECT 
            DATE,
            SPY_CLOSE,
            ES_CLOSE,
            VIX_CLOSE,
            VVIX_CLOSE
        FROM ZEN_MARKET.FORECASTING.DAILY_MARKET_DATA 
        ORDER BY DATE DESC
        LIMIT 100
        """
        market_df = pd.read_sql(market_query, conn)
        
        # Load forecast summary
        summary_query = """
        SELECT 
            DATE,
            INDEX as SYMBOL,
            FORECAST_BIAS,
            SUPPORTS,
            RESISTANCES,
            ATM_STRADDLE,
            NOTES
        FROM ZEN_MARKET.FORECASTING.FORECAST_SUMMARY 
        ORDER BY DATE DESC
        LIMIT 50
        """
        summary_df = pd.read_sql(summary_query, conn)
        
        cursor.close()
        return forecast_df, market_df, summary_df
        
    except Exception as e:
        st.error(f"Data loading failed: {str(e)}")
        # Try with a more specific error message
        if "not authorized" in str(e).lower():
            st.warning("⚠️ **Permission Issue Detected**")
            st.info("The Streamlit Cloud user may need additional permissions. Check your Snowflake user grants for ZEN_MARKET.FORECASTING schema access.")
        return None, None, None
    finally:
        if conn:
            conn.close()

def main():
    st.set_page_config(
        page_title="Zen Grid Market Forecaster",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Zen Grid Market Forecaster Dashboard")
    st.markdown("**Real-time analysis of your 88% accuracy forecasting system**")
    st.markdown("*Deployed on Streamlit Cloud - Access from anywhere!*")
    
    # Connection status check
    with st.sidebar:
        st.subheader("🔗 Connection Status")
        conn = create_connection()
        if conn:
            st.success("✅ Snowflake Connected")
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                context = cursor.fetchone()
                st.write(f"**User:** {context[0]}")
                st.write(f"**Role:** {context[1]}")
                st.write(f"**Database:** {context[2]}")
                st.write(f"**Schema:** {context[3]}")
                cursor.close()
            except:
                pass
            conn.close()
        else:
            st.error("❌ Connection Failed")
    
    # Add refresh button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔄 Refresh Data"):
            st.rerun()
    with col2:
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load all data
    with st.spinner("Loading data from Snowflake..."):
        forecast_df, market_df, summary_df = load_data()
    
    if forecast_df is None:
        st.error("Failed to load data. Please check permissions.")
        st.info("""
        **Troubleshooting Steps:**
        1. Verify your Snowflake user has access to ZEN_MARKET database
        2. Check that your user has SELECT privileges on FORECASTING schema
        3. Ensure the COMPUTE_WH warehouse is accessible
        """)
        return
    
    if len(forecast_df) == 0:
        st.warning("No forecast data found in FORECAST_POSTMORTEM table")
        return
    
    # Calculate metrics
    total_forecasts = len(forecast_df)
    hits = forecast_df['HIT'].sum()
    accuracy = (hits / total_forecasts) * 100 if total_forecasts > 0 else 0
    misses = total_forecasts - hits
    
    # Display metrics with better formatting
    st.markdown("### 📊 Performance Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Overall Accuracy", 
            f"{accuracy:.1f}%",
            delta=f"Target: 88%" if accuracy < 88 else "🎯 Above target!"
        )
    with col2:
        st.metric("Total Forecasts", total_forecasts)
    with col3:
        st.metric("Hits", hits, delta=f"+{hits}")
    with col4:
        st.metric("Misses", misses, delta=f"-{misses}")
    
    # Main content layout
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("### 📈 Forecast Performance Over Time")
        
        if len(forecast_df) > 0:
            # Performance chart
            fig = go.Figure()
            
            hits_data = forecast_df[forecast_df['HIT'] == True]
            misses_data = forecast_df[forecast_df['HIT'] == False]
            
            if len(hits_data) > 0:
                fig.add_trace(go.Scatter(
                    x=hits_data['DATE'],
                    y=hits_data['ACTUAL_CLOSE'],
                    mode='markers',
                    marker=dict(color='green', size=12, symbol='circle'),
                    name=f'Hits ({len(hits_data)})',
                    text=hits_data['FORECAST_BIAS'],
                    hovertemplate='<b>%{text}</b><br>Date: %{x}<br>Price: $%{y:,.2f}<br>Result: HIT ✅<extra></extra>'
                ))
            
            if len(misses_data) > 0:
                fig.add_trace(go.Scatter(
                    x=misses_data['DATE'],
                    y=misses_data['ACTUAL_CLOSE'],
                    mode='markers',
                    marker=dict(color='red', size=12, symbol='x'),
                    name=f'Misses ({len(misses_data)})',
                    text=misses_data['FORECAST_BIAS'],
                    hovertemplate='<b>%{text}</b><br>Date: %{x}<br>Price: $%{y:,.2f}<br>Result: MISS ❌<extra></extra>'
                ))
            
            fig.update_layout(
                title="Forecast Results Over Time",
                xaxis_title="Date",
                yaxis_title="Actual Close Price ($)",
                hovermode='closest',
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Forecast Bias Analysis")
        
        # Bias breakdown
        if len(forecast_df) > 0:
            bias_stats = forecast_df.groupby('FORECAST_BIAS').agg({
                'HIT': ['count', 'sum']
            })
            bias_stats.columns = ['Total', 'Hits']
            bias_stats['Accuracy %'] = (bias_stats['Hits'] / bias_stats['Total'] * 100).round(1)
            
            st.dataframe(
                bias_stats[['Total', 'Hits', 'Accuracy %']], 
                use_container_width=True
            )
            
            # Bias pie chart
            if len(bias_stats) > 0:
                fig_pie = px.pie(
                    bias_stats.reset_index(),
                    values='Total',
                    names='FORECAST_BIAS',
                    title='Forecast Distribution by Bias'
                )
                st.plotly_chart(fig_pie, use_container_width=True)
    
    # Recent forecasts table
    st.markdown("### 📋 Recent Forecast Results")
    
    if len(forecast_df) > 0:
        display_df = forecast_df[['DATE', 'SYMBOL', 'FORECAST_BIAS', 'ACTUAL_CLOSE', 'HIT']].head(15)
        
        # Format the dataframe
        display_df = display_df.copy()
        display_df['ACTUAL_CLOSE'] = display_df['ACTUAL_CLOSE'].apply(lambda x: f"${x:,.2f}")
        display_df['HIT'] = display_df['HIT'].apply(lambda x: "✅ HIT" if x else "❌ MISS")
        
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "DATE": "Date",
                "SYMBOL": "Symbol", 
                "FORECAST_BIAS": "Bias",
                "ACTUAL_CLOSE": "Actual Price",
                "HIT": "Result"
            }
        )
    
    # Data summary in expander
    with st.expander("📊 Data Summary & Raw Tables"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**📈 Forecast Data**")
            if forecast_df is not None and len(forecast_df) > 0:
                st.write(f"**Records:** {len(forecast_df)}")
                st.write(f"**Date Range:** {forecast_df['DATE'].min()} to {forecast_df['DATE'].max()}")
                st.write(f"**Symbols:** {', '.join(forecast_df['SYMBOL'].unique())}")
        
        with col2:
            st.markdown("**💹 Market Data**")
            if market_df is not None and len(market_df) > 0:
                st.write(f"**Records:** {len(market_df)}")
                st.write(f"**Date Range:** {market_df['DATE'].min()} to {market_df['DATE'].max()}")
                non_null_cols = [col for col in ['SPY_CLOSE', 'ES_CLOSE', 'VIX_CLOSE', 'VVIX_CLOSE'] 
                               if market_df[col].notna().any()]
                st.write(f"**Available:** {', '.join(non_null_cols)}")
        
        with col3:
            st.markdown("**📝 Summary Data**")
            if summary_df is not None and len(summary_df) > 0:
                st.write(f"**Records:** {len(summary_df)}")
                st.write(f"**Date Range:** {summary_df['DATE'].min()} to {summary_df['DATE'].max()}")
        
        # Raw data tabs
        tab1, tab2, tab3 = st.tabs(["Forecast Performance", "Market Data", "Forecast Summaries"])
        
        with tab1:
            if forecast_df is not None:
                st.dataframe(forecast_df, use_container_width=True)
        
        with tab2:
            if market_df is not None:
                st.dataframe(market_df, use_container_width=True)
        
        with tab3:
            if summary_df is not None:
                st.dataframe(summary_df, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("🎯 **Zen Council Market Forecaster** | Deployed on Streamlit Cloud")

if __name__ == "__main__":
    main()
