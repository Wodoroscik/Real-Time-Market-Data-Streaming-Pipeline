import streamlit as st
import sqlite3
import pandas as pd
import time

st.set_page_config(page_title="Market Stream", layout="wide")
st.title("Price Anomaly Detection (Kafka + Spark)")

placeholder = st.empty()

while True:
    try:
        with sqlite3.connect('market_data.db') as conn:
            df = pd.read_sql("SELECT * FROM market_aggs", conn)
    except sqlite3.OperationalError:
        df = pd.DataFrame()

    with placeholder.container():
        if not df.empty:
            # Deduplicate tumbling windows (upsert logic simulation)
            df = df.drop_duplicates(subset=['window_end', 'ticker'], keep='last')
            
            df['end_time'] = pd.to_datetime(df['window_end'])
            df = df.sort_values(by='end_time')
            
            tickers = df['ticker'].unique()
            cols = st.columns(len(tickers))
            
            for i, ticker in enumerate(tickers):
                with cols[i]:
                    st.subheader(ticker)
                    ticker_df = df[df['ticker'] == ticker]
                    
                    chart_data = ticker_df.set_index('end_time')['avg_price']
                    st.line_chart(chart_data)
            
            st.subheader("Latest unique tumbling windows")
            st.dataframe(df.tail(15))
        else:
            st.write("Waiting for initial data batches from Spark...")

    time.sleep(2)
    st.rerun()