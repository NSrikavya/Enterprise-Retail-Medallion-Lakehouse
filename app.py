import os
import pandas as pd
import streamlit as st

# Configure page layout
st.set_page_config(
    page_title='Retail Lakehouse Analytics', page_icon='🛒', layout='wide'
)

st.title('🛒 Enterprise Retail Lakehouse Analytics Dashboard')
st.markdown(
    'Real-time retail analytics dashboard powered by Kafka, Python, Parquet, and'
    ' Streamlit.'
)

# File paths for Gold Layer metrics
STORE_PATH = 'gold_layer/sales_by_store/metrics_by_store.parquet'
PRODUCT_PATH = 'gold_layer/top_products/metrics_top_products.parquet'


# Load Gold Layer data safely
@st.cache_data
def load_gold_data():
  store_df = (
      pd.read_parquet(STORE_PATH) if os.path.exists(STORE_PATH) else pd.DataFrame()
  )
  product_df = (
      pd.read_parquet(PRODUCT_PATH)
      if os.path.exists(PRODUCT_PATH)
      else pd.DataFrame()
  )
  return store_df, product_df


sales_by_store, top_products = load_gold_data()

# Check if data exists
if sales_by_store.empty or top_products.empty:
  st.warning(
      '⚠️ Gold layer data not found! Please run your `python'
      ' gold_aggregation.py` script first to generate the metrics.'
  )
else:
  # Top-level KPI Summary Metrics
  total_revenue = sales_by_store['total_revenue'].sum()
  total_txns = sales_by_store['total_transactions'].sum()

  col1, col2 = st.columns(2)
  col1.metric(
      label='💰 Total Enterprise Revenue', value=f'${total_revenue:,.2f}'
  )
  col2.metric(label='📦 Total Transactions Processed', value=f'{total_txns:,}')

  st.markdown('---')

  # Visualizations (Side-by-Side Charts)
  col_a, col_b = st.columns(2)

  with col_a:
    st.subheader('📊 Total Revenue by Store')
    st.bar_chart(sales_by_store.set_index('store_id')['total_revenue'])

  with col_b:
    st.subheader('🏆 Top Selling Products (Units Sold)')
    st.bar_chart(top_products.set_index('item_name')['total_quantity_sold'])

  st.markdown('---')

  # Data Tables Section
  st.subheader('📋 Detailed Store Performance Metrics')
  st.dataframe(sales_by_store, use_container_width=True)

  st.subheader('📋 Product Sales Breakdown')
  st.dataframe(top_products, use_container_width=True)