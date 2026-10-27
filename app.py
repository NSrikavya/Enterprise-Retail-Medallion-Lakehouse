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


# Load Gold Layer data with cloud fallback support
@st.cache_data
def load_gold_data():
  if os.path.exists(STORE_PATH) and os.path.exists(PRODUCT_PATH):
    # Load actual local data from the Gold Layer
    store_df = pd.read_parquet(STORE_PATH)
    product_df = pd.read_parquet(PRODUCT_PATH)
  else:
    # Fallback mock data for cloud deployment (Streamlit Cloud)
    store_df = pd.DataFrame({
        'store_id': ['STORE_NY_01', 'STORE_LA_02', 'STORE_CHI_03', 'STORE_TX_04'],
        'total_revenue': [45200.50, 38100.20, 29400.00, 51200.75],
        'total_transactions': [1200, 980, 750, 1400],
        'avg_basket_size': [37.67, 38.88, 39.20, 36.57],
    })

    product_df = pd.DataFrame({
        'item_id': ['P1002', 'P1003', 'P1001', 'P1005', 'P1004'],
        'item_name': [
            'Mechanical Keyboard',
            'HD Monitor',
            'Wireless Mouse',
            'Ergonomic Chair',
            'USB-C Hub',
        ],
        'total_quantity_sold': [450, 320, 610, 180, 530],
        'total_product_revenue': [35995.50, 60640.00, 15853.90, 44820.00, 7950.00],
    })

  return store_df, product_df


sales_by_store, top_products = load_gold_data()

# Top-level KPI Summary Metrics
total_revenue = sales_by_store['total_revenue'].sum()
total_txns = sales_by_store['total_transactions'].sum()

col1, col2 = st.columns(2)
col1.metric(label='💰 Total Enterprise Revenue', value=f'${total_revenue:,.2f}')
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