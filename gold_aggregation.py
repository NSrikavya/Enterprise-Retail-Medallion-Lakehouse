from glob import glob
import os
import pandas as pd

SILVER_DIR = 'silver_layer/retail_transactions'
GOLD_DIR = 'gold_layer'

# Create local Gold directories for our business metrics
os.makedirs(os.path.join(GOLD_DIR, 'sales_by_store'), exist_ok=True)
os.makedirs(os.path.join(GOLD_DIR, 'top_products'), exist_ok=True)

print('🏆 Starting Gold Layer Aggregation Pipeline...')

# Read all cleaned silver parquet files
silver_files = glob(os.path.join(SILVER_DIR, '*.parquet'))

if not silver_files:
  print('⚠️ No Silver Parquet files found to aggregate yet.')
else:
  # Combine all silver files into a single master pandas DataFrame
  df_list = [pd.read_parquet(f) for f in silver_files]
  df = pd.concat(df_list, ignore_index=True)

  # --- GOLD LAYER BUSINESS METRICS ---

  # 1. Metric 1: Total Sales and Transaction Count per Store
  sales_by_store = (
      df.groupby('store_id')
      .agg(
          total_revenue=('total_amount', 'sum'),
          total_transactions=('transaction_id', 'count'),
          avg_basket_size=('total_amount', 'mean'),
      )
      .reset_index()
  )

  # 2. Metric 2: Top Selling Products by Quantity Sold
  top_products = (
      df.groupby(['item_id', 'item_name'])
      .agg(
          total_quantity_sold=('quantity', 'sum'),
          total_product_revenue=('total_amount', 'sum'),
      )
      .reset_index()
      .sort_values(by='total_quantity_sold', ascending=False)
  )

  # Save Gold tables as Parquet files (ready for Power BI or Superset)
  store_path = os.path.join(GOLD_DIR, 'sales_by_store', 'metrics_by_store.parquet')
  product_path = os.path.join(
      GOLD_DIR, 'top_products', 'metrics_top_products.parquet'
  )

  sales_by_store.to_parquet(store_path, index=False)
  top_products.to_parquet(product_path, index=False)

  print('✅ [GOLD LAYER SAVED] Store metrics generated successfully!')
  print(sales_by_store)
  print('\n✅ [GOLD LAYER SAVED] Product rankings generated successfully!')
  print(top_products)