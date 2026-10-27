from datetime import datetime
from glob import glob
import os
import pandas as pd

BRONZE_DIR = 'bronze_layer/retail_transactions'
SILVER_DIR = 'silver_layer/retail_transactions'

# Create the local Silver directory if it doesn't exist
os.makedirs(SILVER_DIR, exist_ok=True)

print('🧹 Starting Silver Layer Transformation Pipeline...')

# Find all parquet files currently in the bronze layer
bronze_files = glob(os.path.join(BRONZE_DIR, '*.parquet'))

if not bronze_files:
  print('⚠️ No Bronze Parquet files found to process yet.')
else:
  for file_path in bronze_files:
    # Read the raw bronze data file
    df = pd.read_parquet(file_path)

    # --- SILVER LAYER CLEANING & REFINEMENT ---
    # 1. Remove duplicate transactions based on transaction_id
    df = df.drop_duplicates(subset=['transaction_id'])

    # 2. Type casting and sanitization
    df['quantity'] = df['quantity'].astype(int)
    df['unit_price'] = df['unit_price'].astype(float)
    df['total_amount'] = df['total_amount'].astype(float)

    # 3. Standardize text values (e.g., uppercase store names)
    df['store_id'] = df['store_id'].str.upper()

    # 4. Add an audit column tracking when the silver transformation happened
    df['silver_processed_at'] = datetime.utcnow().isoformat()

    # Define path for the cleaned silver file
    file_name = os.path.basename(file_path)
    silver_file_path = os.path.join(SILVER_DIR, file_name)

    # Write cleaned data to the Silver Layer
    df.to_parquet(silver_file_path, index=False)
    print(
        f'✨ [SILVER LAYER SAVED] Cleaned & transformed file ->'
        f' {silver_file_path}'
    )

  print('🎉 Silver batch processing complete!')