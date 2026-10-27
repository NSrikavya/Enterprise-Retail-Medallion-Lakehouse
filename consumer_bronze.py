import json
import os
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd

TOPIC_NAME = 'retail-transactions'
BRONZE_DIR = 'bronze_layer/retail_transactions'

# Create the local directory structure for our Bronze Lake if it doesn't exist
os.makedirs(BRONZE_DIR, exist_ok=True)

# Initialize the Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='retail-bronze-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

print(f'🛢️ Listening to Kafka & writing to Local Bronze Lake (Parquet)...')

buffer = []
BATCH_SIZE = 10  # Write to a Parquet file every 10 transactions

try:
  for message in consumer:
    data = message.value
    buffer.append(data)
    print(
        f"📥 Buffered Transaction: {data['transaction_id']} | Store:"
        f" {data['store_id']}"
    )

    # When the buffer hits our batch size, flush it to a Parquet file
    if len(buffer) >= BATCH_SIZE:
      df = pd.DataFrame(buffer)

      # Generate a unique timestamp-based file name for this batch
      timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
      file_path = os.path.join(BRONZE_DIR, f'batch_{timestamp_str}.parquet')

      # Write DataFrame to Parquet format (Raw/Bronze Layer)
      df.to_parquet(file_path, index=False)
      print(
          f'✅ [BRONZE LAKE SAVED] Wrote {len(buffer)} records to ->'
          f' {file_path}'
      )

      # Clear buffer for the next batch
      buffer = []

except KeyboardInterrupt:
  print('\n🛑 Bronze consumer stopped safely.')
finally:
  consumer.close()