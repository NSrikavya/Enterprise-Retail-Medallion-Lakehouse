import json
from kafka import KafkaConsumer

TOPIC_NAME = 'retail-transactions'

# Initialize the Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic if no offset is stored
    enable_auto_commit=True,
    group_id='retail-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

print(
    f'👂 Listening for live retail transactions on Kafka topic:'
    f' "{TOPIC_NAME}"...'
)

try:
  for message in consumer:
    data = message.value
    print(
        f"📥 Consumed: [Txn: {data['transaction_id']}] Store:"
        f" {data['store_id']} | Product: {data['item_name']} | Qty:"
        f" {data['quantity']} | Total: ${data['total_amount']}"
    )
except KeyboardInterrupt:
  print('\n🛑 Consumer stopped safely.')
finally:
  consumer.close()