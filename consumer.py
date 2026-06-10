from kafka import KafkaConsumer
import json
import time


# --- Configuration matching the Producer ---

consumer = KafkaConsumer(
    'stock_analysis',  # Specify the Kafka topic to subscribe to
    bootstrap_servers = "localhost:9094",  # Specify the Kafka broker address
    auto_offset_reset = 'earliest',  # Start consuming from the earliest available message if no offset is present
    enable_auto_commit = True,  # Enable automatic offset committing to keep track of consumed messages
    group_id = 'my-consumer-group',  # Specify the consumer group ID for managing offsets
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))  # Define a deserializer to convert the JSON messages back into Python dictionaries
)

print("starting kafka consumer. Waiting for messages on topic 'customer_info'...")

for message in consumer:
    
    data = message.value
    
    # Print the received data to the console for verification
    
    print(f" Value (Deserialized): {data}")    # Print the deserialized value of the received message to the console for verification, allowing you to see the actual data being consumed from the Kafka topic
consumer.close()  # Close the consumer to release any resources it may be using and to cleanly shut down the connection to the Kafka broker
print("Kafka consumer closed.")  # Print a message to the console indicating that the Kafka consumer has been closed successfully