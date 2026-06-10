from kafka import KafkaProducer
import json



topic = "stock_analysis"  # Define the Kafka topic to which the data will be sent


def init_producer():
    producer = KafkaProducer(
        bootstrap_servers = "localhost:9094",  # Specify the Kafka broker address
        value_serializer = lambda v: json.dumps(v).encode('utf-8')  # Define a serializer to convert the data to JSON format before sending it to Kafka
    )
    
    return producer