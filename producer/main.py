import time

from extract import connect_to_api, extract_json
from producer_setup import init_producer, topic

def main():
    response = connect_to_api()
    
    data = extract_json(response)
    
    producer = init_producer()    # Initialize the Kafka producer using the init_producer function from the producer_setup module
    
    for stock in data:
        result = {
            'date': stock['datetime'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'high': stock['high'],
            'low': stock['low'],
            'close': stock['close']
        }

        producer.send(topic, result)     # Send the extracted stock data to the specified Kafka topic using the producer's send method, which takes the topic name and the data to be sent as arguments
        print(f'Data sent to {topic} topic')     # Print a message to the console indicating that the data has been sent to the specified Kafka topic, along with the topic name for confirmation
        
        time.sleep(2)  # Sleep for 2 seconds to avoid overwhelming the Kafka broker with too many messages in a short period of time
    
    producer.flush()  # Flush the producer to ensure that all messages are sent to the Kafka broker before the program exits
    producer.close()  # Close the producer to release any resources it may be using and to cleanly shut down the connection to the Kafka broker
        
    return None


if __name__ == "__main__":
    main()