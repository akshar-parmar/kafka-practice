# filename: consume_specific_partitions.py
from datetime import datetime
import json
import os
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError

# --- Consumer Configuration ---
# A 'group.id' is still required for the client to identify itself to the broker.
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your broker address
    'group.id': 'my-manual-assignment-group',
    'auto.offset.reset': 'earliest'
}
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)
# Create a single consumer instance
consumer = Consumer(consumer_conf)

# --- Define the specific partitions to consume from ---
# This is the core of the solution.
main_topic = 'practice-topic'
dlq_topic = 'dead-letter-topic'
partitions_to_assign = [
    TopicPartition(main_topic, 0),
    TopicPartition(main_topic, 1)
]

# Manually assign the consumer to these partitions
consumer.assign(partitions_to_assign)
print(f"Consumer assigned to partitions: {partitions_to_assign}")

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0) # 1-second timeout

        if msg is None:
            # No message received within the timeout period
            continue
        if msg.error():
            # Handle potential errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event is not an error
                print(f"Reached end of partition for {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Error: {msg.error()}")
            continue

        # Successfully received a message
        print(f"Received message from partition {msg.partition()}: key='{msg.key().decode('utf-8') if msg.key() else None}' value='{msg.value().decode('utf-8')}'")
        
        try:
            # Decode the message value for inspection
            message_value = msg.value().decode('utf-8')
            print(f"Processing message from partition {msg.partition()}: value='{message_value}'")

            # SIMULATED FAILURE CONDITION: Check if the message contains "error"
            if 'error' in message_value.lower():
                # This is our "processing failure"
                print(f"--> Failure detected! Sending message to DLQ: '{dlq_topic}'")
                
                # Produce the original message to the dead-letter topic
                dlq_payload = {
                    'original_message': message_value,
                    'original_topic': msg.topic(),
                    'original_partition': msg.partition(),
                    'original_offset': msg.offset(),
                    'failure_reason': 'Business logic: Message contained "error"',
                    'failure_timestamp': datetime.now().isoformat() # Adds a UTC timestamp
                }

                # Convert the dictionary to a JSON string
                dlq_message = json.dumps(dlq_payload)

                producer.produce(
                    topic=dlq_topic,
                    key=msg.key(),
                    value=dlq_message.encode('utf-8') # Produce the new JSON payload
                )
                
                # Ensure the message is sent before we continue
                producer.flush()
            else:
                # This represents a successfully processed message
                print("--> Message processed successfully.")

        except Exception as e:
            print(f"--> UNEXPECTED ERROR during processing: {e}. Sending to DLQ.")
            
            ### <<< MODIFIED: Also enrich the message for unexpected errors
            dlq_payload = {
                'original_message': msg.value().decode('utf-8', 'ignore'), # Decode safely
                'original_topic': msg.topic(),
                'original_partition': msg.partition(),
                'original_offset': msg.offset(),
                'failure_reason': f'Unexpected exception: {str(e)}',
                'failure_timestamp': datetime.now().isoformat()
            }
            dlq_message = json.dumps(dlq_payload)
            producer.produce(topic=dlq_topic, key=msg.key(), value=dlq_message.encode('utf-8'))
            producer.flush()
            
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    # Cleanly close the consumer connection
    consumer.close()