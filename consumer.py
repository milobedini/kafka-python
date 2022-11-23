from kafka import KafkaConsumer
import kafka
import json
import time
import logging

# TOPIC_NAME = 'items'

# consumer = KafkaConsumer(TOPIC_NAME)
# for message in consumer:
#     print(message)

TOPIC_NAME = 'Users'
KAFKA_SERVER = 'localhost:9092'

consumer = KafkaConsumer(TOPIC_NAME, group_id="test",

                         #  bootstrap_servers=[KAFKA_SERVER],
                         fetch_max_wait_ms=5000, security_protocol="PLAINTEXT",
                         enable_auto_commit=False,
                         value_deserializer=lambda v: json.loads(
                             v.decode('ascii')),
                         auto_offset_reset='earliest')


for message in consumer:
    print(message)

# def read_objects():
#     print("read objects")
#     for data in consumer:
#         print(
#             f"Read {len(data.value)} bytes from {TOPIC_NAME} with value {data.message}")
#         message = json.loads(data.value)

#         # Yield suspends the execution and returns the value to the caller.
#         # Return stops the function execution entirely.
#         # Yield saves the state of the function, and picks it back up.
#         yield message


# consumer.subscribe(topics=TOPIC_NAME)

# for msg in consumer:
#     print(msg.partition)


# def run():
#     while True:


# try:
#     run()
# except:
#     print("Could not consume")
