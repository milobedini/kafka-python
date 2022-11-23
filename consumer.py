from kafka import KafkaConsumer
import json

TOPIC_NAME = 'Users'
KAFKA_SERVER = 'localhost:9092'

consumer = KafkaConsumer(TOPIC_NAME, group_id="test",
                         bootstrap_servers=KAFKA_SERVER,
                         fetch_max_wait_ms=5000,
                         security_protocol="PLAINTEXT",
                         #  enable_auto_commit=False,
                         #  auto_offset_reset='earliest')
                         )


def read_objects(msg):

    print(
        f"Read position {msg.offset} with {len(msg.value)} bytes from {TOPIC_NAME} with value {msg.value}")

    print(json.loads(msg.value))

    # yield json.loads(msg.value)


def run():
    print("Consumer starting")
    while True:
        for msg in consumer:
            read_objects(msg)


try:
    run()
except:
    print("Could not consume")
