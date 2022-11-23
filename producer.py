from kafka import KafkaProducer
import json
import time


TOPIC_NAME = 'Users'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                         security_protocol="PLAINTEXT",
                         )


topic = TOPIC_NAME


def write_object(obj: dict = None):
    value = json.dumps(obj).encode("utf-8")
    send(topic, value)


def send(topic, value, key=None):
    print(f"Sending {len(value)} bytes with value {value} to {topic}")
    producer.send(topic, value=value, key=key)


def run(user="Milo"):
    print(f"Start generator {KAFKA_SERVER}/{TOPIC_NAME}")
    obj_id = 0
    while True:
        write_object({
            "id": obj_id,
            "user": user
        })
        obj_id += 1
        time.sleep(10)


try:
    run()
except:
    print("Error connecting")
