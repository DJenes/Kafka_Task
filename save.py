import json
from time import sleep

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'parsed_movies'
    movies_data = []
    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        record = json.loads(msg.value)
        movies_data.append(record)
    with open("movies.json", "w") as f:
        json.dump(movies_data, f, indent=4)

    if consumer is not None:
        consumer.close()