import json
from time import sleep

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def ParseMovieData (html):
    movies = []
    soup = BeautifulSoup(html, 'lxml')
    name = soup.find("title").text
    rating = soup.find(class_="sc-7ab21ed2-1 jGRxWM").text
    genre = soup.find(class_="ipc-inline-list__item ipc-chip__text").text
    description = soup.find(class_="sc-16ede01-0 fMPjMP").text
    movies.append({"name": name,
                           "rating": rating,
                           "genre": genre,
                           "description": description})
    return json.dumps(movies)    
    
if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'raw_movies2'
    parsed_topic_name = 'parsed_movies'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        html = msg.value
        result = ParseMovieData(html)
        parsed_records.append(result)
    consumer.close()
    sleep(5)   
    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec
                            ) 