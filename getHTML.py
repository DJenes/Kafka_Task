from pickletools import uint1
import requests
from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
import sys
import time
import json



def fetch_raw_html(url):
    html = None
    print('Processing..{}'.format(url))
    try:
        r = requests.get(url)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html  

def movies_links():
    movies = []
    url = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    titles = soup.findAll("td","titleColumn")
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            titles = soup.findAll("td","titleColumn")
            for title in titles:
                name = title.find("a").get('href')
                link =f"https://www.imdb.com{name}"
                title_page = fetch_raw_html(link)
                movies.append(title_page)

    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return movies

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



if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    all_movies = movies_links()
    if len(all_movies) > 0:
        kafka_producer = connect_kafka_producer()
        for movie in all_movies:
            publish_message(kafka_producer, 'raw_movies2', 'raw', movie.strip())
        if kafka_producer is not None:
            kafka_producer.close()          