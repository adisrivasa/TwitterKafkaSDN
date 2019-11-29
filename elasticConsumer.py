from kafka import KafkaConsumer
from json import loads
import pandas as pd
from elasticsearch import Elasticsearch

i=2200
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
consumer = KafkaConsumer(
    'twitter_stream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=False,
     group_id='ElasticConsumerE',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message=message.value
    # message=json.dumps()
    # print(message['user']['screen_name'])
    es.create(index='idx_twp_con', doc_type='twitter_twp_con', id=i, body=message)
    i = i+1
    print('{} indexed.'.format(i))
