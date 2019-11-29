from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads
import pandas as pd
import json

consumer = KafkaConsumer(
    'twitter_stream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='ElasticConsumerL',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    df=pd.DataFrame({'id':[message['id']],'user':[message['user']['screen_name']],'text':[message['user']['text']],'client':[message['source']],'lang':[message['lang']]})
    with open('twitterrdata.csv', 'a') as f:
        df.to_csv("twitterlocal.csv", header=True,index=False)
    print('{} added.'.format(message))
