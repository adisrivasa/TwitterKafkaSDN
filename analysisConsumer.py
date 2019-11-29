from kafka import KafkaConsumer
from flask import Flask, render_template, url_for
from json import loads
import re
import pandas as pd
import csv
import threading
import atexit
from textblob import TextBlob
from elasticsearch import Elasticsearch

POOL_TIME = 5 #Seconds

# variables that are accessible from anywhere
tweets = []
# lock to control access to variable
dataLock = threading.Lock()
# thread handler
yourThread = threading.Thread()


def create_app():
    app = Flask(__name__)

    def interrupt():
        global yourThread
        yourThread.cancel()

    def clean_tweets(text):
        #Cleaning tweets using Regex
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split())

    def get_sentiment(text):
        senti = TextBlob(clean_tweets(text))
        return (senti.sentiment.polarity)

    def analyse():
        global commonDataStruct
        global yourThread
        with dataLock:
            consumer = KafkaConsumer(
                'twitter_stream',
                 bootstrap_servers=['localhost:9092'],
                 auto_offset_reset='earliest',
                 enable_auto_commit=True,
                 group_id=None,
                 value_deserializer=lambda x: loads(x.decode('utf-8')),
                 consumer_timeout_ms = 10000)
            for message in consumer:
                message=message.value
                st = str(message['text'])
                sentiment = get_sentiment(st)
                tweets.append((message['id'], message['user']['screen_name'], message['text'], sentiment))
            consumer.close()
        yourThread = threading.Timer(POOL_TIME, analyse, ())
        yourThread.start()

    def analyseStart():
        global yourThread
        yourThread = threading.Timer(POOL_TIME, analyse, ())
        yourThread.start()

    analyse()
    atexit.register(interrupt)
    return app

app = create_app()

@app.route("/sdn")
def prog():
    global tweets
    return render_template('ee.html', list=tweets)

if __name__=='__main__':
    app.run( host = '0.0.0.0', port = 5000, debug = True )

    # print('{} added.'.format(message))
# for index, row in df.iterrows():
#     # tweet_sent.append(sentiment)
#     senti_df = pd.DataFrame()
