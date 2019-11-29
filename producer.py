from __future__ import print_function
import json
from kafka import KafkaProducer, KafkaClient
import tweepy
from datetime import datetime
from elasticsearch import Elasticsearch
# global i
# Twitter Credentials Obtained from http://dev.twitter.com
consumer_key = 'ZLq595SorU3DFUgH0vQySKKJo'
consumer_secret = 'Uzrm0TCsQJOQnqm1RqtCbjlHLpP4dBttdChCAFflcmLvuKrXHV'
access_key = '1484190548-C6R6UnNe7Z1Nx5pgMn3ATS8MNeJ6LEYvJekv2wi'
access_secret = 'vjEQkrcMuRAaB4DKaaX5Hoi94clSaKOReSZyRyrTUvfj1'
i = 300
# Words to track
WORDS = ['#NCP', '#AjitPawar', '#BJP']

class StreamListener(tweepy.StreamListener):
    # i =1
    # This is a class provided by tweepy to access the Twitter Streaming API.
    # status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print("Error received in kafka producer " + repr(status_code))
        return True # Don't kill the stream

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        # global i
        # data_dct =[]
        try:
            producer.send('twitter_stream', data.encode('utf-8'))
            print(1)
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

# Kafka Configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Create Auth object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=60, retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
stream = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
stream.filter(track=WORDS, languages = ['en'])
