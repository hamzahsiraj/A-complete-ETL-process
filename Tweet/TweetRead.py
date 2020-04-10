import tweepy
from tweepy import OAuthHandler, Stream, API
import findspark
findspark.init()
from tweepy.streaming import StreamListener
import socket
import json
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext

consumer_key = 'consume_key from twitter'
consumer_secret = 'consumer_secret from twitter'
access_token = 'access_token from twitter'
access_secret = 'access_secret from twitter'

tweet_count = 0
num_of_tweets = 120


savefile = open('tweet.txt', 'w+')
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        # print(status.id_str)
        # if "retweeted_status" attribute exists, flag this tweet as a retweet.
        is_retweet = hasattr(status, "retweeted_status")
        global tweet_count,num_of_tweets
        # check if text has been truncated
        if hasattr(status,"extended_tweet"):
            text = status.extended_tweet["full_text"]
        else:
            text = status.text
        
        if tweet_count < num_of_tweets:
            print("The tweet number is ",tweet_count+1)
            # with open("out.csv", "a", encoding='utf-8') as f:
            savefile.write("%s~%s\n" % (status.created_at,(text).encode('utf-8')))
            tweet_count +=1
        else:
            print("All 100 tweets streamed")
            sys.exit()

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        sys.exit()

if __name__ == "__main__":
    # complete authorization and initialize API endpoint
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    # initialize stream
    streamListener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=streamListener,tweet_mode='extended')
    tags = ["taxi", 'Taxi']
    stream.filter(track=tags, languages=['en'])
savefile.close()

