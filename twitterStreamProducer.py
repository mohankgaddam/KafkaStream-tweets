
#Import required libraries

import os
import tweepy
import json
from pathlib import Path
from kafka import KafkaProducer
from tweepy.streaming import StreamListener
from tweepy import Stream

#path to the config file that has twitter authentication keys

consumer_key = "<INSERT KEY>"
consumer_secret = "<INSERT KEY>"
access_token = "<INSERT KEY>"
access_secret = "<INSERT KEY>"

def authenticateTwitter():
    """
    This function authenticates and connects to twitter by creating Twitter API object
    """
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    print(twitter.me())
    #print("Connected as {}".format(twitter.me().screen_name))
    
    return twitter

class tweetStreamListener(StreamListener):

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def on_data(self, data):
        """
        This method handles tweets from the stream and ingested into kafka producer
        """
        try:
            raw_data = json.loads(data)
            if "text" in raw_data:
                tweet = raw_data['text'].encode('utf-8')
				#Publish the message to the topic "tweet"
                self.producer.send('tweet', tweet)
                print(tweet)
        except:
            pass
            
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    print("Running the streaming program")
    # authenticate twitter
    twitter = authenticateTwitter()
    #create stream listener
    myStreamListener = tweetStreamListener()
    myStream = Stream(auth=twitter.auth, listener=myStreamListener)
	#Filter the tweets with given words
    myStream.filter(track="Kafka", languages=["en"])