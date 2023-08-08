import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "u49nVFIP0g3b95lcId4dgkt4lz73Ury71oUf5SbF2d1et58ftP" 
    access_secret = "CqUl5v73SJJ3muOTTZYsuMIP6" 
    consumer_key = "1686286715672281088-qCDM5J4Q6v6hbD3QF2WHG9KWeO2ZPY"
    consumer_secret = "xIkurKfnnk8L41Oj9ADCcVA5NfrhffHzE3BLPsEquuV0p"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            count=200,
                            include_rts = False,
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://ngocnq-astrosdk/refined_tweets.csv')