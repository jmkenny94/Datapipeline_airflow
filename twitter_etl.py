import tweepy 
import pandas as pd 
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    access_key = "SAbEjK747x9C66eLnB2GCyQ9V"
    access_secret = "mg6qlwVDs6UNHAn02dN4AaktSoFvcb5bF1cGaPuK3BN04kr1e8"
    consumer_key = '1606657386642563078-Rhc0HGJdrhQHTAyOxbGob5Yh8jrVHi'
    consumer_secret = 'rWFYv9tHHgK7lPS16Svk6yAKmlI9R3sElhus0xcV63NE8'

    ##Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #Creating an API object 
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name = "@elonmusk",
                                #200 maximum count 
                                count=200,
                                include_rts = False,
                                #necessary to keep full_text
                                #otherwise only the first 140 words 
                                tweet_mode = 'extended'
    )

    tweet_list=[]
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text': text,
                        'favorite_count': tweet.favorite_count,
                        'retweet_count': tweet.retweet_count,
                        'created_at': tweet.created_at}


        tweet_list.append(refined_tweet)


    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://twitter-airflow-project-mk/elonmusk_twitter_data.csv")

