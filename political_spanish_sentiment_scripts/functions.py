import tweepy
import pandas as pd
import re
import datetime
import numpy as np

# machine learning
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification


# UPDATING BBD TWEETS

def extracting_tweets(df, account, string_party, string_post_type, string_campaign):
    """
    This function extract a data frame with the tweets of an account
    and filter the data frame with the columns selected.
    Input:
    - df: bbdd
    - account: twitter account.
    - string_party: 'Partido Popular', 'PSOE'...
    - string_post_type: 'publicación'
    - string_campaing: 'general' or 'madrid'
    Output: a data frame with the last tweets of the accoount indicated.
    """

    # api
    CONSUMER_KEY = '5lXBIUCjAkOnbIvMlMJn7dlaS'
    CONSUMER_SECRET = 'qeRlxcGaTmbCkNk1bMtsSlU0tVCAFL928wH8cqPi8VJX4mqkLh'
    ACCESS_TOKEN = '787642138263683072-JnFO9ahRyIldLnmVCpfx4EP61Z33Jnr'
    ACCESS_TOKEN_SECRET = 'FPPnZwmV1f6VqqcYzzMtXtxO9TL0wxf2TwNxYMkhfFE6k'
    BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAALNMOAEAAAAAP%2BesQx5avGgJJ93HpEfmJ5Itg0g%3Df9o5pCoaONvJDna12ko5StopuNOPFZeJrVgd3ykj83Z38IzkiJ'

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # parameters
    last_tweet_id = max(df['id'])
    count = 200

    # extracting the tweets
    tweets = [tweet for tweet in tweepy.Cursor(api.user_timeline,
                                               screen_name=account,
                                               tweet_mode='extended',
                                               exclude_replies=True,
                                               max_id=last_tweet_id,
                                               include_rts=False).items(count)]
    # creating the data frame
    tweets_json = [tweet._json for tweet in tweets]
    df_tweets = pd.json_normalize(tweets_json)

    # selecting useful columns
    columns_selected = ['user.name', 'created_at', 'id', 'full_text', 'display_text_range',
                        'source', 'retweet_count', 'favorite_count', 'user.followers_count',
                        'user.friends_count', 'user.statuses_count']

    df_tweets_filtered = df_tweets[columns_selected]

    # cleaning date time
    df_tweets_filtered.loc[:, 'created_at'] = pd.to_datetime(df_tweets_filtered['created_at'])

    # cleaning source of the tweet
    list_sources = list(df_tweets_filtered['source'])
    df_tweets_filtered.loc[:, 'source'] = [re.findall(r'\>(.*?)\<', s) for s in list_sources]

    # adding a column with the party
    df_tweets_filtered['partido'] = string_party

    # adding a column with the type of post (publicación o mención)
    df_tweets_filtered['tipo de post'] = string_post_type

    # adding a column with the type of post (publicación o mención)
    df_tweets_filtered['campaña'] = string_campaign

    # extract hashtags (column ['full_text'])
    list_hashtags = list(df_tweets_filtered['full_text'])
    hashtags = [re.findall(r"#(\w+)", tweet) for tweet in list_hashtags]
    df_tweets_filtered['hashtags'] = hashtags

    return df_tweets_filtered



# UPTDATING BBDD MENTIONS

def extracting_mentions(query):
    """
    This function extract a data frame with the result of a query.
    """
    # api connection
    CONSUMER_KEY = '5lXBIUCjAkOnbIvMlMJn7dlaS'
    CONSUMER_SECRET = 'qeRlxcGaTmbCkNk1bMtsSlU0tVCAFL928wH8cqPi8VJX4mqkLh'
    ACCESS_TOKEN = '787642138263683072-JnFO9ahRyIldLnmVCpfx4EP61Z33Jnr'
    ACCESS_TOKEN_SECRET = 'FPPnZwmV1f6VqqcYzzMtXtxO9TL0wxf2TwNxYMkhfFE6k'
    BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAALNMOAEAAAAAP%2BesQx5avGgJJ93HpEfmJ5Itg0g%3Df9o5pCoaONvJDna12ko5StopuNOPFZeJrVgd3ykj83Z38IzkiJ'

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # date and amount information
    count = 15000

    today = datetime.date.today()
    yest = today - datetime.timedelta(days=1)
    yest2 = today - datetime.timedelta(days=2)
    yest3 = today - datetime.timedelta(days=3)
    week = today - datetime.timedelta(days=7)
    tomorrow = today + datetime.timedelta(days=1)
    since = week
    until = yest3

    print(f'Until: {until}')
    print(f'Since: {since}')

    # getting the mentions

    print(f'Getting mentions for {query} since {since} until {until}.')

    mentions = [tweet for tweet in tweepy.Cursor(api.search,
                                                 q=query + ' -filter:retweets',
                                                 lang="es",
                                                 tweet_mode='extended',
                                                 until=until,
                                                 since=since,
                                                 result_type="recent").items(count)]
    mentions_json = [tweet._json for tweet in mentions]
    df_mentions = pd.json_normalize(mentions_json)

    # selecting useful columns
    columns_selected = ['user.name', 'created_at', 'id', 'full_text', 'display_text_range',
                        'source', 'retweet_count', 'favorite_count', 'user.followers_count',
                        'user.friends_count', 'user.statuses_count']

    df_mentions_filtered = df_mentions[columns_selected]

    # cleaning date time
    df_mentions_filtered.loc[:, 'created_at'] = pd.to_datetime(df_mentions_filtered['created_at'])

    # cleaning source of the tweet
    list_sources = list(df_mentions_filtered['source'])
    df_mentions_filtered.loc[:, 'source'] = [re.findall(r'\>(.*?)\<', s) for s in list_sources]

    return df_mentions_filtered


# MACHINE LEARNING CLASSIFICATION: 1, 2, 3, 4 OR 5 STARTS
def sentiment_classificaton(df, column):
    model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    classifier = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

    list_tweets = column.tolist()

    results = classifier(list_tweets)

    sentiment_list = []
    scores = []

    for result in results:
        scores_list = result['score']
        scores.append(scores_list)

        for i in range(len(result)):
            results_list = result['label'][i]
            if results_list != " ":
                sentiment_list.append(results_list)

    df['sentimiento'] = sentiment_list
    df['scores'] = scores

    return df

