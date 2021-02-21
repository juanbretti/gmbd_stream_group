# https://www.jcchouinard.com/documentation-on-reddit-apis-json/
# https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c

import requests
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from constants import *
import sys
import time

# Parameters
subreddit = 'Bitcoin'
limit = 1
timeframe = 'month' #hour, day, week, month, year, all
listing = 'new' # controversial, best, hot, new, random, rising, top

# authenticate API
client_auth = requests.auth.HTTPBasicAuth(REDDIT_PERSONAL_USE, REDDIT_SECRET_TOKEN)

# we use this function to convert responses to dataframes
def df_from_response(res):
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    # loop through each post pulled from res and append to df
    for post in res.json()['data']['children']:
        df = df.append({
            'subreddit': post['data']['subreddit'],
            'title': post['data']['title'],
            'selftext': post['data']['selftext'],
            'upvote_ratio': post['data']['upvote_ratio'],
            'ups': post['data']['ups'],
            'downs': post['data']['downs'],
            'score': post['data']['score'],
            'link_flair_css_class': post['data']['link_flair_css_class'],
            'created_utc': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'id': post['data']['id'],
            'kind': post['kind']
        }, ignore_index=True)

    return df

def remove_non_ascii(x):
    return x.encode('ascii', 'ignore').decode('ascii').replace('\n', ' ').replace('\t', ' ')

# Parameters
params = {'limit': 1}
headers = {'User-agent': 'gmbd'}

def send_reddit_to_kafka(producer, topic):
    try:
        # make request
        base_url = 'https://www.reddit.com/r/{}/{}.json?limit={}&t={}'.format(subreddit, listing, limit, timeframe)
        res = requests.get(base_url, headers=headers, params=params)

        # get dataframe from response
        new_df = df_from_response(res)
        # take the final row (oldest entry)
        row = new_df.iloc[len(new_df)-1]
        # create fullname
        fullname = row['kind'] + '_' + row['id']
        # add/update fullname in params
        params['after'] = fullname
        
        # Print
        print("Reddit Title: " + new_df['title'][0])
        print("Reddit Text: " + new_df['selftext'][0])
        print("-"*20)

        # Send to Kafka
        reddit_text = remove_non_ascii(new_df['title'][0] + " " + new_df['selftext'][0])
        producer.send(topic, value= reddit_text + '_eot')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        print("-"*20)

producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                         value_serializer=lambda x: x.encode('utf-8'))

while True:
    send_reddit_to_kafka(producer, KAFKA_TOPIC_TWEET)
    time.sleep(0.5)

