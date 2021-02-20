import sys
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer
from constants import *

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

# https://stream.twitter.com/1.1/statuses/filter.json?delimited=length&track=twitterapi:
# https://stream.twitter.com/1.1/statuses/filter.json?track=twitter

def get_tweets():
    # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/guides/basic-stream-parameters
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # Continental USA, https://boundingbox.klokantech.com/
    # ('locations', '-169.7369071437,23.6595686079,-64.35549002,71.60482164')
    query_data = [('delimited', 'length'), ('language', 'en'), ('track','bitcoin,monero,ripple,ybcoin,dogecoin,dash,maidsafecoin,lisk,siacoin,criptocurrency,cripto'), ('locations', '-169.7369071437,23.6595686079,-64.35549002,71.60482164')]
    # query_data = [('delimited', 'length'), ('language', 'en'), ('track','bitcoin,monero,ripple,ybcoin,dogecoin,dash,maidsafecoin,lisk,siacoin,criptocurrency,cripto')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def remove_non_ascii(x):
    return x.encode('ascii', 'ignore').decode('ascii').replace('\n', ' ').replace('\t', ' ')

def send_tweets_to_kafka(http_resp, producer, topic):
    for line in http_resp.iter_lines(): 
        try:
            # JSON load
            full_tweet = json.loads(line)
            # Extraction
            tweet_text = remove_non_ascii(full_tweet['text'])
            tweet_screen_mame = remove_non_ascii(full_tweet['user']['screen_name'])
            tweet_place = remove_non_ascii(full_tweet['place']['full_name'])
            tweet_country = remove_non_ascii(full_tweet['place']['country'])
            tweet_lang = remove_non_ascii(full_tweet['lang'])
            # Print
            print("Tweet Text: " + tweet_text)
            print("Message written by {} in {}, {}, in the language {}.".format(tweet_screen_mame, tweet_place, tweet_country, tweet_lang))
            print("-"*20)
            # Send to Kafka
            producer.send(topic, value=tweet_text + '_eot')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

http_resp = get_tweets()
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                         value_serializer=lambda x: x.encode('utf-8'))
send_tweets_to_kafka(http_resp, producer, KAFKA_TOPIC_TWEET)
