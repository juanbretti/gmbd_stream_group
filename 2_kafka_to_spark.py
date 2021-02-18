from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import regexp_replace, udf, col, lit
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.streaming.kafka import KafkaUtils
# import pandas as pd
import sys
from textblob import TextBlob
from constants import *

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

# Tweet preprocessing
# Remove unwanted characters from the tweet, line new lines and tabs
def text_cleanup(line):
    line = line.withColumn('tweet', regexp_replace('tweet', r'http\S+', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', '@\w+', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', '#', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', 'RT', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', ':', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', '\n', ' '))
    line = line.withColumn('tweet', regexp_replace('tweet', '\t', ' '))
    return line


def process_rdd(time, rdd, header, words_positive, words_negative):
    print("{sep} {header} {sep} {time} {sep}".format(sep="-"*5, time=str(time), header=header))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(tweet=w[0], tweet_count=w[1]))
        df = sql_context.createDataFrame(row_rdd)
        df = text_cleanup(df)

        # Filter is the tweet contains any of the predefined positive or negative terms
        # https://stackoverflow.com/a/48874376/3780957
        df_positive = df.filter(col('tweet').rlike('(^|\s)(' + '|'.join(words_positive) + ')(\s|$)'))
        df_negative = df.filter(col('tweet').rlike('(^|\s)(' + '|'.join(words_negative) + ')(\s|$)'))

        # Check what is trending
        if (df_positive.count() > df_negative.count()):
            print('Positive: {} positive vs {} negative'.format(df_positive.count(), df_negative.count()))
        else:
            print('Negative: {} positive vs {} negative'.format(df_positive.count(), df_negative.count()))

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf()
conf.setAppName("Bitcoin_Recommender")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
# Only one StreamingContext can be active in a JVM at the same time. (https://spark.apache.org/docs/2.0.0/streaming-programming-guide.html)
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery
# TODO: setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# Read word for sentiment analysis
ssc_session = SparkSession(sc)
# Converted to Python lists
# https://stackoverflow.com/a/64764406/3780957
words_positive = ssc_session.read.option("header", "false").csv("words/positive-words.txt").select('_c0').rdd.flatMap(list).collect()
words_negative = ssc_session.read.option("header", "false").csv("words/negative-words.txt").select('_c0').rdd.flatMap(list).collect()

# read data from the port
dataStream = KafkaUtils.createStream(ssc, KAFKA_ZOOKEEPER_SERVERS, 'streaming-consumer', {KAFKA_TOPIC: 1})
# No need to decode() from 'utf-8' in Python3
dataStream = dataStream.map(lambda x: x[1].lower())
# Split each single tweet. The "end of tweet" was set at the Twitter pull application
lines = dataStream.flatMap(lambda line: line.split('_eot'))
# Filter the null or hashtags
lines = lines.filter(lambda x: x not in ['', ' ', '#'])
# Remove '_eot' and add counter
tweets = lines.map(lambda x: (x.replace('_eot', ''), 1))
tweets_totals = tweets.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 10, 2)
tweets_totals.foreachRDD(lambda time, rdd: process_rdd(time, rdd, "Tweets total", words_positive, words_negative))

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()

