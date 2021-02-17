from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import regexp_replace, udf, col
from pyspark.sql.types import LongType, StringType
from pyspark.streaming.kafka import KafkaUtils
import sys

from constants import *

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

# Tweet preprocessing
def text_cleaup(line):
    line = line.withColumn('tweet', regexp_replace('tweet', r'http\S+', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', '@\w+', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', '#', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', 'RT', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', ':', ''))
    line = line.withColumn('tweet', regexp_replace('tweet', '\n', ' '))
    line = line.withColumn('tweet', regexp_replace('tweet', '\t', ' '))
    return line

def process_rdd(time, rdd, header):
    print("{sep} {header} {sep} {time} {sep}".format(sep="-"*5, time=str(time), header=header))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(tweet=w[0], tweet_count=w[1]))
        df = sql_context.createDataFrame(row_rdd)
        df = text_cleaup(df)

        from textblob import TextBlob

        def polarity_detection(text):
            return TextBlob(text).sentiment.polarity
        def subjectivity_detection(text):
            print(text)
            return TextBlob(text).sentiment.subjectivity

        # df = sqlContext.createDataFrame([{'name': 'Alice', 'age': 1}])
        # df.withColumn("maturity", maturity_udf(df.age))

        # udf_subjectivity_detection = udf(subjectivity_detection, StringType())

        # custom_udf = udf(lambda x: subjectivity_detection(x), StringType())

        result = df.select([subjectivity_detection(x) for x in col('tweet')])

        print(result)
        # df = df.withColumn('tweet_super_count', custom_udf(df.tweet))
        # df = df.withColumn("subjectivity", lit('saa'))

        # print(df.take(5))
        # df.printSchema()
        # df.show(5)
        # df.pprint()
        # df.orderBy("tweet_count", ascending=False).show(10)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp_JPBM")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
# Only one StreamingContext can be active in a JVM at the same time. (https://spark.apache.org/docs/2.0.0/streaming-programming-guide.html)
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery
# TODO: setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

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
tweets_totals = tweets.updateStateByKey(aggregate_tags_count)
tweets_totals.foreachRDD(lambda time, rdd: process_rdd(time, rdd, "Tweets total"))

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
