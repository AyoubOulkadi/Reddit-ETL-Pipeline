import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark
from pyspark.sql.functions import when,concat_ws
from subprocess import call
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, udf, regexp_replace, isnull
from pyspark.sql.functions import split, col, trim
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, DateType, IntegerType, DoubleType, FloatType
from pyspark.sql import SparkSession
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import format_number as fmt
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import re
import numpy as np
import pandas as pd
import nltk
from os import path, getcwd
import matplotlib.pyplot as plt

def removePattern(inputText, pattern):
    r = re.findall(pattern, inputText)
    for i in r:
        inputText = re.sub(i, '', inputText)
    return inputText

def cleanTweet(txt):
    '''
    Remove twitter return handles (RT @xxx:)
    '''
    txt = removePattern(txt, 'RT @[\w]*:')
    '''
    Remove twitter handles (@xxx)
    '''
    txt = removePattern(txt, '@[\w]*')
    '''
    Remove URL links (httpxxx)
    '''
    txt = removePattern(txt, 'https?://[A-Za-z0-9./]*')
    '''
    Remove special characters, numbers, punctuations
    '''
    txt = re.sub('[^A-Za-z]+', ' ', txt)
    return txt

def getCleanTweetText(filteredTweetText):
    return ' '.join(filteredTweetText)

def getSentimentScore(tweetText):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(tweetText)
    return float(vs['compound'])

def getSentiment(score):
    return 1 if score > 0 else 0

def getTweetArray(tweet):
    return tweet.split(' ')

def preprocessing():
    spark = SparkSession.builder.appName("DataFrame").getOrCreate()
    df1=spark.read.csv('s3://reddit--data/Reddit_data.csv',header=True)    
    df2 = df1.dropna()
    df3 = df2.withColumn("id", col("id").cast(StringType())) \
             .withColumn("_c0", col("_c0").cast(IntegerType())) \
             .withColumn("followers_count", col("followers_count").cast(IntegerType()))
    df4=df3.dropna()         
    udfCleanTweet = udf(cleanTweet, StringType())
    dfCleanTweet = df4.withColumn('cleanTweetText', udfCleanTweet('text'))
    dfCleanTweet.select('text', 'cleanTweetText').show(5)
    tokenizer = Tokenizer(inputCol='cleanTweetText', outputCol='words')
    dfCleanTweetTokenized = tokenizer.transform(dfCleanTweet)
    dfCleanTweetTokenized.select('text', 'cleanTweetText', 'words').show(5)
    remover = StopWordsRemover(inputCol='words', outputCol='filteredTweetText')
    dfCleanTweetTokenizedFiltered = remover.transform(dfCleanTweetTokenized)
    dfCleanTweetTokenizedFiltered.select('text', 'cleanTweetText', 'words', 'filteredTweetText').show(5)
    word2Vec = Word2Vec(inputCol='filteredTweetText', outputCol='word2vec')
    model = word2Vec.fit(dfCleanTweetTokenizedFiltered)
    dfCleanTweetTokenizedFilteredVec = model.transform(dfCleanTweetTokenizedFiltered)
    dfCleanTweetTokenizedFilteredVec.select('text', 'cleanTweetText', 'words', 'filteredTweetText', 'word2vec').show(5)
   
    udfGetCleanTweetText = udf(getCleanTweetText, StringType())
    dfCleanTweetText = dfCleanTweetTokenizedFilteredVec.withColumn('tweetText', udfGetCleanTweetText('filteredTweetText'))
    dfCleanTweetText.select('text', 'cleanTweetText', 'words', 'filteredTweetText', 'word2vec', 'tweetText').show(5)
    udfGetSentimentScore = udf(getSentimentScore, FloatType())
    dfCleanTweetTextScore = dfCleanTweetText.withColumn('sentimentScore', udfGetSentimentScore('tweetText'))
    dfCleanTweetTextScore.select('text', 'cleanTweetText', 'words', 'filteredTweetText', 'word2vec', 'tweetText', 'sentimentScore').show(5)
    udfGetSentiment = udf(getSentiment, IntegerType())
    dfCleanTweetTextScoreSentiment = dfCleanTweetTextScore.withColumn('sentiment', udfGetSentiment('sentimentScore'))
    dfCleanTweetTextScoreSentiment.select('cleanTweetText', 'sentimentScore', 'sentiment').show(5,False)
    dfCleanTweetTextScoreSentiment = dfCleanTweetTextScoreSentiment.withColumn("sentiment_label", when(dfCleanTweetTextScoreSentiment.sentiment >= 0, 1).otherwise(0))
    dfCleanTweetTextScoreSentiment.select('cleanTweetText', 'sentimentScore', 'sentiment_label').show(5,False)
    return dfCleanTweetTextScoreSentiment
 
    
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'file_path'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
## Create a Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3 = args['bucket_name']
file_path = args['file_path']

# apply preprocessing function
preprocessed_df = preprocessing()

# save preprocessed data to S3 as a CSV file
preprocessed_df1=preprocessed_df.toPandas()
preprocessed_df1.to_csv(f"s3://{s3}/preprocessed_reddit_data.csv", index=False)
job.commit()