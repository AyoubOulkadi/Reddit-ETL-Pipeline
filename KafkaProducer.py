from kafka import KafkaProducer
import praw
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
# Define the Reddit API credentials and user agent
reddit = praw.Reddit(
    client_id='fSkTgQ6SGruc2s4FpYIXKA',
    client_secret='z-0IvOzusyI9RcavZx9dtFKN6JWY-w',
    username='ayoub_oulkadi',
    password='Zxcv20050',
    user_agent='your_user_agent')

# Define the Kafka topic names
topics = {
    'high_prices': 'HighPrices',
    'government': 'GovMorocco',
    'national_team_cup': 'nationalteam',
    'layoffs': 'Layoffs'
}
# Define the keywords for each topic
keywords = {
    'high_prices': ['high prices', 'expensive', 'overpriced', 'pricey', 'costly', 'exorbitant', 'outrageous', 'prohibitive', 'unaffordable', 'luxury', 'premium', 'steep', 'spendy', 'inflation>    'government': ['government', 'politics', 'election', 'democracy', 'republic', 'monarchy', 'dictatorship', 'authoritarian', 'totalitarian', 'fascism', 'communism', 'socialism', 'capitalism>    'keywords_moroccan_team' :['Moroccan national team', 'Morocco football', 'Morocco soccer', 'Moroccan league', 'Moroccan football federation', 'Moroccan football championship', 'Moroccan f>    'keywords_layoffs' : ['Layoffs', 'Job cuts', 'Redundancies', 'Retrenchment', 'Termination', 'Displacement', 'Downsizing', 'Rightsizing', 'Restructuring', 'Reorganization', 'Streamlining',>}

# Define the Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Define a function to filter submissions by keywords and send them to Kafka
def stream(topic, keywords):
    for submission in reddit.subreddit('all').search(keywords, limit=10):
        post = {
            'id': submission.id,
            'text': submission.selftext,
            'created_at': submission.created_utc,
            'user_id': str(submission.author),
            'subreddit': submission.subreddit.display_name,
            'user_name': submission.author.name,
            'followers_count': submission.subreddit.subscribers,
        }
        producer.send(topics[topic], value=post)
        print(f"Sent post '{submission.title}' to Kafka topic '{topics[topic]}'")

threads = []
# Create a thread for each topic
for topic, kw in keywords.items():
    t = threading.Thread(target=stream, args=(topic, kw))
    t.start()
    threads.append(t)

# Wait for all threads to complete
for t in threads:
    t.join()