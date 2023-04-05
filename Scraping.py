import praw
import pandas as pd
# Define the Reddit API credentials and user agent
reddit = praw.Reddit(
    client_id='fSkTgQ6SGruc2s4FpYIXKA',
    client_secret='z-0IvOzusyI9RcavZx9dtFKN6JWY-w',
    username='ayoub_oulkadi',
    password='Zxcv20050',
    user_agent='your_user_agent')

# Define the Kafka topic you want to produce messages to
topic_name = 'crypto-posts'

# Define the Kafka broker(s) you want to connect to
bootstrap_servers = ['localhost:9092']

# Create a KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Define the keywords for each topic
keywords = {
    'crypto' : ['51% attack', 'ASIC', 'bear market', 'bull market', 'BUIDL', 'bounty', 'burn', 'Casper', 'consensus', '>}

# Define a function to filter submissions by keywords and send them to Kafka
def stream(keywords):
    for submission in reddit.subreddit('all').search(keywords, limit=5):
        post = {
            'id': submission.id,
            'text': submission.selftext,
            'created_at': submission.created_utc,
            'user_id': str(submission.author),
            'subreddit': submission.subreddit.display_name,
            'user_name': submission.author.name,
            'followers_count': submission.subreddit.subscribers,
        }
        # Send the post to Kafka
        producer.send(topic_name, post)
        print(f"Sent post '{post}' to Kafka topic '{topic_name}'")

# Start streaming posts and sending them to Kafka
stream(keywords['crypto'])