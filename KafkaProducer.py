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
topic = {
    'crypto': 'Cryptocurrency'
}
# Define the keywords for each topic
keywords = {
    'crypto': ["Bitcoin", 
    "Ethereum", 
    "Ripple", 
    "Litecoin", 
    "Bitcoin Cash", 
    "Stellar", 
    "Cardano", 
    "Chainlink", 
    "Polkadot", 
    "Dogecoin", 
    "Tether", 
    "Binance Coin", 
    "Monero", 
    "Zcash", 
    "Dash", 
    "NEM", 
    "IOTA", 
    "EOS", 
    "Tron", 
    "VeChain", 
    "Bitcoin SV", 
    "Cosmos", 
    "Tezos", 
    "Synthetix", 
    "Compound", 
    "Uniswap", 
    "Aave", 
    "Maker", 
    "Curve", 
    "Yearn Finance", 
    "SushiSwap", 
    "Kyber Network", 
    "REN", 
    "1inch", 
    "Balancer", 
    "Chain Guardians", 
    "CryptoKitties", 
    "Cryptovoxels", 
    "Decentraland", 
    "Gods Unchained", 
    "Axie Infinity", 
    "The Sandbox", 
    "Nifty Gateway", 
    "SuperRare", 
    "OpenSea", 
    "CryptoPunks", 
    "Meebits", 
    "Bored Ape Yacht Club", 
    "Art Blocks", 
    "Golem", 
    "Augur", 
    "Quantstamp", 
    "Enjin Coin", 
    "Hedera Hashgraph", 
    "Chainalysis", 
    "Coinbase", 
    "Binance", 
    "Kraken", 
    "Gemini", 
    "Bitfinex", 
    "OKEx", 
    "Huobi", 
    "Upbit", 
    "Bitstamp", 
    "KuCoin", 
    "FTX", 
    "Crypto.com", 
    "BitMEX", 
    "Bybit", 
    "BitFlyer", 
    "Coincheck", 
    "BitBay", 
    "Bithumb", 
    "Coinone", 
    "Liquid", 
    "Zebpay", 
    "WazirX", 
    "Bitso", 
    "Mercado Bitcoin", 
    "Binance Academy", 
    "CoinMarketCap", 
    "CoinGecko", 
    "CryptoCompare", 
    "Messari", 
    "CoinDesk", 
    "The Block", 
    "Decrypt", 
    "Cointelegraph", 
    "Bitcoin Magazine", 
    "CryptoSlate", 
    "Coin Telegraph", 
    "Coin Telegraph Brazil", 
    "Cointelegraph Japan", 
    "The Merkle", 
    "Coinnounce", 
    "Bitcoinist", 
    "Crypto Briefing", 
    "CryptoSlate", 
    "Blockchain News", 
    "NewsBTC", 
    "BeInCrypto", 
    "CoinCentral", 
    "Crypto Insider", 
    "Blockonomi", 
    "Coinnounce", 
    "Bitcoin.com", 
    "Bitcoin News", 
    "Bitcoin.org", 
    "CryptoCurrency News", 
    "CryptoSlate News", 
    "CryptoSlate Research", 
    "BlockFi", 
    "Nexo", 
    "Ledger", 
    "Trezor", 
    "MetaMask"]
}

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
        producer.send(topic[topic], value=post)
        print(f"Sent post '{submission.title}' to Kafka topic '{topic[topic]}'")

threads = []
# Create a thread for each topic
for topic, kw in keywords.items():
    t = threading.Thread(target=stream, args=(topic, kw))
    t.start()
    threads.append(t)

# Wait for all threads to complete
for t in threads:
    t.join()