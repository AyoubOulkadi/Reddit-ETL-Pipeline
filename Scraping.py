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
    'crypto' : ["Bitcoin", 
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
    "MetaMask"]}

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