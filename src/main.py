import tweepy
import pandas as pd
import re
from textblob import TextBlob
from keys import *
 
# -------------------------------
# Twitter API Authentication
# -------------------------------
 
client = tweepy.Client(
    bearer_token=Bearer_Token_api,
    consumer_key=api_key,
    consumer_secret=api_secret,
)
 
# -------------------------------
# Collect Tweets
# -------------------------------
query = "PUMA shoes -is:retweet lang:en"   # Change keyword here
tweets = client.search_recent_tweets(query=query, max_results=100,
                                     tweet_fields=["created_at", "text", "author_id"])
 
tweet_list = []
for t in tweets.data:
    tweet_list.append([t.id, t.text, t.created_at, t.author_id])
 
df = pd.DataFrame(tweet_list, columns=["tweet_id", "tweet_text", "created_at", "author_id"])
print("\n🔹 Raw tweets collected:")
print(df.head())
 
# -------------------------------
# Filter & Clean Tweets
# -------------------------------
def clean_tweet(text):
    text = text.lower()
    text = re.sub(r"http\S+|www\S+", "", text)      # Remove URLs
    text = re.sub(r"@\w+", "", text)                # Remove mentions
    text = re.sub(r"#\w+", "", text)                # Remove hashtags
    text = re.sub(r"[^a-zA-Z\s]", "", text)         # Remove special characters
    text = re.sub(r"\s+", " ", text).strip()        # Remove extra spaces
    return text
 
df["clean_text"] = df["tweet_text"].apply(clean_tweet)
 
print("\n🔹 Cleaned tweets:")
print(df[["tweet_text", "clean_text"]].head())
 
# -------------------------------
# Sentiment Analysis
# -------------------------------
def get_sentiment(text):
    score = TextBlob(text).sentiment.polarity
    if score > 0: return "Positive"
    elif score < 0: return "Negative"
    else: return "Neutral"
 
df["sentiment"] = df["clean_text"].apply(get_sentiment)
 
print("\n🔹 Sentiment Results:")
print(df[["clean_text", "sentiment"]].head())
 
# -------------------------------
# Save to CSV
# -------------------------------
# df.to_csv("tweet_sentiment_report.csv", index=False)
print("\n CSV saved as: tweet_sentiment_report.csv")
