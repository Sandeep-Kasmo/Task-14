import os
import time
import tweepy
import pandas as pd
from datetime import datetime

# --- Configuration Constants ---
# NOTE: Set this as an Environment Variable (e.g., $env:BEARER_TOKEN="...")
BEARER_TOKEN = os.environ.get("BEARER_TOKEN") 
SEARCH_QUERY = '#MyCampaignName OR "product review" lang:en -is:retweet -is:reply'
MAX_TWEETS_TO_COLLECT = 5000  # Total limit of tweets you want to collect
MAX_RESULTS_PER_REQUEST = 100 # Maximum allowed per single request in V2 API
OUTPUT_FILE = 'campaign_sentiment_data.csv'

# --- Initialization ---
# --- Initialization ---
def initialize_client():
    """Initializes the Tweepy Client with the Bearer Token and rate limit handling."""
    if not BEARER_TOKEN:
        print("ERROR: BEARER_TOKEN environment variable not set.")
        exit(1)
    
    # *** MODIFICATION HERE ***
    # Set wait_on_rate_limit=True to automatically pause and resume upon hitting the limit.
    return tweepy.Client(
        bearer_token=BEARER_TOKEN,
        wait_on_rate_limit=True
    )

def collect_tweets_to_dataframe(client: tweepy.Client, query: str, limit: int) -> pd.DataFrame:
    """
    Collects tweets using the Paginator with rate limit handling.
    """
    print(f"Starting collection for query: '{query}'")
    tweets_list = []
    
    # Use Tweepy's Paginator for automatic iteration across result pages
    paginator = tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=["created_at", "lang", "public_metrics"],
        expansions=["author_id"],
        max_results=MAX_RESULTS_PER_REQUEST,
        limit=limit  # Total number of tweets to collect
    )

    for page in paginator:
        if page.data:
            for tweet in page.data:
                # Flatten the complex Tweet object into a simple dictionary (row)
                row = {
                    'tweet_id': tweet.id,
                    'text': tweet.text,
                    'created_at': tweet.created_at,
                    'language': tweet.lang,
                    'author_id': tweet.author_id,
                    'retweet_count': tweet.public_metrics.get('retweet_count', 0),
                    'like_count': tweet.public_metrics.get('like_count', 0),
                    'reply_count': tweet.public_metrics.get('reply_count', 0)
                }
                tweets_list.append(row)
        
        print(f"Collected {len(tweets_list)} tweets so far...")

        # --- Manual Rate Limit Handling (Fallback) ---
        # The paginator handles most rate limits internally, but it's good practice
        # to add a small pause between batches to prevent consecutive 429s.
        time.sleep(3) # Wait 3 seconds between 100-tweet requests

    return pd.DataFrame(tweets_list)

# --- Main Execution ---
if __name__ == "__main__":
    try:
        client = initialize_client()
        
        # 1. Collect Data
        df_tweets = collect_tweets_to_dataframe(
            client=client,
            query=SEARCH_QUERY,
            limit=MAX_TWEETS_TO_COLLECT
        )
        
        # 2. Load/Save Data
        if not df_tweets.empty:
            df_tweets.to_csv(OUTPUT_FILE, index=False)
            print(f"\n--- SUCCESS ---")
            print(f"Total tweets collected: {len(df_tweets)}")
            print(f"Data saved to: {OUTPUT_FILE}")
        else:
            print("\n--- WARNING ---")
            print("No tweets were collected. Check your query or API access.")

    except tweepy.errors.TooManyRequests as e:
        # Catch the specific 429 error
        print("\n--- CRITICAL ERROR: RATE LIMIT EXCEEDED (429) ---")
        print("The script was blocked by X's API. Wait at least 15 minutes before re-running.")
        # You would typically log the exact time and stop the process in production
        print(f"Error details: {e}")
        
    except tweepy.errors.TweepyException as e:
        # Catch other API errors (e.g., authentication, invalid query)
        print(f"\n--- TWEEPY ERROR ---")
        print(f"A Tweepy API error occurred: {e}")
        
    except Exception as e:
        # Catch any unexpected system errors
        print(f"\n--- UNEXPECTED ERROR ---")
        print(f"An unexpected error occurred: {e}")