import asyncio
from twikit import Client, TooManyRequests
from datetime import datetime
from configparser import ConfigParser
from db_connection import get_db_connection
import re

MINIMUM_TWEETS = 1000
QUERY = '$ETH'

config = ConfigParser()
config.read('config.ini')
username = config["X"]['username']
password = config["X"]['password']
email = config["X"]['email']

async def main():
    conn = await get_db_connection()  # Use asyncpg connection
    
    client = Client()

    # await client.login(
    #     auth_info_1=username,
    #     auth_info_2=email,
    #     password=password
    # )
    # client.get_cookies()
    # client.save_cookies('cookies.json')

    client = Client(language='en-US')
    client.load_cookies('cookies.json')

    def categorize_text(text):
        # Expanded list of cryptocurrency-related keywords
        crypto_keywords = [
            "ETH", "Ethereum", "Bitcoin", "BTC", "crypto", "blockchain", "token", 
            "airdrop", "wallet", "altcoin", "DeFi", "NFT", "Web3", "smart contract", 
            "exchange", "cryptocurrency", "staking", "mining"
        ]

        if any(word in text for word in crypto_keywords):
            return "Cryptocurrency Related"
        else:
            return "Other"

    tweet_count = 0
    tweets = None
    while tweet_count < MINIMUM_TWEETS:
        if tweets is None:
            print(f'{datetime.now()} -- Getting tweets...')
            tweets = await client.search_tweet(QUERY, product='TOP')
        else:
            print(f'{datetime.now()} -- Getting tweets...')
            tweets = await tweets.next()

        if not tweets:
            print(f'{datetime.now()} -- No more tweets found')

        for tweet in tweets:
            # print(vars(tweet))
            tweet_count += 1
            tweet_data = {
                'tweet_id': tweet._data['rest_id'], # CORRECT
                'user_id': tweet._data['core']['user_results']['result']['rest_id'], # CORRECT
                'text': tweet.text,  # content correct
                'created_at': tweet._data['legacy']['created_at'], # first wrong now correct
                'retweet_count': tweet._data['legacy']['retweet_count'], # correct
                'like_count' : tweet._data.get('favorite_count', 0),
                'reply_count': tweet._data.get('reply_count', 0),
                'lang': tweet.lang, # correct
                'media_url' : tweet._data['media'][0]['media_url_https'] if 'media' in tweet._data and tweet._data['media'] else None
            }

            user_data = {
                'user_id': tweet._data['core']['user_results']['result']['rest_id'], # CORRECT
                'username': tweet._data['core']['user_results']['result']['legacy']['screen_name'], # CORRECT
                'screen_name': tweet._data['core']['user_results']['result']['legacy']['name'], # CORRECT
                'profile_image_url': tweet._data['core']['user_results']['result']['legacy']['profile_image_url_https'], # correct
                'profile_banner_url': tweet._data['core']['user_results']['result']['legacy'].get('profile_banner_url', 'None'),  # Use get() with a default
                'users_url' : (tweet._data['core']['user_results']['result']['legacy']['entities']['urls'][0]['expanded_url']
                    if 'urls' in tweet._data['core']['user_results']['result']['legacy']['entities'] and 
                    tweet._data['core']['user_results']['result']['legacy']['entities']['urls'] else None),
                'description': tweet._data['core']['user_results']['result']['legacy']['description'], # correct 
                'is_blue_verified': tweet._data['core']['user_results']['result']['is_blue_verified'], # correct
                'location': tweet._data['core']['user_results']['result']['legacy']['location'], # correct
                'followers_count': tweet._data['core']['user_results']['result']['legacy']['followers_count'], # CORRECT
                'following_count': tweet._data['core']['user_results']['result']['legacy']['friends_count'],  # correct
                'tweets_count': tweet._data['core']['user_results']['result']['legacy']['statuses_count'], # correct
                'joined': tweet._data['core']['user_results']['result']['legacy']['created_at'], # correct
            }

            hashtag_data = {
                'hashtags': [tag['text'] for tag in tweet._data['legacy']['entities']['hashtags']] if 'hashtags' in tweet._data['legacy']['entities'] else []
            }

            mention_data = {
                'mentioned_user_id': (
                    tweet._data['legacy']['entities']['user_mentions'][0].get('id', 'None')
                    if 'user_mentions' in tweet._data['legacy']['entities'] and tweet._data['legacy']['entities']['user_mentions']
                    else 'None'
                )
            }


            topic = categorize_text(tweet_data['text'])

            topic_data = {
                'topic': topic
            }
            # Convert to datetime object
            joined_datetime = datetime.strptime(user_data['joined'], '%a %b %d %H:%M:%S %z %Y')

            # Convert to offset-naive datetime if necessary
            joined_datetime_naive = joined_datetime.replace(tzinfo=None)

            # Insert user data into the database
            result = await conn.fetchrow('''
                INSERT INTO users (user_id, username, screen_name, profile_image_url, profile_banner_url, users_url, description, is_blue_verified, location, following_count, followers_count, joined)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id;
            ''', int(user_data['user_id']), user_data['username'], user_data['screen_name'], user_data['profile_image_url'], user_data['profile_banner_url'], user_data['users_url'], user_data['description'], str(user_data['is_blue_verified']), user_data['location'], user_data['followers_count'], user_data['following_count'], joined_datetime_naive)

            user_id = result['id'] if result else None
            if user_id:
                created_at = datetime.strptime(tweet_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                # Insert tweet data into the database
                result = await conn.fetchrow('''
                    INSERT INTO tweets (tweet_id, user_id, created_at, content, retweet_count, like_count, lang)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id; 
                ''', int(tweet_data['tweet_id']), user_id, created_at, tweet_data['text'], tweet_data['retweet_count'], tweet_data['like_count'], tweet_data['lang'])
                
                tweet_id = result['id'] if result else None
                if tweet_id:
                    await conn.execute('''
                        INSERT INTO mentions (tweet_id, mentioned_user_id) VALUES ($1, $2);
                    ''', tweet_id, mention_data['mentioned_user_id'])

                    await conn.execute('''
                        INSERT INTO topic (tweet_id, topic) VALUES ($1, $2);                   
                    ''', tweet_id, topic_data['topic'])

                    for hashtag in hashtag_data['hashtags']:
                        await conn.execute('''
                        INSERT INTO hashtags (hashtag, tweet_id) VALUES ($1, $2);
                    ''', hashtag, tweet_id)

                    await conn.execute('''
                        INSERT INTO media (tweet_id, media_url) VALUES ($1, $2);
                    ''', tweet_id, tweet_data['media_url'])  # Assuming 'image' as media type, update as necessary

    # Close the connection
    await conn.close()
    print(f"{datetime.now()} - Done! Got {tweet_count} tweets found")
    print("Data has been successfully added to the database.")

# Run the async main function
asyncio.run(main())
