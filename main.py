import asyncio
from twikit import Client, TooManyRequests
from datetime import datetime
from configparser import ConfigParser
from db_connection import get_db_connection

MINIMUM_TWEETS = 100
QUERY = '$ETH'

config = ConfigParser()
config.read('config.ini')
username = config["X"]['username']
password = config["X"]['password']
email = config["X"]['email']

async def main():
    conn = await get_db_connection()  # Use asyncpg connection
    
    client = Client(language='en-US')
    client.load_cookies('cookies.json')

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
            tweet_count += 1
            tweet_data = {
                'tweet_id': tweet._data['rest_id'],
                'user_id': tweet._data['core']['user_results']['result']['rest_id'],
                'created_at': tweet._data['core']['user_results']['result']['legacy']['created_at'],
                'text': tweet.text,
                'retweet_count': tweet._data['legacy']['retweet_count'],
                'favorite_count': tweet._data['legacy']['favorite_count'],
                'lang': tweet.lang,
                'media_url': 'None',
                'hashtags': "hashtags"
            }

            user_data = {
                'user_id': tweet._data['core']['user_results']['result']['rest_id'],
                'username': tweet._data['core']['user_results']['result']['legacy']['screen_name'],
                'screen_name': tweet._data['core']['user_results']['result']['legacy']['name'],
                'profile_image_url': tweet._data['core']['user_results']['result']['legacy']['profile_image_url_https'],
                'profile_banner_url': tweet._data['core']['user_results']['result']['legacy'].get('profile_banner_url', 'None'),  # Use get() with a default
                'users_url': 'None',
                'description': tweet._data['core']['user_results']['result']['legacy']['description'],
                'is_blue_verified': tweet._data['core']['user_results']['result']['is_blue_verified'],
                'location': tweet._data['core']['user_results']['result']['legacy']['location'],
                'followers_count': tweet._data['core']['user_results']['result']['legacy']['followers_count'],
                'joined': tweet._data['core']['user_results']['result']['legacy']['created_at'],
            }
            # Convert to datetime object
            joined_datetime = datetime.strptime(user_data['joined'], '%a %b %d %H:%M:%S %z %Y')

            # Convert to offset-naive datetime if necessary
            joined_datetime_naive = joined_datetime.replace(tzinfo=None)

            # Insert user data into the database
            await conn.execute('''
                INSERT INTO users (user_id, username, screen_name, profile_image_url, profile_banner_url, users_url, description, is_blue_verified, location, followers_count, joined)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (user_id) DO NOTHING;
            ''', int(user_data['user_id']), user_data['username'], user_data['screen_name'], user_data['profile_image_url'], user_data['profile_banner_url'], user_data['users_url'], user_data['description'], str(user_data['is_blue_verified']), user_data['location'], user_data['followers_count'], joined_datetime_naive)

            created_at = datetime.strptime(tweet_data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            # Insert tweet data into the database
            await conn.execute('''
                INSERT INTO tweets (tweet_id, user_id, created_at, content, retweet_count, like_count, lang, hashtags)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (tweet_id) DO NOTHING;
            ''', int(tweet_data['tweet_id']), int(tweet_data['user_id']), created_at, tweet_data['text'], tweet_data['retweet_count'], tweet_data['favorite_count'], tweet_data['lang'], tweet_data['media_url'])

    # Close the connection
    await conn.close()
    print(f"{datetime.now()} - Done! Got {tweet_count} tweets found")
    print("Data has been successfully added to the database.")

# Run the async main function
asyncio.run(main())
