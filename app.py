import asyncio
from datetime import datetime
from db_connection import get_db_connection
from twikit import Client
from random import randint
from configparser import ConfigParser
import traceback
import json

MINIMUM_TWEETS = 5
QUERY = 'eth'

async def main():
    # Create a database connection
    db_conn = get_db_connection()
    db_conn.autocommit = True  # Ensures automatic commit after each transaction
    cursor = db_conn.cursor()

    # Load configuration
    config = ConfigParser()
    config.read('config.ini')
    username = config["X"]['username']
    password = config["X"]['password']
    email = config["X"]['email']

    client = Client()

    await client.login(
        auth_info_1=username,
        auth_info_2=email,
        password=password
    )   

    client.get_cookies()
    client.save_cookies('cookies.json')

    with open('cookies.json', 'r', encoding='utf-8') as f:
        client.set_cookies(json.load(f))

    tweets = await client.search_tweet('$eth', 'Top')
    for tweet in tweets:
        print(tweet)
    result = await client.search_user('$eth')
    for user in result:
        print(user)

    #get user highlights tweets
    result = await client.get_user_highlights_tweets('295218901')
    for tweet in result:
        print(tweet)


# Run the main asynchronous function
if __name__ == "__main__":
    asyncio.run(main())
