import asyncio
from twikit import Client, TooManyRequests
import time
from datetime import datetime
import csv
from configparser import ConfigParser
from random import randint

MINIMUM_TWEETS = 10
QUERY = '$ETH'

config = ConfigParser()
config.read('config.ini')
username = config["X"]['username']
password = config["X"]['password']
email = config["X"]['email']

async def main():
    client = Client(language='en-US')

    # Await login since it's an async function
    # await client.login(
    #     auth_info_1=username,
    #     auth_info_2=email,
    #     password=password
    # )
    # Save cookies after logging in
    # client.save_cookies('cookies.json')
    client.load_cookies('cookies.json')

    tweets = await client.search_tweet(QUERY, product = 'TOP')

    for tweet in tweets:
        print(vars(tweet))
        break

    


# Run the async main function
asyncio.run(main())
