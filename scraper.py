import pandas as pd
import seaborn as sns
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import praw
import pickle
import time
import datetime


def open_pickle(file_name):
    with open(file_name, 'rb') as handle:
        obj = pickle.load(handle)
    return obj

def get_credentials(path):
    
    with open(path, 'rb') as handle:
         cred = pickle.load(handle)
    
    return cred
    
    
def retrieve_comment_and_post_count(reddit_data_dict,subreddit):
    lookback_hours = 24

    headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        }
    stats = {'comment':'num_of_comments', 'submission':'num_of_posts'}
    url = "https://api.pushshift.io/reddit/{}/search?&limit=1000&sort=desc&subreddit={}&before="

    startTime = datetime.datetime.utcnow()
    endTime = startTime - timedelta(hours=lookback_hours)
    endEpoch = int(endTime.timestamp())

    for stat in stats:
        count = 0
        breakOut = False
        previousEpoch = int(startTime.timestamp())
        while True:
            newUrl = url.format(stat, subreddit)+str(previousEpoch)
            json = requests.get(newUrl, headers=headers)
            objects = json.json()['data']
            for obj in objects:
                previousEpoch = obj['created_utc'] - 1
                if previousEpoch < endEpoch:
                    breakOut = True
                    break
                count += 1

            if breakOut:
                reddit_data_dict[stats[stat]].append(count)
                break

def parse_recent_activity(current_timestamp, credentials, subreddit_names):
    now = datetime.datetime.now().strftime("%Y_%m_%d")
    file_name = 'subreddit_scrape_' + now + '_dict.pkl'
    
    reddit_data = {
        'date':[],
        'title':[],
        'id':[],
        'subscribers':[],
        'num_of_comments':[],
        'num_of_posts':[] 
    }
    
    reddit = praw.Reddit(client_id=credentials['client_id'],
                     client_secret=credentials['client_secret'],
                     user_agent=credentials['user_agent'],
                     username=credentials['username'],
                     password=credentials['password'])
    
    for sub in subreddit_names:
        print(sub)
        #rate limit is one request per second (60 requests per min)
        start_time = time.time()
        subreddit = reddit.subreddit(sub)
        reddit_data['date'].append(current_timestamp)
        reddit_data['title'].append(subreddit.title)
        reddit_data['id'].append(subreddit.id)
        reddit_data['subscribers'].append(subreddit.subscribers)
        retrieve_comment_and_post_count(reddit_data,subreddit)
        save_pickle(file_name, reddit_data)
        sleep_time = 2 - (time.time() - start_time)
        if sleep_time < 0:
            sleep_time = 0.25
        time.sleep(sleep_time)
        
        
    return reddit_data





now = datetime.datetime.now()
current_timestamp = now.strftime("%Y-%m-%d")
credentials = get_credentials('reddit_credentials.pkl')

df = open_pickle('data/game_and_subreddit_pairing_05072020.pkl')
subreddit_names = df['subreddit'].unique()

parse_recent_activity(current_timestamp, credentials, subreddit_names)