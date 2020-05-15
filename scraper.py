import pandas as pd
import seaborn as sns
import requests
import requests.auth
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import praw
import pickle
import time
import urllib, json
from urllib3.util import Retry
from datetime import timedelta


def save_pickle(file_name,obj):
    with open(file_name, 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return file_name + ' is saved'

def open_pickle(file_name):
    with open(file_name, 'rb') as handle:
        obj = pickle.load(handle)
    return obj

def get_credentials(path):
    
    with open(path, 'rb') as handle:
         cred = pickle.load(handle)
    
    return cred

def get_access_token_headers(credentials):
    client_auth = requests.auth.HTTPBasicAuth(credentials['client_id'], 
                                              credentials['client_secret'])
    post_data = {"grant_type": "password", "username": credentials['username'],
                 "password": credentials['password']}
    headers = {"User-Agent": credentials['user_agent']}
    response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
    token = 'bearer '+response.json()['access_token']
    return {"Authorization": token, "User-Agent": credentials['user_agent']}

    
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


def requests_retry_session(retries=3,
                           backoff_factor=0.3,
                           status_forcelist=(500, 502, 503, 504),
                           session=None,):
    session = session or requests.Session()
    retry = Retry(total=retries,
                  read=retries,
                  connect=retries,
                  backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist,)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def call_api(url, headers,payload={}, timeout=10):
    
    try:
        response = requests_retry_session().get(url, headers=headers,
                                              params=payload, timeout=timeout)
        response.encoding = 'utf-8'
        if response.status_code != 401:
            response.raise_for_status()
    except (requests.exceptions.Timeout, requests.exceptions.HTTPError):
        logger.error("Error calling api with payload %s", payload, exc_info=True)
        raise Exception('API response: {}'.format(response.status_code))
    return response

def add_data_to_dict(reddit_data_dict,time_stamp,sub,file_name,headers):
    #rate limit is one request per second (60 requests per min)
    start_time = time.time()
    subreddit = call_api(f"https://oauth.reddit.com/r/{sub}/about", headers, payload={}, timeout=10)
    reddit_data_dict['date'].append(current_timestamp)
    reddit_data_dict['title'].append(subreddit.json()['data']['title'])
    reddit_data_dict['id'].append(subreddit.json()['data']['id'])
    reddit_data_dict['subscribers'].append(subreddit.json()['data']['subscribers'])
    retrieve_comment_and_post_count(reddit_data_dict,sub)
    
    save_pickle(file_name, reddit_data_dict)
    sleep_time = 5 - (time.time() - start_time)
    if sleep_time < 0:
        sleep_time = 0.5
    time.sleep(sleep_time)

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
    
    
    auth_header = get_access_token_headers(credentials)
    
    skipped = []
    
    c = 0
    for sub in subreddit_names:
        
        c += 1
        try:
            print(sub)
            add_data_to_dict(reddit_data,current_timestamp,sub,file_name,auth_header)
            
            if c == 1000:
                c = 0
                get_access_token_headers(path)
        except Exception as err:
            print(err)
            skipped.append(sub)
            save_pickle('skipped_'+file_name, skipped)
            pass
        
        
    return reddit_data




now = datetime.datetime.now()
current_timestamp = now.strftime("%Y-%m-%d")
credentials = get_credentials('reddit_credentials.pkl')

df = open_pickle('data/game_and_subreddit_pairing_05072020.pkl')

subreddit_names = df['subreddit']

parse_recent_activity(current_timestamp, credentials, subreddit_names.unique())