from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import json
import time
from google.cloud import firestore
from airflow.decorators import dag, task

load_dotenv()

ENDPOINT = "https://api.mobygames.com/v1/"
API_STEP = 100
REQ_RATE = 0.1
API_KEY = os.environ.get("MOBY_GAMES_API_KEY")

def RateLimited(max_per_second):
    minInterval = 1.0 / float(max_per_second) # 10 seconds
    def decorate(func):
        last_time_called = [0.0]

        def wrapper(*args, **kargs):
            elapsed = time.perf_counter() - last_time_called[0]
            left_to_wait = minInterval - elapsed

            if left_to_wait > 0:
                time.sleep(left_to_wait)

            last_time_called[0] = time.perf_counter()
            return func(*args, **kargs)

        return wrapper
    return decorate

@RateLimited(REQ_RATE)  # Default is one in ten seconds
def moby_games_client(resource, params):
    auth = HTTPBasicAuth(API_KEY, '')
    response = requests.get(ENDPOINT + resource, params=params, auth=auth)
    return response.json()

def persist_games(games):
    db = firestore.Client(project="gamerfeels")
    for game in games:
        db.collection("games").document(str(game["game_id"])).set(game)

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'schedule_interval': '@daily'
}

@dag(
    default_args=default_args,
    dag_id='moby_games_taskflow_dag',
    description='Ingest moby games data to Firestore',
)
def moby_games_taskflow():

    @task
    def get_modern_platforms(*args, **kargs):
        print("get_modern_platforms called")
        modern_platforms = [
            "Windows",
            "Macintosh",
            "iPhone",
            "iPad",
            "Android",
            "Nintendo Switch",
            "PlayStation 4",
            "PlayStation 5",
            "Xbox One",
            "Xbox Series",
            "Xbox Cloud Gaming"
        ]
        key = 'platforms'
        platforms = []
        try:
            res_json = moby_games_client(key, {})
            if key not in res_json:
                    print(f'No key "{key}" in JSON')
                    print(json.dumps(res_json, indent=2))
            
            for platform in res_json[key]:
                if platform["platform_name"] in modern_platforms:
                    platforms.append(platform)
            return platforms
        except Exception as e:
                print(e)

    @task
    def get_games(platforms, *args, **kargs):
        print("get_games called")
        key = 'games'
        total_games = 0
        for platform in platforms:
            last_no_res = 1
            page = 0
            params = {}
            params["platform"] = platform["platform_id"]
            while last_no_res > 0 and last_no_res <= API_STEP:
                try:
                    params['offset'] = str(API_STEP * page)
                    res_json = moby_games_client(key, params)
                    total_games += len(res_json)
                    page += 1
                    if key not in res_json:
                        print(f'No key "{key}" in JSON')
                        print(json.dumps(res_json, indent=2))

                    last_no_res = len(res_json[key])
                    print(f'Page {page} : {last_no_res} {key} for {platform["platform_name"]}')
                    persist_games(res_json[key])
                except Exception as e:
                    print(e)
        return f'Total Games: {total_games}'
    platforms = get_modern_platforms()
    get_games(platforms=platforms)
moby_games_taskflow()