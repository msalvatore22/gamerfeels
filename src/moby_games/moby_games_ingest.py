from dotenv import load_dotenv
import os
import requests
from requests.auth import HTTPBasicAuth
import json
import time
from google.cloud import firestore


load_dotenv()
with open('config.json', 'r') as f:
    config = json.load(f)

ENDPOINT = config['moby']['endpoint']
API_STEP = config['moby']['api_step']
REQ_RATE = config['moby']['request_rate_limit']
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

@RateLimited(REQ_RATE)
def get_modern_platforms():
    modern_platforms = config["moby"]["modern_platforms"]
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

def get_games(platforms):
    key = 'games'
    for platform in platforms:
        last_no_res = 1
        page = 0
        params = {}
        params["platform"] = platform["platform_id"]
        while last_no_res > 0 and last_no_res <= API_STEP and page < 2:
            try:
                params['offset'] = str(API_STEP * page)
                res_json = moby_games_client(key, params)
                page += 1
                if key not in res_json:
                    print(f'No key "{key}" in JSON')
                    print(json.dumps(res_json, indent=2))

                last_no_res = len(res_json[key])
                print(f'Page {page} : {last_no_res} {key} for {platform["platform_name"]}')
                persist_games(res_json[key])
            except Exception as e:
                print(e)

def persist_games(games):
    db = firestore.Client(project="gamertracker")
    for game in games:
        db.collection("games").document(str(game["game_id"])).set(game)


if __name__ == '__main__':
    platforms = get_modern_platforms()
    get_games(platforms)
    