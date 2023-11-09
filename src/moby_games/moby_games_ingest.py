from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()



def fetch_games(API_KEY):
    try:
        response = requests.get(f'https://api.mobygames.com/v1/games?api_key={API_KEY}')
        response_json = response.json()
        print(json.dumps(response_json["games"][0], indent=2))
    except  Exception as e:
        print(e)


if __name__ == '__main__':
    API_KEY = os.environ.get("MOBY_GAMES_API_KEY")
    fetch_games(API_KEY=API_KEY)