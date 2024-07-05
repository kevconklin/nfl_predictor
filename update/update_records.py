import requests
import json

url = "https://api.sportradar.com/nfl/official/trial/v7/en/games/2024/REG/schedule.json?api_key=l8gLeLiCa65N8jxti0MCI7FOlPrRGbKM3KMCaH5I"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

data = json.loads(response.text)

with open('nfl_schedule_2024.json', 'r') as json_file:
    json.dump(data, json_file, indent=4)