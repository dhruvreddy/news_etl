import requests
import json

URL="https://saurav.tech/NewsAPI/top-headlines/category/health/in.json"

def extract_news_data():
    url = URL
    response = requests.get(url)
    data = response.json()
    return data