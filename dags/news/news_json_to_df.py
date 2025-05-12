import pandas as pd
from .extract_news import extract_news_data

def news_json_to_df():    
    data = extract_news_data()
    json_data = data['articles']
    news_df = pd.json_normalize(json_data, sep='_')
    return news_df
