import pandas as pd
import requests
from bs4 import BeautifulSoup
from db import get_connection

def fetch_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'constituents'})
    df = pd.read_html(str(table))[0]
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
    return df[['Symbol', 'Security']]

def save_to_db(df):
    conn = get_connection()
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO sp500_companies (symbol, name) VALUES (%s, %s)",
            (row['Symbol'], row['Security'])
        )
    conn.commit()
    cur.close()
    conn.close()

