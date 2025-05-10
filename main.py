from sp500 import fetch_sp500, save_to_db
import yfinance as yf
from db import get_connection
from datetime import date

def fetch_and_store_prices(symbols):
    conn = get_connection()
    cur = conn.cursor()

    for symbol in symbols:
        print(f"Fetching {symbol}")
        try:
            df = yf.download(symbol, start="2025-05-01", end="2025-05-10")
            for idx, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO stock_prices
                    (symbol, date, open, high, low, close, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (symbol, idx.date(), row['Open'], row['High'], row['Low'],
                     row['Close'], row['Adj Close'], int(row['Volume']))
                )
        except Exception as e:
            print(f"Failed to fetch {symbol}: {e}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    df = fetch_sp500()
    save_to_db(df)
    fetch_and_store_prices(df['Symbol'].tolist())

