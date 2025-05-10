from sp500 import fetch_sp500, save_to_db
import yfinance as yf
import pandas as pd
from db import get_connection
from datetime import date
import time
import random

def fetch_with_retry(symbol, start_date, end_date, max_retries=3):
    for attempt in range(max_retries):
        try:
            df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            return df
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {symbol}: {e}")
            wait = random.uniform(1, 3) * (2 ** attempt)  # Exponential backoff
            print(f"Retrying in {wait:.2f} seconds...")
            time.sleep(wait)
    print(f"All attempts failed for {symbol}, skipping.")
    return pd.DataFrame()  # Return empty so it can be skipped


def fetch_and_store_prices(symbols):
    conn = get_connection()
    cur = conn.cursor()

    for symbol in symbols:
        print(f"Fetching {symbol}")
        try:
            # Inside your loop after each download
            time.sleep(random.uniform(4, 20))
            df = yf.download(symbol, start="2025-05-01", end="2025-05-10", auto_adjust=False)
            # Flatten multi-index if needed
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            # Sleep for a short time to avoid overwhelming the API/DNS
            time.sleep(5)
            if df.empty:
                print(f"No data for {symbol}, skipping.")
                continue
            else :
                print(df.head())
                print(df.columns)

            for idx, row in df.iterrows():
                try:
                    open_price = row['Open']
                    high = row['High']
                    low = row['Low']
                    close = row['Close']
                    adj_close = row['Adj Close']
                    volume = row['Volume']

                    cur.execute(
                        """
                        INSERT INTO stock_prices
                        (symbol, date, open, high, low, close, adj_close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            adj_close = EXCLUDED.adj_close,
                            volume = EXCLUDED.volume
                        """,
                        (
                            symbol,
                            idx.date(),
                            float(open_price) if pd.notna(open_price) else None,
                            float(high) if pd.notna(high) else None,
                            float(low) if pd.notna(low) else None,
                            float(close) if pd.notna(close) else None,
                            float(adj_close) if pd.notna(adj_close) else None,
                            int(volume) if pd.notna(volume) else None
                        )
                    )

                except Exception as e:
                   print(f"Failed to insert row for {symbol} on {idx.date()}: {e}")


        except Exception as e:
            print(f"Failed to fetch {symbol}: {e}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    df = fetch_sp500()
    save_to_db(df)
    fetch_and_store_prices(df['Symbol'].tolist())

