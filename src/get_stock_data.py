import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = '056fb4df470d42eeba0cb1642d1d7c6f'
BASE_URL = "https://api.twelvedata.com/time_series"
TICKERS = ["XLE", "XLF", "XLK", "XLI", "XLV", "SPY"]

def fetch_daily_prices(symbol: str) -> pd.DataFrame:
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 5000,
        "apikey": API_KEY,
        "format": "JSON"
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "values" not in data:
        raise ValueError(f"Unexpected API response for {symbol}: {data}")

    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").rename(columns={"datetime": "date"})

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ticker"] = symbol
    return df[["date", "ticker", "open", "high", "low", "close", "volume"]]

def main():
    all_data = []

    for ticker in TICKERS:
        print(f"Downloading {ticker}...")
        df = fetch_daily_prices(ticker)
        all_data.append(df)
        time.sleep(1)

    final_df = pd.concat(all_data, ignore_index=True)
    os.makedirs("data/raw/stocks", exist_ok=True)
    final_df.to_csv("data/raw/sector_etfs_daily.csv", index=False)
    print("Saved to data/raw/sector_etfs_daily.csv")

if __name__ == "__main__":
    main()