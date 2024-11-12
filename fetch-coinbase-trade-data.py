import os
import requests
import polars as pl
import datetime
from save_to_delta import save_to_delta_table
# from run_langchain_analysis import run_langchain_analysis
import argparse

# Function to fetch historical data from Coinbase API
def fetch_coinbase_data(product_id, start_date, end_date, granularity=86400):
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"

    params = {
        "start": start_date,
        "end": end_date,
        "granularity": granularity,  # Granularity of the data (e.g., 86400 for 1 day)
    }

    response = requests.get(url, params=params)
    response.raise_for_status()  # Ensure we raise an exception for errors

    # The API returns data as a list of [timestamp, low, high, open, close, volume]
    data = response.json()

    # Convert to a Polars DataFrame
    # Explicitly specify row orientation
    df = pl.DataFrame(
        data,
        schema=["timestamp", "low", "high", "open", "close", "volume"],
        orient="row",
    )

    # Convert timestamp from Unix to datetime
    df = df.with_columns(
        pl.col("timestamp").cast(pl.Datetime).alias("datetime"),
        pl.col("timestamp").cast(pl.Datetime).dt.date().alias("date")
    )

    return df


# Function to process and analyze the data
def analyze_data(df):
    # Calculate moving averages (e.g., 7-day and 30-day)
    df = df.with_columns(
        [
            pl.col("close").rolling_mean(window_size=7).alias("7_day_MA"),
            pl.col("close").rolling_mean(window_size=30).alias("30_day_MA"),
        ]
    )

    # Calculate RSI (Relative Strength Index)
    # Here you can implement a custom function to calculate RSI if needed

    return df

def ingest_flow(start_date, end_date, tickers):
    try:
        print("started main process...")
        # only fetch data
        for ticker in tickers:
            print(f"Fetching data for {ticker}...")
            data = fetch_coinbase_data(ticker, start_date, end_date)
    except Exception as e:
        print("exception ingest flow")
        
    return data

def main(start_date, end_date, tickers):
    try:
            data = ingest_flow(start_date, end_date, tickers)
            
            path = "coinbase-trade-data.csv"
            data.write_csv(path, include_header=True, separator=",")
            delta_table_path = "delta-lake/coinbase-trade-data"
            # docker_delta_table_path = "app/delta_lake/delta-lake/coinbase-trade-data"
            save_to_delta_table(data, delta_table_path, mode="append")
            
            # Do LANGCHAIN analysis
            # either read delta lake
            # create cache based on the query prompt incoming
            # create a RAG and append all the cached prompt there just in case cache expires or deletes
            # run_langchain_analysis(data)
            
    except Exception as e:
        print("errored:")
        print(e)


# Fetch and analyze data
# data = fetch_and_analyze_multiple_tickers(tickers, start_date, end_date)

# Print the analysis for BTC-USD
# print(data["BTC-USD"])


def parse_arguments():
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Fetch Coinbase Trade Data with optional start and end dates")
    # Define the arguments you want to pass
    parser.add_argument("--start_date", type=str, default="2024-10-01T00:00:00Z", help="Start date (default is '2024-01-01')")
    parser.add_argument("--end_date", type=str, default="2024-11-01T00:00:00Z", help="End date (default is '2024-11-01')")
    # Parse the arguments
    args = parser.parse_args()
    
    # Return the parsed arguments
    return args

if __name__ == "__main__":
    import argparse
    args = parse_arguments()
    tickers = ["BTC-USD", "ETH-USD"]
    main(args.start_date, args.end_date,tickers)
