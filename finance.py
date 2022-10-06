from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import yfinance as yf
import requests
import pandas as pd



@task(retries=2, retry_delay_seconds=1, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(minutes=1))
def get_yf_data(ticker):
    "Get ticker data from yfinance."
    df = yf.download(ticker, period = "5y")
    #print(df)
    return df


@flow
def data_pipe(ticker):
    df = get_yf_data(ticker)
    return df


if __name__ == "__main__":
    ticker = "CSCO"
    data_pipe(ticker)
