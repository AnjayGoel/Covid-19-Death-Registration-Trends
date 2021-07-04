import os
from functools import reduce

import pandas as pd

from time_series_from_data import *
from covid_stats import *
from config import *
from scraper import delhi_old_website, delhi_new_website, chennai, kolkata


def make_dirs():
    for directory in ("csv", "sqlite"):
        if not os.path.exists(directory):
            os.makedirs(directory)


def get_data():
    """Starts Scraping and also downloads covid stats"""
    delhi_new_website.init()
    delhi_old_website.init()
    kolkata.init()
    chennai.init()
    download_covid_stats()


def generate_combined_csv():
    dfs = [
        get_kolkata_time_series(),
        get_chennai_time_series(),
        get_delhi_time_series(),
        get_state_data(),
        get_district_data()
    ]

    df_final = reduce(lambda left, right: pd.merge(left, right, how="outer", left_index=True, right_index=True), dfs)
    df_final.index.name = "date"
    df_final = df_final[(df_final.index >= scrape_start_date) & (df_final.index < scrape_end_date)]
    df_final.columns = [i.lower() for i in df_final.columns]
    df_final.to_csv("csv/all_combined.csv")


def init():
    make_dirs()
    get_data()
    generate_combined_csv()


if __name__ == "__main__":
    init()
