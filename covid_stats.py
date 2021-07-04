"""Data Source:  https://api.covid19india.org/"""
from os import path

import pandas as pd
import requests
import tabulate

download_urls = [
    "https://api.covid19india.org/csv/latest/districts.csv",
    "https://api.covid19india.org/csv/latest/states.csv"
]
states = ["Delhi", "West Bengal", "Tamil Nadu"]
districts = ["Kolkata", "Delhi", "Chennai"]
int_cols = ["Confirmed", "Recovered", "Deceased", "Other", "Tested"]


def download_file(url: str, base_dir="csv"):
    resp = requests.get(url)
    file_name = url.split('/')[-1]
    with open(path.join(base_dir, file_name), "wb+") as f:
        f.write(resp.content)


def get_state_data():
    sw = pd.read_csv("./csv/states.csv")
    sw["Date"] = pd.to_datetime(sw['Date'], format="%Y-%m-%d")
    sw.set_index("Date", inplace=True)
    df = pd.DataFrame()
    for state in states:
        df[f"{state.replace(' ', '_')}_state_covid_deaths"] = sw[sw["State"] == state]["Deceased"].diff()

    return df


def get_district_data():
    dw = pd.read_csv("./csv/districts.csv")
    dw["Date"] = pd.to_datetime(dw['Date'], format="%Y-%m-%d")
    dw.set_index("Date", inplace=True)
    df = pd.DataFrame()
    for district in districts:
        df[f"{district.replace(' ', '_')}_dist_covid_deaths"] = dw[dw["District"] == district]["Deceased"].diff()
    return df


def download_covid_stats():
    for url in download_urls:
        download_file(url)


def test_covid_stats():
    df_1 = get_district_data()
    df_2 = get_state_data()
    df_3 = df_1.join(df_2, how="outer")
    print(tabulate.tabulate(df_3, headers=df_3.columns))


if __name__ == "__main__":
    test_covid_stats()
