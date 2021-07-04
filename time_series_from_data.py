import sqlite3
from os import path

import pandas as pd


def get_df_from_sqlite(file_name, table_name, custom_qry=""):
    con = sqlite3.connect(path.join("sqlite", file_name))
    if custom_qry != "":
        df = pd.read_sql_query(custom_qry, con)
    else:
        df = pd.read_sql_query(f"SELECT * from {table_name}", con)
    con.close()
    return df


def get_time_series_df(df: pd.DataFrame, name: str):
    """Individual registrations to time-series dataframe"""
    df = df[["date_of_death", "gender"]]
    df["gender"] = df["gender"].apply(lambda x: map_gender(x))
    df = pd.get_dummies(df, columns=["gender"], prefix=name)
    df[f"{name}_total"] = 1
    df = df.groupby('date_of_death').agg('sum')
    return df


def map_gender(gender: str):
    """standardize gender column"""
    gender = gender.lower()
    if gender in ("male", "m"):
        return "male"
    elif gender in ("female", "f"):
        return "female"
    else:
        return "transgender"


def get_delhi_time_series():
    df = get_df_from_sqlite(
        "delhi.db",
        "main",
        custom_qry="select * from (select distinct lower(trim(deceased_name)) as deceased_name, age, date_of_death,gender from main);")
    # Custom query to avoid duplicates
    df['date_of_death'] = pd.to_datetime(df['date_of_death'], format="%d/%m/%Y")

    return get_time_series_df(df, "delhi_scraped")


def get_kolkata_time_series():
    df = get_df_from_sqlite("kolkata.db", "main")
    df['dateOfDeath'] = pd.to_datetime(df['dateOfDeath'], format="%d/%m/%Y")
    df = df[["dateOfDeath", "crematoriumName", "deceasedSex"]]

    col_map = {
        "crematoriumName": "crematorium_name",
        "dateOfDeath": "date_of_death",
        "deceasedSex": "gender"
    }
    df.columns = (col_map.get(k, k) for k in df.columns)
    df = df.applymap(lambda x: x.title() if type(x) == str else x)
    return get_time_series_df(df, "kolkata_scraped")


def get_chennai_time_series():
    df = get_df_from_sqlite("chennai.db", "main")
    df['dateOfDeath'] = pd.to_datetime(df['dateOfDeath'], format="%Y-%m-%d")
    df = df[["dateOfDeath", "sex"]]
    col_map = {
        "sex": "gender",
        "dateOfDeath": "date_of_death",
    }
    df.columns = (col_map.get(k, k) for k in df.columns)

    return get_time_series_df(df, "chennai_scraped")


if __name__ == "__main__":
    pass
