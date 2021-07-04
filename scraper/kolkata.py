import logging

import demjson
import pandas as pd
import requests

from config import *
from scraper import Scraper

dummy_data = {
    "deceasedName": "",
    "yearOfregistration": "",
    "dateOfRegistration": "",
    "deathRegnNo": "",
    "deathDate": "",
    "dateOfDeath": "",
    "crematoriumName": "",
    "deceasedSex": "",
    "crematoriumCode": "",
    "fatherName": "",
    "regnNo": "",
    "deathSite": ""
}


def query_scraper(query_dict):
    resp = requests.post(url=query_dict["url"], data=query_dict["params"])
    try:
        data = demjson.decode(resp.text)
        if "deathRecords" in data.keys():
            for rec in data["deathRecords"]:
                rec["dateOfDeath"] = query_dict["params"]["dateOfDeath"]
            return data["deathRecords"]
        else:
            return []
    except:
        logging.error("Error parsing resp")
        return []


def query_generator():
    url = "https://www.kmcgov.in/KMCPortal/KMCDeathRegistrationAction.do?var=getVal"

    query_params = {
        "deceasedName": "",
        "dateOfDeath": "07/12/2020",
    }

    date_range = pd.date_range(scrape_start_date, scrape_end_date)[::-1]
    for date_param in date_range:
        params = query_params.copy()
        params["dateOfDeath"] = date_param.strftime("%d/%m/%Y")
        yield {"params": params, "url": url}


def init():
    scraper = Scraper(
        file_name="sqlite/kolkata.db",
        num_workers=10,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_data,
        cmt_interval=1000,
        timeout=10
    )
    th = scraper.start_scraping()
    th.join()


if __name__ == "__main__":
    init()
