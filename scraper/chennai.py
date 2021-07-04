import json

import pandas as pd
import requests
from lxml import html

from config import *
from scraper import Scraper

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Content-Type": "application/x-www-form-urlencoded"
}
dummy_data = {
    "name": "",
    "sex": "",
    "f_h_name": "",
    "dateOfDeath": "",
    "dod": ""
}
df_columns = ("name", "sex", "f_h_name", "dod", "print")


def query_scraper(query_dict: dict):
    resp = requests.post(url=query_dict["url"], data=query_dict["params"], headers=headers)
    try:
        html_tree = html.fromstring(resp.text)
        table = html_tree.xpath("//table[@class='tableBorder']")[0]
        df = pd.read_html(html.tostring(table), header=0)[0]
        df.columns = df_columns
        df["dateOfDeath"] = query_dict["params"]["dateOfDeath"]
        df.drop(columns=["print"], inplace=True)
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        return []


def query_generator():
    url = "https://chennaicorporation.gov.in/online-civic-services/deathCertificateNew.do?do=getBasicRecords"
    query_params = {
        "Gender": "F",
        "dateOfDeath": "2020-12-07",
        "cb_hosp": "",
        "txtCaptcha": "5199732",
        "txtCaptcha_t": "5199732",
        "captchavalue": "5199732"
    }
    date_range = pd.date_range(scrape_start_date, scrape_end_date)[::-1]
    for date_param in date_range:
        for gender in ["F", "M", "G"]:
            params = query_params.copy()
            params["dateOfDeath"] = date_param.strftime("%Y-%m-%d")
            params["Gender"] = gender
            yield {"params": params, "url": url}


def init():
    scraper = Scraper(
        file_name="sqlite/chennai.db",
        num_workers=30,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_data,
        cmt_interval=1000,
        timeout=10)
    th = scraper.start_scraping()
    th.join()


if __name__ == "__main__":
    init()
