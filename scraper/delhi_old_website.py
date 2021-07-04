import logging

import pandas as pd
import requests
import urllib3
from lxml import html

from config import *
from scraper import Scraper

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

dummy_data = {
    'death_registration_no': '',
    'date_of_death': '03/01/2015',
    'deceased_name': '',
    'relative_name': '',
    'address': '',
    'gender': ''
}

dummy_delhi_common = {
    "deceased_name": "",
    "date_of_death": "",
    "age": "",
    "gender": "",
    "death_registration_no": ""
}


def get_age_and_fixed_name(name_age_str: str):
    age_num = -1
    name_age_str = name_age_str.lower()
    name_splits = name_age_str.split()
    for name_prt in name_splits[::-1]:
        if name_prt.isdigit():
            name_age_str = name_age_str.replace(name_prt, "")
            age_num = int(name_prt)
            break

    if "years" in name_splits:
        name_age_str = name_age_str.replace("years", "")
    elif "months" in name_splits:
        age_num = age_num / 12.0
        name_age_str = name_age_str.replace("months", "")
    else:
        age_num = age_num / 365.0
        name_age_str = name_age_str.replace("days", "")
    return age_num, name_age_str.upper()


def make_common_dict(curr_dict: dict):
    common_dict = {k: v for k, v in curr_dict.items() if k in dummy_delhi_common.keys()}

    age, fixed_name = get_age_and_fixed_name(common_dict["deceased_name"])
    common_dict["age"] = age
    common_dict["deceased_name"] = fixed_name
    return common_dict


urls = [f"https://csb.mcd.gov.in/csb{mcb}dmc/rbd/onlinedeathcertificates.php" for mcb in
        ['e', 'n', 's']]
genders = ["F", "M", "O"]

query_params = {
    "searchoption_f": "2",
    "format": "MCDOLIR",
    "regyear": "",
    "DregNo": "",
    "brefNo": "",
    "dod1_f": "",
    "dcname1_f": "",
    "DFname1": "",
    "dod_f": "30 / 04 / 2017",
    "sex_f": "F",
    "dcname_f": "",
    "DFname": "",
    "DSurname": "",
    "submit": "Submit"
}

columns = ("death_registration_no", "date_of_death", "deceased_name", "relative_name", "address", "payment", "other")


def query_scraper(query_dict):
    resp = requests.post(url=query_dict["url"], data=query_dict["params"], verify=False)
    try:
        html_node_tree = html.fromstring(resp.text)
        main_table = html_node_tree.xpath("//table[@class='bodytext']")
        if len(main_table) == 2:
            df = pd.read_html(html.tostring(main_table[1]), header=0)[0]
            df.columns = columns
            df["gender"] = query_dict["params"]["sex_f"]
            df.drop(columns=["other", "payment"], inplace=True)
            return [make_common_dict(i) for i in df.to_dict(orient="records")]
        else:
            return []
    except Exception as e:
        logging.error("%s", str(query_dict), exc_info=True)
        return []


def query_generator():
    date_range = pd.date_range(scrape_start_date, scrape_end_date)[::-1]
    for date_param in date_range:
        for url in urls:
            for gen in genders:
                params = query_params.copy()
                params["dod_f"] = date_param.strftime("%d/%m/%Y")
                params["sex_f"] = gen
                yield {"params": params, "url": url}


def init():
    scraper = Scraper(
        file_name="sqlite/delhi.db",
        num_workers=30,
        query_generator=query_generator,
        query_scraper=query_scraper,
        dummy_data=dummy_delhi_common,
        cmt_interval=1000,
        timeout=10
    )
    th = scraper.start_scraping()
    th.join()


if __name__ == '__main__':
    init()
