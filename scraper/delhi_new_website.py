import json
import logging
from multiprocessing import Value

import pandas as pd
import requests

from config import *
from scraper import Scraper

dummy_data = {
    "deathRegistrationGuid": "", "informant_type": "",
    "addupdaterequesttype": "", "placeOfDeath": "",
    "request_submission_type_name_en": "", "dateOfDeath": "",
    "deathplaceaddresscolonyname": "", "nameOfDeceased": "", "gender": "",
    "deceasedAgeInYears": "", "fathername": "", "mothername": "",
    "informantmobileno": "", "death_certificate_number": "",
    "deathPlaceNameInput": "", "deathPlaceAddressWardName": "",
    "deathPlaceAddressZoneName": "", "submittedDate": 0,
    "statusRemark": "", "processStatusCode": "", "created_uri": "",
    "death_registration_number": "", "citizen_mobile_no": "", "id": 0
}

dummy_delhi_common = {
    "deceased_name": "",
    "date_of_death": "",
    "age": "",
    "gender": "",
    "death_registration_no": ""
}

mapping_dict = {
    "nameOfDeceased": "deceased_name",
    "dateOfDeath": "date_of_death",
    "deceasedAgeInYears": "age",
    "gender": "gender",
    "death_registration_number": "death_registration_no"
}


def make_gender_label(gen: str):
    gen = gen.lower()
    if gen == "female":
        return "F"
    elif gen == "male":
        return "M"
    else:
        return "T"


def make_common_dict(curr_dict: dict):
    common_dict = {mapping_dict[k]: v for k, v in curr_dict.items() if k in mapping_dict.keys()}
    common_dict["gender"] = make_gender_label(common_dict["gender"])
    return common_dict


urls = [f"https://mcdonline.nic.in/intra{mcb}dmc/web/admin/rbd/applicationNumberDeathForCitizen" for mcb in
        ['e', 'n', 's']]
genders = ["F", "M", "T"]

query_params = {
    "regNo": "",
    "appNo": "",
    "fName": "",
    "dName": "",
    "dateOfEvnt": "07/04/2022",
    "gDr": "M",
    "temp": "",
    "temp1": ""
}

LEN_T = Value('i', 0)


def query_scraper(query):
    resp = requests.get(url=query["url"], params=query["params"])
    try:
        data = json.loads(resp.text)
        data = [make_common_dict(i) for i in data]
        return data
    except Exception as e:
        logging.error("Error parsing delhi new website")
        return []


def query_generator():
    date_range = pd.date_range(scrape_start_date, scrape_end_date)[::-1]
    for date_param in date_range:
        for url in urls:
            for gen in genders:
                params = query_params.copy()
                params["dateOfEvnt"] = date_param.strftime("%d/%m/%Y")
                params["gDr"] = gen
                yield {"params": params, "url": url}


def init():
    scraper = Scraper(
        file_name="sqlite/delhi.db",
        num_workers=10,
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
