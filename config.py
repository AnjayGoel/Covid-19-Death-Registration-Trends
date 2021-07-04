import datetime as dt

# Date range to scrape, scrapers can be re ran with overlapping dates, no duplicates will be inserted
scrape_end_date = dt.datetime.now()
scrape_start_date = scrape_end_date - dt.timedelta(days=50)
# scrape_start_date = dt.datetime(2016, 1, 1)

# Date range for final csv
csv_data_end_date = dt.datetime.now()
csv_data_start_date = csv_data_end_date - dt.timedelta(days=50)
