import os
import time
from time import gmtime, strftime
from pathlib import Path
import schedule
from customconnector import CustomConnector
import configparser


def parse_db_configs():
    return {
        "host": cfg["db"]["host"],
        "user": cfg["db"]["user"],
        "password": cfg["db"]["password"],
        "database": cfg["db"]["database"],
        'raise_on_warnings': cfg["db"].getboolean("raise_on_warnings")
    }


def job_spider_profiles():
    now = strftime('%Y-%m-%d_%H-%M-%S', gmtime())
    print("Starting spider - ", now)
    os.system(f"scrapy crawl scholar_profile -o {csv_folder}{now}.csv")
    print(f"End - Output: {csv_folder}{now}.csv")


def job_send_to_db():
    entries = os.listdir(f'{csv_folder}')
    conn = CustomConnector(parse_db_configs())
    for file in entries:
        if ".csv" in file:
            print(f"querying {file} to database")
            conn.insert_paper_csv(f"{csv_folder}{file}",
                                  table=cfg["db"]["papers_table"])
            os.rename(f"{csv_folder}{file}", f"{csv_folder}used/{file}")
    conn.kill()


cfg = configparser.ConfigParser()
cfg.read('config.ini')

csv_folder = cfg["folder"]["spider_out"]
Path(csv_folder).mkdir(parents=True, exist_ok=True)
Path(csv_folder + "used/").mkdir(parents=True, exist_ok=True)

schedule.every().friday.at(cfg["schedule"]["profiles"]).do(job_spider_profiles)
schedule.every().friday.at(cfg["schedule"]["insert"]).do(job_send_to_db)

while True:
    time.sleep(1)
    schedule.run_pending()
