import mysql.connector as connector
from mysql.connector import errorcode
import sys
import csv


class CustomConnector:
    def __init__(self, cfg):
        try:
            self.__conn = connector.connect(**cfg)
        except connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            sys.exit()

    def connection(self):
        print(self.__conn)

    def insert_paper(self, data, **kwargs):
        cursor = self.__conn.cursor()
        query = f"INSERT INTO `paper`(`title`, `authors`, `location`, `year`, `url`) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(query, data)
        self.__conn.commit()
        cursor.close()

    def insert_paper_csv(self, csv_file, **kwargs):
        cursor = self.__conn.cursor()
        query = f"INSERT INTO `{kwargs['table']}`" \
                f"(`title`, `authors`, `location`, `year`, `url`) " \
                f"VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `authors`=%s"
        with open(csv_file, mode='r') as csv_data:
            # TODO fix to update all field not only authors
            reader = csv.DictReader(csv_data, delimiter=',')
            csv_data_list = list(reader)
            for row in csv_data_list:
                row["location"] = None if row["location"] == '' else row["location"]
                row["year"] = None if row["year"] == '' else row["year"]
                row["url"] = None if row["url"] == '' else row["url"]
                vals = list(row.values())
                vals.extend(list(row.values())[1:2])
                cursor.execute(query, vals)
        self.__conn.commit()
        cursor.close()

    def select(self, **kwargs):
        cursor = self.__conn.cursor()
        if "all" in kwargs and kwargs["all"]:
            query = "SELECT * FROM " + kwargs["table"]
            cursor.execute(query)
            for (title, authors, location, year, url, added) in cursor:
                print(f"{title}, {authors}, {location}, {year}, {url}")
        cursor.close()

    def kill(self):
        self.__conn.close()
