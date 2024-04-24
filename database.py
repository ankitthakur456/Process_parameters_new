import sqlite3
import datetime
import ast
import time
import logging
import os
import sys

# determine if application is a script file or frozen exe


log = logging.getLogger("HIS_LOGS")


class DBHelper:
    def __init__(self, db_name):
        if getattr(sys, 'frozen', False):
            dirname = os.path.dirname(sys.executable)
        else:
            dirname = os.path.dirname(os.path.abspath(__file__))

        if not os.path.isdir(f"{dirname}/data/"):
            log.info("[-] data directory doesn't exists")
            try:
                os.mkdir(f"{dirname}/data/")
                log.info("[+] Created data dir successfully")
            except Exception as e:
                log.error(f"[-] Can't create data dir Error: {e}")

        self.db_name = f"{dirname}/data/{db_name}.db"
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS access_data(
        id integer PRIMARY KEY,
        access_token STRING,
        time_ DATETIME
        )""")  # Misc Data

    # region Misc DATA
    def add_access_data(self, access_token):
        try:
            time_ = datetime.datetime.now().isoformat(' ')
            self.cursor.execute("""SELECT access_token FROM access_data where id=1""")
            data = self.cursor.fetchone()
            if data:
                self.cursor.execute("""UPDATE access_data SET time_=?, access_token=?""", (time_, access_token, ))
            else:
                self.cursor.execute("""INSERT INTO access_data(id, time_, access_token)
                                    VALUES (?,?,?)""",
                                    (1, time_, access_token))
            self.connection.commit()
            log.info(f"Successful: accesstoken  data added to the database.")
        except Exception as e:
            log.error(f'Error {e} Could not add Misc data to the Database')

    def get_access_data(self):
        # self.cursor.execute('''SELECT * FROM misc''')
        # check = self.cursor.fetchone()
        # if check is None:
        #     self.add_misc_data()
        self.cursor.execute('''SELECT access_token FROM access_data WHERE id=1''')
        try:
            data = self.cursor.fetchone()
            if data is not None:
                return data[0]
        except Exception as e:
            log.error(f'ERROR: fetching misc data {e}')

        return None

    # endregion


