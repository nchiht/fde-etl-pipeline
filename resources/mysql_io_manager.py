import pandas as pd
import mysql.connector as connector


class MySQLIOManager:

    def __init__(self, config):
        # config for connecting to MySQL database
        self._config = config

    def extract_data(self, sql: str) -> pd.DataFrame:
        try:
            cnx = connector.connect(**self._config)
            cur = cnx.cursor()
            try:
                cur.execute(sql)
                return pd.DataFrame(cur)
            finally:
                cur.close()
                cnx.close()
        except ValueError as e:
            print(f"Error: {e}")
            return pd.DataFrame()
