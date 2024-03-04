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
                data = cur.fetchall()
                columns_name = [col[0] for col in cur.description]

                return pd.DataFrame(data, columns=columns_name)
            finally:
                cur.close()
                cnx.close()
        except ValueError as e:
            print(f"Error: {e}")
            return pd.DataFrame()
