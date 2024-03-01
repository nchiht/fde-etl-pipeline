import pandas as pd

class MySQLIOManager:

def __init__(self, config):
# config for connecting to MySQL database
    self._config = config

def extract_data(self, sql: str) -> pd.DataFrame:
# YOUR CODE HERE
# connect to MySQL database, use input sql statement to retrieve the data
# e.g. SELECT * FROM table_A -> Pandas Dataframe

pass