import pandas as pd
from dagster import IOManager, OutputContext, InputContext
import psycopg2
from sqlalchemy import create_engine


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        try:
            self._connection = psycopg2.connect(**self._config)
        except RuntimeError as e:
            return

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        cur = self._connection
        context.log.info(obj.head(5))
        table_name = context.name

        engine = create_engine(f'postgresql+psycopg2://{self._config["user"]}:{self._config["password"]}@{self._config["host"]}:{self._config["port"]}/{self._config["database"]}')

        obj.to_sql(table_name, engine, if_exists='replace', index=False)
        pass

    def load_input(self, context: "InputContext"):
        pass
