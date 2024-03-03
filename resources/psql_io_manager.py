import pandas as pd
from dagster import IOManager, OutputContext, InputContext


class MinIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):

        pass

    def load_input(self, context: InputContext) -> pd.DataFrame:

        pass
