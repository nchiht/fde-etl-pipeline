import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import os
from io import StringIO, BytesIO


class MinIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self._connection = Minio(
            self._config['endpoint_url'],
            access_key=self._config['aws_access_key_id'],
            secret_key=self._config['aws_secret_access_key'],
            secure=False
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):

        data = obj.to_csv(index=False, header=True).encode('utf-8')

        # can define file's name here
        bucket_name = self._config['bucket']
        destination_path = '/'.join(context.asset_key.path) + ".csv"

        # check whether the bucket exists or not
        found = self._connection.bucket_exists(bucket_name)
        if not found:
            self._connection.make_bucket(bucket_name)
            context.log.info(f"Created bucket {bucket_name}")
        else:
            context.log.info(f"Bucket {bucket_name} already exists")

        self._connection.put_object(bucket_name, destination_path, BytesIO(data), len(data))
        context.log.critical(f"Successfully upload pd.DataFrame to {destination_path}")
        pass

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # define the object path
        file_object = '/'.join(context.asset_key.path) + ".csv"

        # get response from MinIO
        response = self._connection.get_object(self._config['bucket'], file_object)
        data = response.data.decode('utf-8')
        df = pd.read_csv(StringIO(data))

        context.log.info(df.head(5))
        return df
