"""
Connections and methods to interact with S3
"""
import os

import boto3
import pandas as pd


class S3BucketConnector:
    """
    Connect and interact with S3 buckets and objects
    """
    def __init__(self,
                 access_key: str,
                 secret_key: str,
                 endpoint: str,
                 bucket_name: str):
        self.endpoint = endpoint,
        self.session = boto3.Session(
            aws_access_key_id=os.environ[access_key],
            aws_secret_access_key=os.environ[secret_key]
        )

        self._s3 = self.session.resource(
            service_name='s3',
            endpoint_url=endpoint
        )
        self._bucket = self._s3.Bucket(bucket_name)

    def list_files_with_prefix(self):
        """
        List all files with common prefix in filename
        """
        pass

    def read_csv_to_df(self) -> pd.DataFrame:
        """
        Read csv file into pandas dataframe
        :return:
        """
        pass

    def write_df_to_s3(self):
        pass
