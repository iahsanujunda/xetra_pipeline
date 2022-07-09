"""
Unit test cases for S3BucketConnector
"""
import os
import unittest

import boto3
from moto import mock_s3

from etl.extract.s3 import S3BucketConnector


class TestS3BucketConnectorCases(unittest.TestCase):
    def setUp(self) -> None:
        # Setup mock services
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Setup class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # Setup keys as environment variables
        os.environ[self.s3_access_key] = 'ACCESS_KEY'
        os.environ[self.s3_secret_key] = 'SECRET_KEY'

        # Setup bucket using mocked s3 service
        self.s3 = boto3.resource(
            service_name='s3',
            endpoint_url=self.s3_endpoint
        )
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'
            }
        )
        self.s3_bucket_obj = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint=self.s3_endpoint,
            bucket_name=self.s3_bucket_name
        )

    def tearDown(self) -> None:
        # Tear down mock s3 services
        self.mock_s3.stop()

    def test_list_files_with_prefix_return_ok(self):
        test_prefix = 'prefix/'
        test_key1 = f'{test_prefix}test1.csv'
        test_key2 = f'{test_prefix}test2.csv'

        csv_content = """col1,col2
        val1,val2"""
        self.s3_bucket_obj.put_object(Body=csv_content, Key=test_key1)
        self.s3_bucket_obj.put_object(Body=csv_content, Key=test_key2)

        returned_list = self.s3_bucket_conn.list_files_with_prefix(test_prefix)

        self.assertEqual(len(returned_list), 2)
        self.assertIn(test_key1, returned_list)
        self.assertIn(test_key2, returned_list)

    def test_list_files_with_prefix_non_existing_prefix(self):
        test_prefix = 'wrong-prefix/'

        returned_list = self.s3_bucket_conn.list_files_with_prefix(test_prefix)

        self.assertEqual(len(returned_list), 0)


if __name__ == '__main__':
    unittest.main()
