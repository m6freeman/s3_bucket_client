import json
import pytest
import boto3
from botocore.client import ClientError
from moto import mock_aws
from result import Result, Ok, Err


class MyBucket:
    def __init__(self, region: str, bucket: str):
        self.region = region
        self.bucket = bucket
        self.connect()
        self.get_client()
        if not self.is_bucket_created():
            self.create_bucket()

    def connect(self) -> None:
        self.conn = boto3.resource("s3", region_name=self.region)

    def get_client(self) -> None:
        self.client = boto3.client("s3", region_name=self.region)

    def is_bucket_created(self) -> bool:
        try:
            return bool(self.conn.meta.client.head_bucket(Bucket=self.bucket))
        except ClientError:
            return False

    def create_bucket(self) -> None:
        self.client.create_bucket(Bucket=self.bucket)

    def save_object(self, key: str, body: str) -> Result[bool, str]:
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body)
            return Ok(True)
        except Exception as e:
            return Err(str(e))

    def load_object(self, key: str) -> Result[str, str]:
        return Ok(str(json.loads(self.client.get_object(
            Bucket=self.bucket,
            Key=key
        ).get('Body').read().decode("utf-8"))).replace("'", '"'))


@pytest.fixture
def get_json_file():
    with open("./fixtures/example.json") as file:
        return json.load(file)


@mock_aws
def test_my_bucket_save(get_json_file):
    input_json = get_json_file
    my_bucket: MyBucket = MyBucket("us-east-1", "MyBucket")
    assert my_bucket.save_object("home", json.dumps(input_json)).unwrap()
    body = my_bucket.conn.Object(
        "MyBucket",
        "home"
    ).get()["Body"].read().decode("utf-8")
    assert body == json.dumps(input_json)


@mock_aws
def test_my_bucket_load(get_json_file):
    input_json = get_json_file
    my_bucket: MyBucket = MyBucket("us-east-1", "MyBucket")
    my_bucket.save_object("home", json.dumps(input_json))
    assert my_bucket.load_object("home").unwrap() == json.dumps(input_json)
