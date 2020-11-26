import boto3
from botocore.exceptions import ClientError

from concate_datasets import main

session = boto3.session.Session(profile_name="user1")
s3 = session.resource("s3")
bucket = s3.Bucket("test-bucket-user1")


def df_to_csv():
    df = main()
    path = "/Users/eduardagoulart/Documents/study/antena/"
    name = "output_df.csv"
    output = path + name
    df.to_csv(output)
    return name


def upload_file():
    path = df_to_csv()
    try:
        bucket.upload_file(path, bucket)
    except ClientError as e:
        logging.error(e)
        return False
    return bucket + path


if __name__ == "__main__":
    upload_file()
