import boto3
from botocore.exceptions import ClientError

from concate_datasets import main

session = boto3.session.Session(profile_name="user1")
s3 = session.resource("s3")
bucket = s3.Bucket("test-bucket-user1")


def df_to_csv():
    df = main()
    path = "/Users/eduardagoulart/Documents/study/antena/"
    name = "output_df.parquet"
    output = path + name
    df.to_parquet(output)
    return name


def upload_file_to_s3():
    file_name = df_to_csv()
    try:
        bucket.upload_file(file_name, "parquet-files/{}".format(file_name))
    except ClientError as e:
        logging.error(e)
        return False
    return "parquet-files/" + file_name


if __name__ == "__main__":
    upload_file_to_s3()
