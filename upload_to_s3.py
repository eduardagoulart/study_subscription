import boto3
from botocore.exceptions import ClientError

from concate_datasets import main

session = boto3.session.Session(profile_name="user1")
s3 = session.resource("s3")
bucket = s3.Bucket("test-bucket-user1")


def df_to_csv():
    df, not_final_results = main()
    path = "/Users/eduardagoulart/Documents/study/antena/"
    name_matching = "output_df.parquet"
    name_not_final_results = "not_matching_df.parquet"
    output = path + name_matching
    df.to_parquet(output)
    not_final_results.to_parquet(path + name_not_final_results)
    return name_matching, name_not_final_results


def upload_file_to_s3():
    file_name, name_not_final_results = df_to_csv()
    try:
        bucket.upload_file(file_name, "parquet-files/{}".format(file_name))
        bucket.upload_file(file_name, "parquet-files/{}".format(name_not_final_results))
    except ClientError as e:
        logging.error(e)
        return False
    return "parquet-files/" + file_name


if __name__ == "__main__":
    upload_file_to_s3()
