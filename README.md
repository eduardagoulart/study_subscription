# Upload file

Run `python upload_to_s3.py -files <file1> <file2> -merge <id>` to upload the final output and the data it was filtered out, to s3 bucket
example: `python upload_to_s3.py -files matching_Rules.csv test_Data.csv -merge merchant_id`

# Combine more than one file

Run `python concate_datasets.py -files matching_Rules.csv test_Data.csv -merge merchant_id` to combine the files based on a key value.
This script will import the filtering_data one, and the output is two DataFrames, one with the expected results, and another one
with the data it was removed
example: `python concate_datasets.py -files matching_Rules.csv test_Data.csv -merge merchant_id`
