import argparse
import pandas as pd

import filtering_data


def read_files(args):
    """
    This method reads all the CSV files we pass as arguments
    and merge them into one bigger table
    Parameter:
        - args: list of argparse argumnts
    Return 
        - Pandas Dataframe
    """
    df = pd.read_csv(args.files[0])
    if len(args.files) > 1:
        for i in range(1, len(args.files)):
            second_df = pd.read_csv(args.files[i])
            second_df = second_df.drop(["merchant_id"], axis=1)
            if len(args.merge) > 1:
                df = df.merge(
                    second_df, left_on=args.merge[i - 1], right_on=args.merge[i]
                )
            else:
                df = df.merge(second_df, on=args.merge[i])
    return create_operational_df(df)


def create_operational_df(data_df):
    """
    This is a method to call the filter information and return the DF
    with the columns we want
    Parameter:
        - df: Pandas Dataframe
    Return 
        - Pandas Dataframe
    """

    def _filtering_services(df):
        df = filtering_data.remove_merchant(df)
        df = filtering_data.filter_description(df)
        return df

    df = _filtering_services(data_df)
    return create_final_df(df)


def create_final_df(df):
    """
    We want to create a file with some information
    from the currently DF, such as:
    - item_id
    - buyer_id
    - order_date
    - merchant_id
    - merchant_name
    - service_id
    - service_name
    - signal_type
    - description
    Parameter:
        - df: Pandas Dataframe
    Return 
        - Pandas Dataframe
    """
    df["description"] = df["description"].str.lower()
    cancel = ["cancelled", "cancel"]
    signup = ["subscription", "signup", "joining", "join", "welcome"]
    back = ["back", "welcome back", "coming back"]
    plan_update = ["changed", "update"]
    trial = ["trial"]
    df.loc[df["description"].str.contains("|".join(signup)), ["signal_type"]] = "Signup"
    df.loc[
        df["description"].str.contains("|".join(back)), ["signal_type"]
    ] = "Signup back"
    df.loc[df["description"].str.contains("|".join(trial)), ["signal_type"]] = "Trial"
    df.loc[df["description"].str.contains("|".join(cancel)), ["signal_type"]] = "Cancel"
    df.loc[
        df["description"].str.contains("|".join(plan_update)), ["signal_type"]
    ] = "Plan Update"

    df = df[
        [
            "item_id",
            "buyer_id",
            "order_date",
            "merchant_id",
            "merchant_name",
            "service_id",
            "service_name",
            "signal_type",
            "description",
        ]
    ]
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Creates DF with all information and upload to s3"
    )
    parser.add_argument(
        "-files", "--files", nargs="+", help="Name of the dataset files"
    )
    parser.add_argument(
        "-merge", "--merge", nargs="+", help="Name of the columns to merge the files on"
    )
    args, unknown_args = parser.parse_known_args()
    return read_files(args)


if __name__ == "__main__":
    main()
