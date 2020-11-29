import argparse
import pandas as pd
import uuid

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
            if len(args.merge) > 1:
                df = df.merge(
                    second_df,
                    left_on=args.merge[i - 1],
                    right_on=args.merge[i],
                    how="outer",
                )
            else:
                df = df.merge(second_df, on=args.merge[i - 1], how="left")
    df = df.dropna(subset=["service_name", "description"])
    df["idx_value"] = [uuid.uuid4() for _ in range(len(df.index))]
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
        df = filtering_data.get_matching_description(df)
        return df

    not_final_results = filtering_data.get_remaining_values(data_df)
    not_final_results = add_signal_type_info(not_final_results)

    df = _filtering_services(data_df)
    df = add_signal_type_info(df)
    df = create_final_df(df)

    return df, not_final_results


def add_signal_type_info(df):
    """
    We want to add the signa_type information for each
    transaction
    Parameter:
        - df: Pandas Dataframe
    Return 
        - Pandas Dataframe
    """
    df = df.drop(["idx_value"], axis=1)
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
    return df


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
