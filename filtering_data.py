import pandas as pd


SERVICES = ["Netflix", "Hulu", "CBS All Access", "Starz", "Showtime"]


def remove_merchant(df):
    """
    This method removed all the services that are not in the list below:
        - Netflix
        - Hulu
        - CBS All Access
        - Starz
        - Showtime
    Parameter:
        - df: Pandas DataFrame
    Return
        - DataFrame
    """
    df = df.loc[df["merchant_name"].isin(SERVICES)]
    return df


def filter_description(df):
    """
    This method will remove all the rows that does not contain service name
    in the description
    Parameter:
        - df: Pandas DataFrame
    Return
        - DataFrame
    """
    df = df.loc[df["description"].str.contains("|".join(SERVICES))]
    return df
