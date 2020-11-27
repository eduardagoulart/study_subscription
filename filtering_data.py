import numpy as np
import pandas as pd


SERVICES = ["netflix", "hulu", "cbs all access", "starz", "showtime"]


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
    df["service_name"] = df["service_name"].str.lower()
    df = df.loc[df["service_name"].isin(SERVICES)]
    return df


def get_matching_description(df):
    df["description"] = df["description"].str.lower()
    df["text_match"] = df["text_match"].str.lower()
    df["text_exclude"] = df["text_exclude"].str.lower()

    def _get_lower(row, param):
        for i in param:
            row[i] = row[i].lower()
        return row

    def _remove_text_exclude(row):
        if row["text_exclude"] is np.NaN:
            return row
        row = _get_lower(row, param=["description", "text_exclude"])
        row["description"] = row["description"].replace(row["text_exclude"], "")
        return row

    df = df.apply(_remove_text_exclude, axis=1)

    def _match_start_string(row):
        if row["description"] is np.NaN:
            return row
        elif row["text_match"] is np.NaN:
            row = _get_lower(row, param=["description", "service_name"])
            if row["description"].startswith(row["service_name"]):
                return row
            row["description"] = np.NaN
            return row
        row = _get_lower(row, param=["description", "text_match"])
        if row["description"].startswith(row["text_match"]):
            return row
        row["description"] = np.NaN
        return row

    start = df.loc[df["matching"] == "S"]
    start = start.apply(_match_start_string, axis=1)

    def _match_anywhere_string(row):
        if row["description"] is np.NaN:
            return row
        elif row["text_match"] is np.NaN:
            row = _get_lower(row, param=["description", "service_name"])
            if row["service_name"] in row["description"]:
                return row
            row["description"] = np.NaN
            return row
        row = _get_lower(row, param=["description", "text_match"])
        if row["text_match"] in row["description"]:
            return row
        row["description"] = np.NaN
        return row

    anywhere = df.loc[df["matching"] == "A"]
    anywhere = anywhere.apply(_match_anywhere_string, axis=1)

    def _match_regex_string(row):
        row["text_match"] = row["text_match"].replace(".", "")
        row["text_match"] = row["text_match"].replace("*", "")
        row["text_match"] = row["text_match"].replace("(", "")
        row["text_match"] = row["text_match"].replace(")", "")
        row["text_match"] = row["text_match"].replace(" ", "")
        row = _get_lower(row, param=["description", "text_match"])
        if row["text_match"] is np.NaN:
            row = _get_lower(row, param=["service_name"])
            if row["service_name"] in row["description"]:
                return row
            row["description"] = np.NaN
            return row
        regex = row["text_match"].split("|")
        for i in regex:
            if i in row["description"]:
                return row

        row["description"] = np.NaN
        return row

    regex = df.loc[df["matching"] == "R"]
    regex = regex.apply(_match_regex_string, axis=1)

    df = pd.concat([anywhere, start, regex])
    df = df.dropna(subset=["description"])
    df["final_value"] = "T"
    return df


def get_remaining_values(df):
    matching_df = get_matching_description(df)
    matching_df = matching_df[["idx_value", "final_value"]]
    not_mathing = df.merge(matching_df, on="idx_value", how="left")
    not_mathing = not_mathing.loc[not_mathing["final_value"] != "T"]
    return not_mathing
