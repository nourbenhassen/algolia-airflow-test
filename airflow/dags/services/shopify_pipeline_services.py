from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import numpy as np
import psycopg2.extras as extras
import logging
from services.sql_queries import sql_insert_values
from typing import Iterator, List


def generate_date_range(
    date1: datetime.date, date2: datetime.date
) -> Iterator[datetime.date]:
    """yields date1 + 1 day

    Args:
        date1 (datetime.date): the first date with date1 <= date2
        date2 (datetime.date): the second date with date2 >= date1

    Yields:
        Iterator[datetime.date]: yields date1 + 1 day
    """
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)


def fill_date_range(date1: datetime.date, date2: datetime.date) -> List[datetime.date]:
    """Takes two dates as input and gives a list of the date range
    between these two dates.

    Args:
        date1 (datetime.date) : the first date with date1 <= date2
        date2 (datetime.date) : the second date with date2 >= date1

    Returns:
        (list[datetime.date]) : list made out of the date range"""

    date_range_list = []

    if date1 == date2:
        date_range_list.append(date1.strftime("%Y-%m-%d"))
    else:
        for date in generate_date_range(date1, date2):
            date_range_list.append(date.strftime("%Y-%m-%d"))
    return date_range_list


def fetch_data_df(
    url_pattern: str, start_date: datetime.date, end_date: datetime.date
) -> pd.DataFrame:
    """Generates a pandas dataframe out of multiple input files
    a range of dates. The files corresponding to the range between
    start_date and end_date dates will be fetched (dates are mentioned
    in the files pathname) and merged in a single dataframe.

    Args:
        url_pattern (str) : url missing the date part (that is added within the function)
        start_date (datetime.date) : the second date with start_date <= end_date
        end_date (datetime.date) : the second date with date2 >= date1


    Returns:
        (pandas.DataFrame) : pandas dataFrame made out of the aggregation of the
                             fetched files.
    """

    date_range_list = fill_date_range(start_date, end_date)
    input_dfs_list = []

    for date in date_range_list:
        url = url_pattern.format(date)
        try:
            input_df = pd.read_csv(url)
            input_dfs_list.append(input_df)
            logging.info(f"The following file was successfully loaded: {url}")
        except:
            logging.warn(f"The following file could not be loaded: {url}")
            pass

    input_full_df = pd.concat(input_dfs_list, ignore_index=True)

    return input_full_df


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    """Transformations done in the function:
            -filters rows with 'application_id' null values
            -creates 'has_specific_prefix' column
            -casts 'nbrs_pinned_items' to string

    Args:
        input_df (pandas.DataFrame) : input data to be transformed

    Returns:
        (pandas.DataFrame) : transformed output
    """
    transformed_df = input_df[
        (input_df["application_id"].notnull()) & (input_df["application_id"] != "")
    ]
    transformed_df["has_specific_prefix"] = np.where(
        transformed_df.loc[:, "index_prefix"] != "shopify_", True, False
    )
    transformed_df["nbrs_pinned_items"] = transformed_df["nbrs_pinned_items"].astype(
        "|S"
    )

    return transformed_df


def load_values_to_db(conn, input_df: pd.DataFrame, table_name: str):
    """Loads rows in a sql database table given a pandas dataFrame.

    Args:
        conn (connection) : object connection to database
        df (pandas.DataFrame) : the dataFrame to insert in the database
        table_name (str) : the name of the table in which the data is loaded
    """
    tuples = [tuple(x) for x in input_df.to_numpy()]

    cols = ",".join(list(input_df.columns))
    query = sql_insert_values.format(table_name, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    logging.info("The dataframe was successfully inserted")
    cursor.close()
