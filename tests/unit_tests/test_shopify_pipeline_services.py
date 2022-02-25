from datetime import date
import pandas as pd
from pathlib import Path

from services.shopify_pipeline_services import (
    fill_date_range,
    fetch_data_df,
    transform_data,
)


PATH_TEST_DATA = Path(Path.cwd(), "tests", "data_test")
PATH_PATTERN_INPUT = str(PATH_TEST_DATA) + "/test_input_{}.csv"
PATH_PATTERN_OUTPUT = str(PATH_TEST_DATA) + "/test_output_{}.csv"


def test_fill_date_range():
    """tests fill_date_range function"""
    start_date = date(2019, 4, 1)
    end_date = date(2019, 4, 3)
    date_ranges = fill_date_range(start_date, end_date)
    assert date_ranges == ["2019-04-01", "2019-04-02", "2019-04-03"]


def test_fetch_data_df():
    """test fetch_data_df function"""
    start_date = date(2019, 4, 1)
    end_date = date(2019, 4, 2)

    data_aggregated = fetch_data_df(PATH_PATTERN_INPUT, start_date, end_date)
    data_test_output = pd.read_csv(PATH_PATTERN_OUTPUT.format("merged"))
    assert data_aggregated.equals(data_test_output)


def test_transform_data_df():
    """tests transform_data_df function"""
    start_date = date(2019, 4, 1)
    end_date = date(2019, 4, 2)

    input_df = fetch_data_df(PATH_PATTERN_INPUT, start_date, end_date)
    transformed_data = transform_data(input_df)
    data_test_output = pd.read_csv(PATH_PATTERN_OUTPUT.format("transformed"))
    data_test_output["nbrs_pinned_items"] = data_test_output[
        "nbrs_pinned_items"
    ].astype("|S")
    assert transformed_data.equals(data_test_output)
