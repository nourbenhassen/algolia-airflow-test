from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from services.shopify_pipeline_services import (
    fetch_data_df,
    load_values_to_db,
    transform_data,
)
from services.sql_queries import sql_create_table
from dateutil import parser


config = Variable.get("shopify_pipeline_config", deserialize_json=True)
URL_PATTERN = config["url_pattern"]
TABLE_NAME = config["db_table_name"]

hook = PostgresHook()
conn = hook.get_conn()


if "start_date" in config.keys():
    start_date = parser.parse(config["start_date"]).date()

    if "end_date" in config.keys():
        end_date = parser.parse(config["end_date"]).date()

    else:
        end_date = start_date
else:
    yesterday = date.today() - timedelta(days=1)
    start_date = end_date = yesterday


def process_and_load_data(
    url_pattern: str,
    start_date: datetime.date,
    end_date: datetime.date,
    table_name: str,
):
    """This function fetches one or several files (depending on the date range),
    creates a dataframe out of the aggregated data, transforms it given specific business rules,
    and then loads the output in a sql database.

    Args:
        url_pattern (str) : url missing the date part (that is added within the function)
        start_date (datetime.date) : the first date with start_date <= end_date
        end_date (datetime.date) : the second date with end_date >= start_date
        table_name (str) : the name of the table in which the data is loaded
    """
    input_data_df = fetch_data_df(url_pattern, start_date, end_date)
    transformed_data_df = transform_data(input_data_df)
    load_values_to_db(conn, transformed_data_df, table_name)


default_args = {
    "owner": "Nour",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "shopify_pipeline",
    default_args=default_args,
    description="Shopify pipeline",
    schedule_interval="0 8 * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["shopify", "algolia", "data_team"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        sql=sql_create_table,
    )

    process_and_load_data = PythonOperator(
        task_id="process_and_load_data",
        python_callable=process_and_load_data,
        op_kwargs={
            "url_pattern": URL_PATTERN,
            "start_date": start_date,
            "end_date": end_date,
            "table_name": TABLE_NAME,
        },
    )


create_table >> process_and_load_data
