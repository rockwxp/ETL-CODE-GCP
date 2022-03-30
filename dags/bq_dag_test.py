import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator


# Config variables
BQ_CONN_ID = "tensai-gcp-conn"
BQ_PROJECT = "decent-destiny-329402"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2022, 3, 26),
    'email': ['rockwxp@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


schedule_interval = "00 01 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'bigquery_airflow_test',
    default_args=default_args,
    schedule_interval=schedule_interval
    )

# Task 1: Check if source partition data exist
t1 = BigQueryCheckOperator(
        task_id='bq_check_rds_reward_dt',
        sql='/ETL-CODE/bq_check_rds_reward_dt.sql',
        params={"dt": '2022-03-20'},
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID,
        dag=dag
    )

# Task 2: write date to target table partition
t2 = BigQueryOperator(
    task_id='bq_write_to_rds_oeward_dt',
    sql='/ETL-CODE/bq_write_to_rds_oeward_dt.sql',
    params={"project": BQ_PROJECT,"dataset":"solana_rds"},
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
    )

# Task 3: Check if partition data is written successfully
t3 = BigQueryCheckOperator(
    task_id='bq_check_ods_reward_dt',
    sql='''
    #standardSQL
    SELECT
        COUNT(*) AS rows_in_partition FROM `{0}.{1}.ods_reward`    
    WHERE dt = "{2}"
    '''.format(BQ_PROJECT, "solana_ods", '{{ yesterday_ds }}'
        ),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag)

# Setting up Dependencies

t2.set_upstream(t1)
t3.set_upstream(t2)
