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
    'start_date': datetime(2022, 3, 30),
    'email': ['rockwxp@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


schedule_interval = "00 01 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'demo_bigquery_dag',
    default_args=default_args,
    schedule_interval=schedule_interval
    )

# Task 1: sync ods from rds
t1 = BigQueryOperator(
        task_id='l2_ods_user',
        sql='/ETL-CODE/l2_ods_user.sql',
        params={"dt": '2022-03-29'},
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID,
        dag=dag
    )

t2 = BigQueryOperator(
        task_id='l2_ods_movie',
        sql='/ETL-CODE/l2_ods_movie.sql',
        params={"dt": '2022-03-29'},
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID,
        dag=dag
    )

t3 = BigQueryOperator(
        task_id='l2_ods_rating',
        sql='/ETL-CODE/l2_ods_rating.sql',
        params={"dt": '2022-03-29'},
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID,
        dag=dag
    )


# working table for final table
t4 = BigQueryOperator(
    task_id='l3_wrk_female_top_10_movie',
    sql='/ETL-CODE/l3_wrk_female_top_10_movie.sql',
    params={"project": BQ_PROJECT},
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
    )

#  final table
t5 = BigQueryOperator(
    task_id='l4_dm_female_top_movie',
    sql='/ETL-CODE/l4_dm_female_top_movie.sql',
    params={"project": BQ_PROJECT},
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
    )

# Task 3: Check if partition data is written successfully
t6 = BigQueryCheckOperator(
    task_id='bq_check_dm_femle_top_10_movie',
    sql='''
    SELECT
        COUNT(*) FROM `{0}.{1}.dm_femle_top_10_movie`    
    '''.format(BQ_PROJECT, "demo_dm", '{{ yesterday_ds }}'
        ),
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag)

# Setting up Dependencies

t4.set_upstream(t1)
t4.set_upstream(t2)
t4.set_upstream(t3)
t4.set_downstream(t5)
t5.set_downstream(t6)