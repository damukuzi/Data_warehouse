from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
# [END default_args]

# [START instantiate_dag]
load_initial_data_dag = DAG(
    '1_load_initial_data',
    default_args=default_args,
    schedule_interval = None,
)

t1 = PostgresOperator(task_id='create_schema',
                      sql="CREATE SCHEMA IF NOT EXISTS dbt_raw_data;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t2 = PostgresOperator(task_id='drop_table_180_median',
                      sql="DROP TABLE IF EXISTS 180_median;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t3 = PostgresOperator(task_id='create_180_median',
                      sql="create table if not exists dbt_raw_data.180_median (ID integer,	weekday integer,hour integer,minute integer,second integer,	flow1 integer,	occupancy1 integer,	mph1 integer,	flow2 integer,	occupancy2 integer,	mph2 integer,	flow3 integer,	occupancy3 integer,	mph3 integer,	flow4 integer,	occupancy4 integer,	mph4 integer,	flow5 integer,	occupancy5 integer,	mph5 integer,	totalflow integer );",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t4 = PostgresOperator(task_id='load_180_median',
                      sql="COPY dbt_raw_data.180_median FROM '/sample_data/180_median.csv' DELIMITER ',' CSV HEADER;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t5 = PostgresOperator(task_id='drop_table_richards',
                      sql="DROP TABLE IF EXISTS richards;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t6 = PostgresOperator(task_id='create_richards',
                      sql="create table if not exists dbt_raw_data.richards (timestamp integer,	flow1 integer,	occupancy1 integer,	flow2 integer,	occupancy2 integer,	flow3 integer,	occupancy3 integer,	totalflow integer,	weekday integer, hour integer,	minute integer,	second integer);",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t7 = PostgresOperator(task_id='load_richards',
                      sql="	COPY dbt_raw_data.richards FROM '/sample_data/richards.csv' DELIMITER ',' CSV HEADER;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)                     

t8 = PostgresOperator(task_id='drop_table_station_summary',
                      sql="DROP TABLE IF EXISTS aisles;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t9 = PostgresOperator(task_id='create_station_summary',
                      sql="create table if not exists dbt_raw_data.station_summary (ID integer,	flow_99 integer,	flow_max integer,	flow_median integer,	flow_total integer,	n_obs integer);",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t10 = PostgresOperator(task_id='load_station_summary',
                      sql="	COPY dbt_raw_data.station_summary FROM '/sample_data/station_summary.csv' DELIMITER ',' CSV HEADER;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)  

t11 = PostgresOperator(task_id='drop_table_weekday',
                      sql="DROP TABLE IF EXISTS weekday;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t12 = PostgresOperator(task_id='create_weekday',
                      sql="create table if not exists dbt_raw_data.weekday ( ID integer,hour integer, minute integer,second integer,	totalflow integer);",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag)

t13 = PostgresOperator(task_id='load_weekday',
                      sql="	COPY dbt_raw_data.weekday FROM '/sample_data/weekday.csv' DELIMITER ',' CSV HEADER;",
                      postgres_conn_id='dbt_postgres_instance_raw_data',
                      autocommit=True,
                      database="dbtdb",
                      dag=load_initial_data_dag) 
       
t1 >> t2 >> t3 >> t4
t1 >> t5 >> t6 >> t7
t1 >> t8 >> t9 >> t10
t1 >> t11 >> t12 >> t13