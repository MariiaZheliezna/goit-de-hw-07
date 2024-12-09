from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time

# Налаштування DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'hometask07',
    default_args=default_args,
    description='DAG for medals',
    schedule_interval=timedelta(days=1),
    tags=["MZh"],
)

# =================
# SQL scripts
create_table_sql = """
CREATE TABLE IF NOT EXISTS medals_mzh (
    id SERIAL PRIMARY KEY,
    medal_type VARCHAR(50),
    count INTEGER,
    created_at TIMESTAMP
);
"""
calculate_bronze_sql = """
INSERT INTO medals_mzh (medal_type, count, created_at)
VALUES ('Bronze', (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze'), NOW());
"""

calculate_silver_sql = """
INSERT INTO medals_mzh (medal_type, count, created_at)
VALUES ('Silver', (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver'), NOW());
"""

calculate_gold_sql = """
INSERT INTO medals_mzh (medal_type, count, created_at)
VALUES ('Gold', (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold'), NOW());
"""

sensor_sql="""
    SELECT COUNT(*) 
    FROM medals_mzh 
    WHERE created_at >= NOW() - INTERVAL 30 SECOND;
    """
# =================
# Python functions
def pick_medal(ti):
    medal_type = random.choice(['Bronze', 'Silver', 'Gold'])
    ti.xcom_push(key='medal_type', value=medal_type)
    return medal_type

def pick_medal_task_py(ti):
    medal_type = ti.xcom_pull(key='medal_type', task_ids='pick_medal')
    if medal_type == 'Bronze':
        return 'calc_bronze_task'
    if medal_type == 'Silver':
        return 'calc_silver_task'
    if medal_type == 'Gold':
        return 'calc_gold_task'

def next_task_delay_py():
    time.sleep(5)
# =================


create_table = MySqlOperator(
    task_id='create_table',
    sql=create_table_sql,
    mysql_conn_id='hometask07_connection',
    dag=dag,
)

choose_medal_task = PythonOperator(
    task_id='pick_medal',
    python_callable=pick_medal,
    dag=dag,
)

pick_medal_task = BranchPythonOperator(
    task_id='pick_medal_task',
    python_callable=pick_medal_task_py,
    provide_context=True,
    dag=dag,
)

calculate_bronze_task = MySqlOperator(
    task_id='calc_bronze_task',
    sql=calculate_bronze_sql,
    mysql_conn_id='hometask07_connection',
    dag=dag,
)

calculate_silver_task = MySqlOperator(
    task_id='calc_silver_task',
    sql=calculate_silver_sql,
    mysql_conn_id='hometask07_connection',
    dag=dag,
)

calculate_gold_task = MySqlOperator(
    task_id='calc_gold_task',
    sql=calculate_gold_sql,
    mysql_conn_id='hometask07_connection',
    dag=dag,
)

generate_delay = PythonOperator(
    task_id='generate_delay',
    python_callable=next_task_delay_py,
    dag=dag,
    trigger_rule='one_success',
)

check_for_correctness = SqlSensor(
    task_id='check_recent_record',
    conn_id='hometask07_connection',
    sql=sensor_sql,
    timeout=60,
    poke_interval=5,
    mode='poke',
    dag=dag,
    trigger_rule='one_success',
)

# Tasks sequences
create_table >> choose_medal_task
choose_medal_task >> pick_medal_task
pick_medal_task >> [calculate_bronze_task, calculate_silver_task, calculate_gold_task]
[calculate_bronze_task, calculate_silver_task, calculate_gold_task] >> generate_delay
generate_delay >> check_for_correctness