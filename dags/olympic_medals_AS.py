from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
import random
import time
from datetime import datetime

# Конфігурація DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 4, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_aspivak"

dag = DAG(
    'olympic_medals_AS',
    default_args=default_args,
    description='DAG для роботи з медалями',
    schedule_interval=None,
    catchup=False,
    tags=["AS"]
)

# 1. Створення таблиці
create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id=connection_name, 
    sql="""
    CREATE TABLE IF NOT EXISTS olympic_medal_counts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(10),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# 2. Генерація випадкового значення
def choose_random_medal():
    return random.choice(['branch_bronze', 'branch_silver', 'branch_gold'])

choose_medal = BranchPythonOperator(
    task_id='choose_medal',
    python_callable=choose_random_medal,
    dag=dag,
)

# 3. Розгалуження
def count_medals(medal_type, **kwargs):
    hook = MySqlHook(mysql_conn_id=connection_name)  
    result = hook.get_first(f"""
        SELECT COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal_type}';
    """)
    count = result[0] if result else 0
    hook.run(f"""
        INSERT INTO olympic_medal_counts (medal_type, count)
        VALUES ('{medal_type}', {count});
    """)
    return count

branch_bronze = PythonOperator(
    task_id='branch_bronze',
    python_callable=count_medals,
    op_args=['Bronze'],
    provide_context=True,
    dag=dag,
)

branch_silver = PythonOperator(
    task_id='branch_silver',
    python_callable=count_medals,
    op_args=['Silver'],
    provide_context=True,
    dag=dag,
)

branch_gold = PythonOperator(
    task_id='branch_gold',
    python_callable=count_medals,
    op_args=['Gold'],
    provide_context=True,
    dag=dag,
)

# 4. DummyOperator для злиття гілок
merge_branches = DummyOperator(
    task_id='merge_branches',
    trigger_rule='one_success',
    dag=dag,
)

# 5. Затримка
def sleep_task():
    time.sleep(5)

delay_task = PythonOperator(
    task_id='delay_task',
    python_callable=sleep_task,
    dag=dag,
)

# 6. Перевірка часу створення запису
check_recent_entry = SqlSensor(
    task_id='check_recent_entry',
    conn_id=connection_name, 
    sql="""
    SELECT 1 FROM olympic_medal_counts
    WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
    LIMIT 1;
    """,
    mode='poke',
    poke_interval=30,   
    timeout=5,
    dag=dag,
)

# Налаштування залежностей
#create_schema >> 
create_table >> choose_medal
choose_medal >> [branch_bronze, branch_silver, branch_gold]
[branch_bronze, branch_silver, branch_gold] >> merge_branches
merge_branches >> delay_task >> check_recent_entry
