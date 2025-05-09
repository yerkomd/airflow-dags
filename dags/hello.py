from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

#esto es un cambio.


default_args = {
    'owner': 'datazoom',
    'start_date': datetime(2024, 7, 26),
    'catchup': False
}

dag = DAG(
    'hello_world',
    default_args = default_args,
    schedule=timedelta(days=1)
)

t1 = BashOperator(
    task_id = 'hello_world',
    bash_command='echo "Hello World"',
    dag = dag
)

t2 = BashOperator(
    task_id = 'hello_dml',
    bash_command='echo "Hello Data Mastery Lab"',
    dag = dag
)

t1 >> t2