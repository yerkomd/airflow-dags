from datetime import timedelta, datetime

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum  # <- nuevo

k8s_hook = KubernetesHook(conn_id='kubernetes_config')

# Esto es un cambio al codigo. 

#name_job = 'source_code/cstmr/spk_std_customer_paymentmethod_snc.py'

default_args = {
    'owner': 'DataWarehouse',
    'depends_on_past': False,
    #'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}
# [END default_args]

# [START instantiate_dag]

dag = DAG(
    'dag_demo',
    #start_date=days_ago(1),
    default_args=default_args,
    schedule='0 5 * * *',   # <- usar 'schedule' en Airflow 3
    catchup=False,          # <- evita backfill
    max_active_runs=1,      # <- aquí, no en default_args
    tags=['canonical'],
    template_searchpath='/opt/airflow/dags/repo/dags/kubernetes/'
)

# Establecer dependencias
flw_start = DummyOperator(task_id="start")
flw_end = DummyOperator(task_id="finish")

fecha_ayer = (datetime.now() - timedelta(days=1))
fecha_ayer_str = fecha_ayer.strftime("%Y%m%d")

# Lista de tablas y fechas para replicar
tablas = [        
    {"task_id":"spk_demo01","path_py": "source_code/demo/spk_demo01.py","fecha": fecha_ayer,"size": "small"}
]

flw_start
tasks = []  # Lista para almacenar las tareas de Spark

# Generar dinámicamente las tareas
for idx, tabla in enumerate(tablas):
    task_id = f"etl_table_{tabla['task_id']}"
    size_yaml = f"replica_{tabla['size']}"
    
    if size_yaml == 'small' :
        file_yaml = "spark_etl_small.yaml"
    elif size_yaml == 'replica_medium':
        file_yaml = "spark_etl_medium.yaml"
    elif size_yaml == 'replica_large':
        file_yaml = "spark_etl_large.yaml"
    else:
        file_yaml = "spark_etl_small.yaml"  # Condición por defecto

    task = SparkKubernetesOperator(
        task_id=task_id,
        namespace='space-int-de',
        application_file=file_yaml,
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
        retries=3,
        retry_delay=timedelta(minutes=1),
        params={
            "job": tabla["path_py"],
            "fecha": tabla["fecha"],
            "dag_name": task_id
        },
        dag=dag
    )

    tasks.append(task) 
flw_start >> tasks
tasks >> flw_end