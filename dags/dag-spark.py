from datetime import timedelta, datetime

# [START import_module]
from datetime import timedelta, datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
import pendulum
k8s_hook = KubernetesHook(conn_id='kubernetes_config')


name_job = 'taxi.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'max_active_runs': 1,
    'retries': 0
}
# [END default_args]

# [START instantiate_dag]

dag = DAG(
    'spark-s3-example-parameters',
    #start_date=days_ago(1),
    default_args=default_args,
    schedule='0 5 * * *',   # <- usar 'schedule' en Airflow 3
    catchup=False,          # <- evita backfill
    max_active_runs=1,      # <- aquí, no en default_args
    tags=['example-2'],
    template_searchpath='/opt/airflow/dags/repo/dags/kubernetes/'
)

submit = SparkKubernetesOperator(
    task_id='spark_transform_data',
    namespace='spark-operator',
    application_file='spark-deploy.yaml',
    kubernetes_conn_id='kubernetes_default',
    do_xcom_push=False,
    params={"job":name_job},
    dag=dag
)


submit