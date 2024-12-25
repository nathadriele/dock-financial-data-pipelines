import logging
import json
import requests
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.amazon.aws.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.s3.operators.s3 import S3ListOperator

REMOTE_PATH = "dock/digital_accounts_statements"
S3_PATH = "dock/digital_accounts_statements"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

def authenticate(**kwargs):
    """
    Autentica na API da Dock para obter o token de acesso,
    conforme parâmetros de ambiente.
    """
    client_id = Variable.get("DOCK_CLIENT_ID")
    secret = Variable.get("DOCK_SECRET")
    url = Variable.get("DOCK_AUTH_URL")
    
    logging.info("Iniciando processo de autenticação na Dock (Digital Banking)...")

    try:
        response = requests.post(url, auth=(client_id, secret))
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        
        if not token:
            raise ValueError("Token de acesso não encontrado na resposta da API.")
    except Exception as err:
        logging.error("Erro ao tentar autenticar: %s", err)
        raise

    logging.info("Autenticação concluída com sucesso.")
    return token


def request_digital_accounts_statements(**kwargs):
    """
    Solicita o extrato de determinada conta digital (accountId) para um período.
    Vamos supor que a Dock disponibilize ao final um arquivo pronto via SFTP.
    """
    ti = kwargs["ti"]
    token = authenticate()
    
    start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = date.today().strftime("%Y-%m-%d")

    account_id = Variable.get("DOCK_DIGITAL_ACCOUNT_ID")

    base_url = Variable.get("DOCK_DIGITAL_ACCOUNTS_URL")
    url = f"{base_url}/accounts/{account_id}/statements?startDate={start_date}&endDate={end_date}"

    headers = {
        "Authorization": token,
        "Accept": "application/json",
    }

    logging.info(
        "Requisitando extrato de conta digital %s para o período %s a %s...",
        account_id, start_date, end_date
    )

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        file_name = result.get("fileName")
        if not file_name:
            raise ValueError("Nome do arquivo (fileName) não retornado pela API.")

        ti.xcom_push(key="digital_statements_file_name", value=file_name)
        logging.info("Extrato requisitado. Arquivo esperado: %s", file_name)

    except Exception as err:
        logging.error("Erro ao requisitar extrato de conta digital: %s", err)
        raise


def check_statements_in_sftp(**kwargs):
    """
    Verifica se o arquivo de extrato está disponível no SFTP,
    conforme nome retornado na task anterior.
    """
    ti = kwargs["ti"]
    file_name = ti.xcom_pull(
        key="digital_statements_file_name",
        task_ids="request_digital_accounts_statements_task"
    )

    if not file_name:
        raise ValueError("Nome do arquivo não foi recuperado.")

    logging.info("Verificando disponibilidade do arquivo '%s' no SFTP...", file_name)

    sensor = SFTPSensor(
        task_id="wait_for_digital_account_statements_sensor",
        sftp_conn_id="dock_ssh",
        path=f"{REMOTE_PATH}/{file_name}",
        timeout=300,
        poke_interval=30,
    )

    try:
        sensor.execute(context=kwargs)
        logging.info("Arquivo '%s' disponível no SFTP.", file_name)
    except Exception as err:
        logging.error("Erro ao verificar arquivo no SFTP: %s", err)
        raise


def transfer_statements_to_s3(**kwargs):
    """
    Transfere o arquivo do SFTP para o bucket S3.
    """
    ti = kwargs["ti"]
    bucket = Variable.get("AWS_S3_BUCKET")

    file_name = ti.xcom_pull(
        key="digital_statements_file_name",
        task_ids="request_digital_accounts_statements_task"
    )

    if not file_name:
        raise ValueError("Nome do arquivo não foi recuperado.")

    logging.info(
        "Iniciando transferência do arquivo '%s' do SFTP para o S3 (%s)...",
        file_name, bucket
    )

    transfer_op = SFTPToS3Operator(
        task_id="transfer_statements_sftp_to_s3_operator",
        sftp_conn_id="dock_ssh",
        s3_bucket=bucket,
        s3_key=f"{S3_PATH}/{file_name}",
        sftp_path=f"{REMOTE_PATH}/{file_name}",
        replace=True,
        confirm=True,
        use_temp_file=False,
    )

    try:
        transfer_op.execute(context=kwargs)
        logging.info("Transferência do arquivo '%s' para S3 concluída.", file_name)
    except Exception as err:
        logging.error("Erro ao transferir o arquivo '%s' para o S3: %s", file_name, err)
        raise


def unzip_digital_account_statements(**kwargs):
    """
    Lista arquivos ZIP no S3 e invoca a função Lambda para descompactá-los.
    """
    bucket = Variable.get("AWS_S3_BUCKET")
    logging.info(
        "Listando arquivos ZIP no bucket '%s' com prefixo '%s'...",
        bucket, S3_PATH
    )

    list_op = S3ListOperator(
        task_id="list_digital_account_statements_files_operator",
        bucket=bucket,
        prefix=S3_PATH,
    )

    try:
        s3_files = list_op.execute(context=kwargs)
        zip_files = [f for f in s3_files if f.endswith(".zip")]

        if not zip_files:
            logging.info("Nenhum arquivo ZIP encontrado para descompactar.")
            return

        logging.info("Arquivos ZIP encontrados: %s", zip_files)
        logging.info("Invocando função Lambda para descompactação...")

        lambda_op = LambdaInvokeFunctionOperator(
            task_id="dock_unzip_digital_account_statements_operator",
            function_name="dock_unzip_files",
            payload=json.dumps({"bucket": bucket, "keys": zip_files}),
        )

        lambda_op.execute(context=kwargs)
        logging.info("Função Lambda de descompactação executada com sucesso.")

    except Exception as err:
        logging.error("Erro ao descompactar arquivos via Lambda: %s", err)
        raise

with DAG(
    dag_id="dock_digital_accounts_statements",
    description="DAG para extrair extratos de contas digitais (Dock) e armazenar/descompactar no S3 via SFTP",
    default_args=default_args,
    start_date=datetime(2024, 12, 15),
    schedule_interval="30 10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "dock", "digital_accounts"]
) as dag:

    request_digital_accounts_statements_task = PythonOperator(
        task_id="request_digital_accounts_statements_task",
        python_callable=request_digital_accounts_statements,
        provide_context=True,
    )

    check_statements_in_sftp_task = PythonOperator(
        task_id="check_statements_in_sftp_task",
        python_callable=check_statements_in_sftp,
        provide_context=True,
    )

    transfer_statements_to_s3_task = PythonOperator(
        task_id="transfer_statements_to_s3_task",
        python_callable=transfer_statements_to_s3,
        provide_context=True,
    )

    unzip_digital_account_statements_task = PythonOperator(
        task_id="unzip_digital_account_statements_task",
        python_callable=unzip_digital_account_statements,
        provide_context=True,
    )

    (
        request_digital_accounts_statements_task
        >> check_statements_in_sftp_task
        >> transfer_statements_to_s3_task
        >> unzip_digital_account_statements_task
    )