from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta, timezone
from airflow.models.param import Param
from airflow.models import Variable
import subprocess
import pendulum
import json

SCRIPT_PATH = "python3 /usr/local/airflow/dags/data_engineering/scripts/python/snowflak_query_execution.py"


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['siddhesh.ravindradongare@piramal.com'],
    'email_on_failure': True,
    'email_on_success': False,
}

def _build_command(sp_name: str) -> str:
    """
    This is called via BashOperator's bash_command as a template,
    but since we need Variable.get(), we build it differently —
    we use a wrapper BashOperator that calls a small inline script,
    OR we use Jinja templating with `var.value.<key>`.
    """
    # Jinja approach — Airflow resolves {{ var.value.xxx }} at RUNTIME, not parse time
    base = (
        "{{ var.value.host }} "
        "{{ var.value.user }} "
        "{{ var.value.password }} "
        "{{ var.value.database }} "
        "{{ var.value.password_engine }} "
        "{{ var.value.aws_id }} "
        "{{ var.value.aws_secret }} "
        "{{ var.value.role_biu_ns }} "
        "{{ var.value.warehouse_etl_hrms }} "
        "{{ var.value.bucket_biu_de_scripts }} "
        "ac_mcc_dag "
        f"'CALL UNO_DS.{sp_name}();'"
    )
    return f"{SCRIPT_PATH} {base}"


def check_amc_run_window():
    """
    Checks the current IST time. Returns True to let the DAG continue, 
    or False to quietly skip this execution.
    """
    ist_tz = pendulum.timezone('Asia/Kolkata')
    now = pendulum.now(ist_tz)
    hour = now.hour
    day = now.day
    
    print(f"Trigger received from Source DAG. Current IST time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    
    is_valid_time = (
        (5 <= hour <= 7) or  # Window 1: Catch anything up to 10:59 AM
        (11 <= hour <= 13) or # Window 2: Catch anything up to 1:59 PM
        (14 <= hour <= 16) or # Window 3: Catch anything up to 4:59 PM
        (17 <= hour <= 19)    # Window 4: Catch anything up to 9:59 PM
    )

    if not is_valid_time:
        print("Outside target time windows. Skipping this AMC run.")
        return False
    
    if day < 8 and hour >= 11:
        print("WEEK 1 MASTER RULE: No afternoon/evening runs allowed. Short-circuiting the entire DAG.")
        return False
    # =================================================================

    print(f"Trigger received from Source DAG. Current IST time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    return True

def gatekeeper_disbursement():
    # ist_tz = pendulum.timezone('Asia/Kolkata')
    # now = pendulum.now(ist_tz)
    
    # # RULE: In Week 1 (Days 1-7), ONLY allow the Morning run (before 12 PM). 
    # # Skip Afternoon (Progress) and Evening (EOD) runs.
    # if now.day < 8 and now.hour >= 11:
    #     raise AirflowSkipException("Week 1 Rule: Progress and EOD Disbursement paused until the 8th of the month.")
    # print("Disbursement Gatekeeper Passed.")
    ist_tz = pendulum.timezone('Asia/Kolkata')
    now = pendulum.now(ist_tz)
    
    if now.hour >= 11:
        raise AirflowSkipException("TEMPORARY PAUSE: Progress and EOD Disbursement paused.")
        
    print("Disbursement Gatekeeper Passed.")
    
    
def gatekeeper_conversion():
    ist_tz = pendulum.timezone('Asia/Kolkata')
    now = pendulum.now(ist_tz)
    
    # RULE: NEVER run in the first week of the month (Days 1-7)
    if now.day < 8 or now.hour >= 12:
        raise AirflowSkipException("Week 1 Rule: Conversion SP is paused until the 8th of the month.")
    print("Conversion Gatekeeper Passed.")

def gatekeeper_yield():
    
    # ist_tz = pendulum.timezone('Asia/Kolkata')
    # now = pendulum.now(ist_tz)
    
    # # RULE: Only run on Wednesday MORNINGS (before 12 PM)
    # if now.weekday() != 2 or now.hour >= 12: 
    #     raise AirflowSkipException("Day/Time Rule: Yield SP only runs on Wednesday mornings.")
    # print("Yield Gatekeeper Passed.")
    raise AirflowSkipException("TEMPORARY PAUSE: Yield SP is completely disabled for now.")

def wait_until_target_time():
    ist_tz = pendulum.timezone('Asia/Kolkata')
    now = pendulum.now(ist_tz)

    if 5 <= now.hour <= 9:
        target = now.replace(hour=8, minute=30, second=0, microsecond=0)
    elif 11 <= now.hour <= 13:
        target = now.replace(hour=12, minute=30, second=0, microsecond=0)
    elif 14 <= now.hour <= 16:
        target = now.replace(hour=16, minute=0, second=0, microsecond=0)
    elif 17 <= now.hour <= 20:
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
    else:
        return True 

    if now >= target:
        print(f"Target time {target.strftime('%H:%M')} reached! Releasing automailer.")
        return True
    else:
        print(f"Data is ready, but waiting until exact target time: {target.strftime('%H:%M')}. Current time: {now.strftime('%H:%M')}")
        return False
    
# Define the DAG
with DAG(
    'ac_mcc_dag',
    default_args=default_args,
    description='Daily refresh for Analytic Central notification tables',
    schedule=None,
    catchup=False,
    tags=['analytic_central'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting Analytic Central Pipeline"'
    )
    
    time_gatekeeper = ShortCircuitOperator(
        task_id='check_time_window',
        python_callable=check_amc_run_window
    )
    end = BashOperator(
        task_id='end',
        bash_command='echo "Analytic Central Pipeline Complete"'
    )
    check_conv = PythonOperator(
        task_id='gatekeeper_conv',
        python_callable=gatekeeper_conversion
    )       
    conversion_rate = BashOperator(
        task_id='refresh_conversion_rate',
        bash_command=_build_command('SP_CONVERSION_FUNNEL_AMC'),
    )
    check_disb = PythonOperator(
        task_id='gatekeeper_disb',
        python_callable=gatekeeper_disbursement
    )
    disb_target_v1 = BashOperator(
        task_id='refresh_disb_target',
        bash_command=_build_command('SP_DISBURSEMENT_AMC'),
    )
    
    check_yield = PythonOperator(
        task_id='gatekeeper_yield',
        python_callable=gatekeeper_yield
    )
    yield_data_refresh = BashOperator(
        task_id='refresh_yield_data',
        bash_command=_build_command('SP_YIELD_AMC'),
    )

    notification_db = BashOperator(
        task_id='refresh_notification_db',
        bash_command=_build_command('SP_NOTIFICATION_CARD_AMC'),
    )

    notification_description = BashOperator(
        task_id='refresh_notification_desc',
        bash_command=_build_command('SP_NOTIFICATION_DETAILED_AMC'),
    )
    
    analytic_central_sf_to_pg = GlueJobOperator(
        task_id="analytic_central_sf_to_pg",
        job_name='Analytic_central_sf_to_pg',              
        job_desc="triggering glue job sf_to_pg",
        region_name="ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        wait_for_completion=True,
        verbose=False,
        dag=dag,
    )
    
    mail_scheduler_sensor = PythonSensor(
        task_id='wait_for_exact_mail_time',
        python_callable = wait_until_target_time,
        mode='reschedule',  
        poke_interval=60 * 2,
        timeout=60 * 180 
    )
    
    
    analytic_central_dq_check = GlueJobOperator(
        task_id="Analytic_central_dq_check",
        job_name='Analytic_central_automailer',       
        job_desc="triggering glue job for dq checks",
        region_name="ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        wait_for_completion=True,
        verbose=False,
        dag=dag,
    )
    
# Analytic_central_llm_insights
    analytic_central_llm_insights = GlueJobOperator(
        task_id="Analytic_central_llm_insights",
        job_name='Analytic_central_llm_insights',       
        job_desc="triggering glue job for automailers to users",
        region_name="ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        wait_for_completion=True,
        verbose=False,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag,
    )
    
    push_notifications_api_1 = GlueJobOperator(
        task_id="trigger_push_notifications_1",
        job_name='Notif_Kafka_Publish_AC-Android', 
        region_name="ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        wait_for_completion=True,
        verbose=False,
        dag=dag,
    )
    
    push_notifications_api_2 = GlueJobOperator(
        task_id="trigger_push_notifications_2",
        job_name='Notif_Kafka_Publish_AC-IOS', 
        region_name="ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        wait_for_completion=True,
        verbose=False,
        dag=dag,
    )
    
    push_notifications_api_3 = GlueJobOperator(
            task_id="trigger_push_notifications_3",
            job_name='Notif_Kafka_Publish_AC-IOS-Conversion', 
            region_name="ap-south-1",
            iam_role_name="AWSGlueServiceRole",
            wait_for_completion=True,
            verbose=False,
            dag=dag,
    )
    
    push_notifications_api_4 = GlueJobOperator(
            task_id="trigger_push_notifications_4",
            job_name='Notif_Kafka_Publish_AC-Android-Conversion', 
            region_name="ap-south-1",
            iam_role_name="AWSGlueServiceRole",
            wait_for_completion=True,
            verbose=False,
            dag=dag,
    )
    
    start >> time_gatekeeper 
    
    #3 parallel branches with their gatekeepers
    time_gatekeeper >> check_conv >> conversion_rate
    time_gatekeeper >> check_disb >> disb_target_v1
    time_gatekeeper >> check_yield >> yield_data_refresh
    
    # Bring them back together
    [conversion_rate, disb_target_v1, yield_data_refresh] >> analytic_central_llm_insights 
    
    # Continue down the line
    analytic_central_llm_insights >> notification_db >> notification_description >> analytic_central_sf_to_pg >> analytic_central_dq_check >> mail_scheduler_sensor >> [push_notifications_api_1, push_notifications_api_2, push_notifications_api_3, push_notifications_api_4 ] >> end
    
    # analytic_central_automailer_uat >> [push_notifications_api_1, push_notifications_api_2] >> end
