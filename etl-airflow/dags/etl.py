from datetime import datetime, timedelta
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_data(**kwargs):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    API_KEY = os.environ.get("API_KEY", "")
    BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
    data_list = []
    page = 0

    while True:
        page += 1
        params = {
            "api_key": API_KEY,
            "fields": "id,school.name,school.city,school.state,school.zip,school.ownership,school.school_url,"
            "school.price_calculator_url,school.men_only,school.women_only,"
            "latest.admissions.admission_rate.overall,latest.cost.tuition.in_state,latest.cost.tuition.out_of_state,"
            "latest.earnings.10_yrs_after_entry.median,latest.admissions.sat_scores.average.overall,"
            "latest.completion.completion_rate_4yr_150nt,latest.student.demographics.student_faculty_ratio,"
            "school.carnegie_basic,latest.student.demographics.race_ethnicity.non_resident_alien,"
            "latest.school.instructional_expenditure_per_fte,latest.school.tuition_revenue_per_fte,"
            "latest.school.endowment.end,latest.student.retention_rate.four_year.full_time_pooled,"
            "latest.aid.pell_grant_rate,school.institutional_characteristics.level,school.carnegie_undergrad",
            "per_page": 100,
            "page": page,
        }
        response = session.get(url=BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])

        if not results:
            break

        ownership_mapping = {
            1: "Public",
            2: "Private Nonprofit",
            3: "Private For-Profit",
        }
        research_mapping = {15: 1, 16: 2, 27: 3,17:4, 18: 5, 19: 6, 20: 7}
        program_type_mapping ={
                           0:'Exclusively Graduate',
                           1:'higher part-time',
                           2:'mixed part/full-time',
                           3:'medium full-time',
                           4:'higher full-time',
                           5:'higher part-time',
                           6:'medium full-time, inclusive, lower transfer-in',
                           7:'medium full-time, inclusive, higher transfer-in',
                           8:'medium full-time, selective, lower transfer-in',
                           9:'medium full-time , selective, higher transfer-in',
                           10:'full-time, inclusive, lower transfer-in',
                           11:'full-time, inclusive, higher transfer-in',
                           12:'full-time, selective, lower transfer-in',
                           13:'full-time, selective, higher transfer-in',
                           14:'full-time, more selective, lower transfer-in',
                           15:'full-time, more selective, higher transfer-in'
                      }
        duration_mapping = {1: "4-year", 2: "2-year", 3: "less_than_2_years"}
        program_name_mapping={0:'Not classified',
                          1:'Associates Colleges: High Transfer-High Traditional',
                          3: 'Associates Colleges: High Transfer-High Nontraditional',
                          4: 'Associates Colleges: Mixed Transfer/Career & Technical-High Traditional',
                          5:'Associates Colleges: Mixed Transfer/Career & Technical-Mixed Traditional/Nontraditional',
                          6:'Associates Colleges: Mixed Transfer/Career & Technical-High Nontraditional',
                          7:'Associates Colleges: High Career & Technical-High Traditional',
                          8:'Associates Colleges: High Career & Technical-Mixed Traditional/Nontraditional',
                          9:'Associates Colleges: High Career & Technical-High Nontraditional',
                          10:'Health Professions',
                          11:'Technical Professions',
                          12:'Arts & Design',
                          13:'Other Fields',
                          14:'Baccalaureate/Associates Colleges: Associates Dominant',
                          15:'Doctoral Universities: Very High Research Activity',
                          16:'Doctoral Universities: High Research Activity',
                          17:'Doctoral/Professional Universities',
                          18:'Masters Colleges & Universities: Larger Programs',
                          19:'Masters Colleges & Universities: Medium Programs',
                          20:'Masters Colleges & Universities: Small Programs',
                          21:'Baccalaureate Colleges: Arts & Sciences Focus',
                          22:'Baccalaureate Colleges: Diverse Fields',
                          23:'Baccalaureate/Associates Colleges: Mixed Baccalaureate/Associates',
                          24:'Faith-Related Institutions',
                          25:'Medical Schools & Centers',
                          26:'Other Health Professions Schools',
                          27:'Research Institution',
                          28:'Engineering and Other Technology-Related Schools',
                          29:'Business & Management Schools',
                          30:'Arts, Music & Design Schools',
                          31:'Law Schools',
                          32:'Other Special Focus Institutions',
                          33:'Tribal Colleges'
                        }
        data_list =[]
        for result in results:
            data_list.append(
                {
                    'SCHOOL_ID': result.get('id'),
                    'NAME': result.get('school.name'),
                    'CITY': result.get('school.city'),
                    'STATE': result.get('school.state'),
                    'SCHOOL_TYPE': ownership_mapping.get(result.get('school.ownership'), 'Unknown'),
                    'SCHOOL_URL': result.get('school.school_url'),
                    'PRICE_CALCULATOR': result.get('school.price_calculator_url'),
                    'ADMISSION_RATE': result.get('latest.admissions.admission_rate.overall'),
                    'IN_STATE_TUITION': result.get('latest.cost.tuition.in_state'),
                    'OUT_OF_STATE_TUITION': result.get('latest.cost.tuition.out_of_state'),
                    'AVERAGE_SAT_SCORE': result.get('latest.admissions.sat_scores.average.overall'),
                    'EARNINGS_AFTER_10YRS': result.get('latest.earnings.10_yrs_after_entry.median'),
                    'GRADUATION_RATE': result.get('latest.completion.completion_rate_4yr_150nt'),
                    'faculty_quality': result.get('latest.student.demographics.student_faculty_ratio'),
                    'international_outlook': result.get('latest.student.demographics.race_ethnicity.non_resident_alien'),
                    'revenue_per_student': result.get('latest.school.tuition_revenue_per_fte'),
                    'spending_per_student': result.get('latest.school.instructional_expenditure_per_fte'),
                    'endowment': result.get('latest.school.endowment.end'),
                    'RETENTION_RATE': result.get('latest.student.retention_rate.four_year.full_time_pooled'),
                    'FINANCIAL_AID_PERCENT': result.get('latest.aid.pell_grant_rate'),
                    'PROGRAM_NAME':program_name_mapping.get(result.get('school.carnegie_basic'),'Unknown'),
                    'duration':duration_mapping.get(result.get('school.institutional_characteristics.level'),'Unknown'),
                    'PROGRAM_TYPE':program_type_mapping.get(result.get('school.carnegie_undergrad'),'Unknown'),
                    'research_output': research_mapping.get(result.get('school.carnegie_basic'), 8),
                }
            )

    df = pd.DataFrame(data_list)
    kwargs["ti"].xcom_push(key="raw_data", value=df.to_json())


def transform_data(**kwargs):
    ti = kwargs["ti"]
    raw_data_json = ti.xcom_pull(task_ids="fetch_data", key="raw_data")
    df = pd.read_json(raw_data_json)

    df_sorted = df.sort_values(
        by=[
            "research_output",
            "admission_rate",
            "graduation_rate",
            "earnings_after_10yrs",
        ],
        ascending=[True, True, False, False],
    )
    df_top_1000 = df_sorted.head(1000)
    df_top_1000["rank"] = df_top_1000.index + 1

    ti.xcom_push(key="transformed_data", value=df_top_1000.to_json())


def load_to_snowflake(**kwargs):
    ti = kwargs["ti"]
    transformed_data_json = ti.xcom_pull(
        task_ids="transform_data", key="transformed_data"
    )
    df = pd.read_json(transformed_data_json)

    conn = snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
    )

    write_pandas(conn, df, "COLLEGE_RANKINGS")
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "college_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

t1 = PythonOperator(
    task_id="fetch_data", python_callable=fetch_data, provide_context=True, dag=dag
)

t2 = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id="load_to_snowflake",
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3
