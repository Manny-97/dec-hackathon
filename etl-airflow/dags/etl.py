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
            "research_output","ADMISSION_RATE","GRADUATION_RATE","EARNINGS_AFTER_10YRS","faculty_quality",
            "international_outlook", "revenue_per_student", "spending_per_student",
            "endowment"
        ],
        ascending=[True, True, False, False],
    )
    df_top_1000 = df_sorted.head(1000)
    df_top_1000["rank"] = df_top_1000.index + 1
    columns=['PRICE_CALCULATOR','IN_STATE_TUITION',
             'OUT_OF_STATE_TUITION','AVERAGE_SAT_SCORE','EARNINGS_AFTER_10YRS',
             'GRADUATION_RATE','international_outlook','revenue_per_student',
             'spending_per_student','endowment','RETENTION_RATE','FINANCIAL_AID_PERCENT','faculty_quality']
    df_top_1000[columns]=df_top_1000[columns].fillna(0)
    df_top_1000['ADMISSION_RATE'].fillna(1,inplace=True)
    ti.xcom_push(key="transformed_data", value=df_top_1000.to_json())


def load_to_snowflake(**kwargs):
    ti = kwargs["ti"]
    transformed_data_json = ti.xcom_pull(task_ids="transform_data", key="transformed_data")
    df_top_1000 = pd.read_json(transformed_data_json)
    
    conn = snowflake.connector.connect(
        user='your_username',
        password='your_password',
        account='your_account',
        warehouse='your_warehouse',
        database='your_database',
        schema='your_schema'
    )
    cur = conn.cursor()
    
    # Extract unique programs & program types
    dim_program = df_top_1000[['PROGRAM_NAME']].dropna().drop_duplicates().reset_index(drop=True)
    dim_program['PROGRAM_ID'] = range(1, len(dim_program) + 1)
    
    dim_program_type = df_top_1000[['PROGRAM_TYPE']].dropna().drop_duplicates().reset_index(drop=True)
    dim_program_type['PROGRAM_TYPE_ID'] = range(1, len(dim_program_type) + 1)
    
    # Create dimension tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_program (
            program_id INT AUTOINCREMENT PRIMARY KEY,
            program_name VARCHAR(255) UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_program_type (
            program_type_id INT AUTOINCREMENT PRIMARY KEY,
            program_type VARCHAR(255) UNIQUE
        )
    """)
    print("dimension tables created")

    # Load DataFrame into a Snowflake temporary table and upsert into main table 
    def upsert_dimension_table(table_name, df, key_column, value_column):
        temp_table = f"staging_{table_name}"
        
        # Create Staging Table
        cur.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0")

        # Load Data into Staging Table
        for _, row in df.iterrows():
            cur.execute(f"""
                INSERT INTO {temp_table} ({value_column})
                SELECT '{row[value_column]}' WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} WHERE {value_column} = '{row[value_column]}'
                )
            """)

        # Merge Data from Staging to Main Table
        cur.execute(f"""
            MERGE INTO {table_name} AS target
            USING {temp_table} AS source
            ON target.{value_column} = source.{value_column}
            WHEN NOT MATCHED THEN INSERT ({value_column}) VALUES (source.{value_column});
        """)

    # Perform Upserts
    upsert_dimension_table('dim_program', dim_program, 'program_id', 'PROGRAM_NAME')
    upsert_dimension_table('dim_program_type', dim_program_type, 'program_type_id', 'PROGRAM_TYPE')
    print("Dimension tables upsert successful!")

    # MAP PROGRAM & PROGRAM TYPE IDs BACK TO OUR DF
    df_top_1000 = df_top_1000.merge(dim_program, on='PROGRAM_NAME', how='left')
    df_top_1000 = df_top_1000.merge(dim_program_type, on='PROGRAM_TYPE', how='left')

    # UNIVERSITY FACT TABLE 
    #Create staging table 
    cur.execute("CREATE OR REPLACE TEMP TABLE staging_university AS SELECT * FROM university WHERE 1=0")

    df_top_1000 = df_top_1000.reset_index(drop=True)

    columns=['SCHOOL_ID','NAME','CITY','STATE','SCHOOL_URL','ADMISSION_RATE','PRICE_CALCULATOR','FINANCIAL_AID_PERCENT',
            'IN_STATE_TUITION','OUT_OF_STATE_TUITION','RETENTION_RATE','GRADUATION_RATE','AVERAGE_SAT_SCORE',
            'EARNINGS_AFTER_10YRS','SCHOOL_TYPE','PROGRAM_ID','PROGRAM_TYPE_ID'
    ]

    df_university=df_top_1000[columns]

    success, row_count, total_rows, load_details = write_pandas(conn, df_university, "STAGING_UNIVERSITY")
    if success:
        print("Data successfully loaded into staging_university!")

    update_program_id_query = """
    MERGE INTO staging_university su
    USING dim_program p
    ON su.program_id = p.program_id
    WHEN MATCHED THEN
        UPDATE SET su.program_id = p.program_id;
    """
    cur.execute(update_program_id_query)
    conn.commit()
    print("Updated program_id successfully in staging_university.")

    update_program_type_id_query = """
    MERGE INTO staging_university su
    USING dim_program_type pt
    ON su.program_type_id = pt.program_type_id
    WHEN MATCHED THEN
        UPDATE SET su.program_type_id = pt.program_type_id;
    """
    cur.execute(update_program_type_id_query)
    conn.commit()
    print("Updated program_type_id successfully in staging_university.")


    # Merge data from staging_university into university
    merge_query = """
    MERGE INTO university AS u
    USING staging_university AS s
    ON u.school_id = s.school_id  
    WHEN MATCHED THEN 
        UPDATE SET 
            u.name = s.name,
            u.city = s.city,
            u.state = s.state,
            u.school_url = s.school_url,
            u.admission_rate = s.admission_rate,  
            u.price_calculator = s.price_calculator,
            u.in_state_tuition = s.in_state_tuition,
            u.out_of_state_tuition = s.out_of_state_tuition,
            u.average_sat_score = s.average_sat_score,
            u.earnings_after_10yrs = s.earnings_after_10yrs,
            u.graduation_rate = s.graduation_rate,
            u.retention_rate = s.retention_rate,
            u.financial_aid_percent = s.financial_aid_percent,
            u.program_id = s.program_id,  
            u.program_type_id = s.program_type_id,  
            u.school_type = s.school_type
    WHEN NOT MATCHED THEN
        INSERT (
            school_id, name, city, state, school_url,
            price_calculator, admission_rate,
            in_state_tuition, out_of_state_tuition, average_sat_score,
            earnings_after_10yrs, graduation_rate,
            retention_rate, financial_aid_percent,
            program_id, program_type_id, school_type
        ) VALUES (
            s.school_id, s.name, s.city, s.state, s.school_url,
            s.price_calculator, s.admission_rate,
            s.in_state_tuition, s.out_of_state_tuition, s.average_sat_score,
            s.earnings_after_10yrs, s.graduation_rate,
            s.retention_rate, s.financial_aid_percent,
            s.program_id, s.program_type_id, s.school_type
        );
    """

    cur.execute(merge_query)
    conn.commit()  

    print("Update and merge operations completed successfully!")



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
