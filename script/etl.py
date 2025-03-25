pip install snowflake-connector-python
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from datetime import datetime


# Setup retry strategy
session = requests.Session()
retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

# Extraction layer
def read_data(page=0):
    """
    This function reads data from the collegescorecard api
    page: the current page of the api
    """
    # API parameters
    API_KEY = os.environ.get('API_KEY', '')
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
                  "latest.aid.pell_grant_rate",
        "per_page": 100,  
        "page": page
    }
    # Endpoint
    BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
    try:
        response = session.get(url=BASE_URL, params=params, timeout=30)
        response.raise_for_status()  # Raise HTTP errors
        results = response.json().get('results', [])
        if not results:
            logging.warning(f"Warning: Empty response received on page {page}")
            return None
        ownership_mapping = {1: "Public", 2: "Private Nonprofit", 3: "Private For-Profit"}
        research_mapping = {15: 1, 16: 2, 27: 3, 18: 4, 19: 5, 20: 6} 
        data_list =[]
        for result in results:
            data = {
                'id': result.get('id'),
                'name': result.get('school.name'),
                'city': result.get('school.city'),
                'state': result.get('school.state'),
                'zip': result.get('school.zip'),
                'ownership': ownership_mapping.get(result.get('school.ownership'), 'Unknown'),
                'school_url': result.get('school.school_url'),
                'price_calculator': result.get('school.price_calculator_url'),
                'men_only': result.get('school.men_only', False),
                'women_only': result.get('school.women_only', False),
                'admission_rate': result.get('latest.admissions.admission_rate.overall'),
                'in_state_tuition': result.get('latest.cost.tuition.in_state'),
                'out_of_state_tuition': result.get('latest.cost.tuition.out_of_state'),
                'average_sat_scores': result.get('latest.admissions.sat_scores.average.overall'),
                'earnings_after_10_yrs_entry': result.get('latest.earnings.10_yrs_after_entry.median'),
                'research_output': research_mapping.get(result.get('school.carnegie_basic'), 7),
                'graduation_rate': result.get('latest.completion.completion_rate_4yr_150nt'),
                'faculty_quality': result.get('latest.student.demographics.student_faculty_ratio'),
                'international_outlook': result.get('latest.student.demographics.race_ethnicity.non_resident_alien'),
                'revenue_per_student': result.get('latest.school.tuition_revenue_per_fte'),
                'spending_per_student': result.get('latest.school.instructional_expenditure_per_fte'),
                'endowment': result.get('latest.school.endowment.end'),
                'full_time_retention_rate': result.get('latest.student.retention_rate.four_year.full_time_pooled'),
                'financial_aid_percent': result.get('latest.aid.pell_grant_rate')
            }
            data_list.append(data)
        return data_list
    except requests.exceptions.RequestException as e:
        logging.error(f"API Request Error on page {page}: {e}")
        return None

if __name__ == '__main__':
    page = 0
    data = []
    while True:
        page += 1
        result = read_data(page=page)
        if not result:
            break
        logging.info(f"Currently on page {page}")
        data.extend(result)

    df = pd.DataFrame(data)
    
    if df.empty:
        logging.error("No data was extracted! Exiting script.")
        exit(1)
        
# Transformation layer
columns=['graduation_rate','faculty_quality','international_outlook','revenue_per_student','spending_per_student','endowment','full_time_retention_rate','school_url','price_calculator','men_only','women_only','in_state_tution','out_of_state_tution','average_sat_scores','earnings_after_10_yrs_entry','financial_aid_percent']
df[columns]=df[columns].fillna(0)  #filling empty values with 0
df["admission_rate"].fillna(1, inplace=True) #filling empty admission rate records with 1 instead because of priority to lower values
df=df[df['admission_rate']!=0] # logic to filter out irrelevant records with very low admision rate

# Sorting values based on priority of criteria
df_sorted = df.sort_values(
    by=[
        "research_output","admission_rate","graduation_rate","earnings_after_10_yrs_entry","faculty_quality",
        "international_outlook", "revenue_per_student", "spending_per_student",
        "endowment"
    ],
    ascending=[True,True, False,False, True, False, False, False, False],
    na_position='last'
)

# Filter down to 1000
df_top_1000 = df_sorted.head(1000)

logging.info("Data transformation completed successfully.")

# Loading Layer

#connection details to snowflake
try:
    conn = snowflake.connector.connect(
    user='CHOICEUGWUEDE',
    password='3sUEi4bqnQyehzB',
    account='KVPAJQA-ZQ87651',
    warehouse='DEC_WH',
    database='UNIVERSITIES_DB',
    schema='PUBLIC')
    
    cur=conn.cursor()
    logging.info("Connected to Snowflake successfully.")
    
    # Create University Table with Type 3 SCD Fields
    cur.execute("""
        CREATE TABLE IF NOT EXISTS university (
            university_id INT AUTOINCREMENT PRIMARY KEY,
            school_id INT UNIQUE,
            name STRING,
            city STRING,
            school_type STRING,
            state STRING,
            prev_name STRING,
            prev_city STRING,
            prev_school_type STRING,
            prev_state STRING,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logging.info("University table checked/created.")
    
    #slice dataframe to get univeristy fields
    universities_df = df_top_1000[['id', 'name', 'city', 'ownership', 'state']].copy()
    universities_df.columns = ['SCHOOL_ID', 'NAME', 'CITY', 'SCHOOL_TYPE', 'STATE']
    
    for _, row in universities_df.iterrows():
        cur.execute("SELECT name, city, school_type, state FROM university WHERE school_id = %s", (row['SCHOOL_ID'],))
        existing_record = cur.fetchone()
        
        if existing_record:
            # Update if changed
            if (row['NAME'], row['CITY'], row['SCHOOL_TYPE'], row['STATE']) != existing_record:
                cur.execute("""
                    UPDATE university SET
                        prev_name = name,
                        prev_city = city,
                        prev_school_type = school_type,
                        prev_state = state,
                        name = %s,
                        city = %s,
                        school_type = %s,
                        state = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE school_id = %s
                """, (row['NAME'], row['CITY'], row['SCHOOL_TYPE'], row['STATE'], row['SCHOOL_ID']))
                logging.info(f"Updated record for school_id: {row['SCHOOL_ID']}")
        else:
            # Insert new record
            cur.execute("""
                INSERT INTO university (school_id, name, city, school_type, state)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['SCHOOL_ID'], row['NAME'], row['CITY'], row['SCHOOL_TYPE'], row['STATE']))
            logging.info(f"Inserted new record for school_id: {row['SCHOOL_ID']}")
    
    # Commit the changes
    conn.commit()

    logging.info("University table updated successfully.")  
    
except Exception as e:
    logging.error(f"Error during process: {str(e)}")
    



    




#insert data into snowflake table with write pandas function
success, num_chunks, num_rows, _ = write_pandas(conn, universities_df, "UNIVERSITY", schema="PUBLIC")
if success:
    print(f"Successfully inserted {num_rows} rows in {num_chunks} chunks.")
else:
    print("Insertion failed.")

# Record match Check:  SQL Query to retrieve university_id for matching school_id 
query = "SELECT university_id, school_id FROM university"
cur.execute(query)
university_mapping = {row[1]: row[0] for row in cur.fetchall()} 

# Admission Table
cur.execute("""
    CREATE TABLE IF NOT EXISTS admission (
        admission_id INT AUTOINCREMENT PRIMARY KEY,
        university_id INT,
        acceptance_rate FLOAT,
        sat_scores FLOAT,
        FOREIGN KEY (university_id) REFERENCES university(university_id)
    )
""")
print("Admission created successfully")

#slice dataframe to get admission fields
admissions_df = df_top_1000[['id', 'admission_rate', 'average_sat_scores']].copy()

# Run Check and Bulk Insert 
# Create a list of tuples for batch insertion
admissions_data = [
    (university_mapping.get(row['id']), row['admission_rate'], row['average_sat_scores'])
    for _, row in admissions_df.iterrows() if row['id'] in university_mapping
]
# Insert data in bulk to optimize performance
cur.executemany("""
    INSERT INTO admission (university_id, acceptance_rate, sat_scores)
    VALUES (%s, %s, %s)
""", admissions_data)

# Cost Table
cur.execute("""
    CREATE TABLE IF NOT EXISTS cost (
        cost_id INT AUTOINCREMENT PRIMARY KEY,
        university_id INT,
        in_state_tution FLOAT,
        out_of_state_tution FLOAT,
        financial_aid_percent FLOAT,
        FOREIGN KEY (university_id) REFERENCES university(university_id)
    )
""")
#slice dataframe to get cost fields
cost_df = df_merged[['id', 'in_state_tution', 'out_of_state_tution','financial_aid_percent']].copy()

# Run Check and Bulk Insert 
# Create a list of tuples for batch insertion
cost_data = [
    (university_mapping.get(row['id']), row['in_state_tution'], row['out_of_state_tution'],row['financial_aid_percent'])
    for _, row in cost_df.iterrows() if row['id'] in university_mapping
]
# Insert data in bulk to optimize performance
cur.executemany("""
    INSERT INTO cost (university_id, in_state_tution, out_of_state_tution,financial_aid_percent)
    VALUES (%s, %s, %s, %s)
""", cost_data)




 




