pip install snowflake-connector-python
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# Setup retry strategy
session = requests.Session()
retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

# EXTRACTION LAYER
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
                  "latest.aid.pell_grant_rate,school,school.institutional_characteristics.level,school.carnegie_undergrad",
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
            print(f"Warning: Empty response received on page {page}")
            return None
        ownership_mapping = {1: "Public", 2: "Private Nonprofit", 3: "Private For-Profit"}
        research_mapping = {15: 1, 16: 2, 27: 3,17:4, 18: 5, 19: 6, 20: 7}
        program_type_mapping ={0:'Exclusively Graduate',
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
        duration_mapping ={1:'4-year',
                   2:'2-year',
                   3:'less_than_2_years'
                              }
                  
        program_category_mapping={0:'Not classified',
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
                'graduation_rate': result.get('latest.completion.completion_rate_4yr_150nt'),
                'faculty_quality': result.get('latest.student.demographics.student_faculty_ratio'),
                'international_outlook': result.get('latest.student.demographics.race_ethnicity.non_resident_alien'),
                'revenue_per_student': result.get('latest.school.tuition_revenue_per_fte'),
                'spending_per_student': result.get('latest.school.instructional_expenditure_per_fte'),
                'endowment': result.get('latest.school.endowment.end'),
                'full_time_retention_rate': result.get('latest.student.retention_rate.four_year.full_time_pooled'),
                'financial_aid_percent': result.get('latest.aid.pell_grant_rate'),
                'program_category':program_category_mapping.get(result.get('school.carnegie_basic'),34),
                'duration':duration_mapping.get(result.get('school.carnegie_basic'),'Unknown'),
                'program_type':program_type_mapping.get(result.get('school.carnegie_undergrad'),'Unknown'),
                'research_output': research_mapping.get(result.get('school.carnegie_basic'), 8)
                }
                data_list.append(data)
            return data_list
        else:
            print("Warning: Empty response received")
            return None

    except requests.exceptions.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return None



if __name__ == '__main__':
    page = 0
    data = []
    while True:
        page += 1
        result = read_data(page=page)
        if not result:
            break
        print(f"Currently on page {page}")
        data.extend(result)

    df = pd.DataFrame(data)

# TRANSFORMATION LAYER
# Filter down to top 1000 based on criterias
df_sorted = df.sort_values(
    by=[
        "research_output","admission_rate","graduation_rate","earnings_after_10_yrs_entry","faculty_quality",
        "international_outlook", "revenue_per_student", "spending_per_student",
        "endowment"
    ],
    ascending=[True,True, False,False, True, False, False, False, False],
    na_position='last'
)

df_top_1000=pd.DataFrame(df_sorted.head(1000))

columns=['price_calculator','in_state_tuition',
         'out_of_state_tuition','average_sat_scores','earnings_after_10_yrs_entry',
         'graduation_rate','international_outlook','revenue_per_student',
         'spending_per_student','endowment','full_time_retention_rate','financial_aid_percent','faculty_quality']
df_top_1000[columns]=df_top_1000[columns].fillna(0)
df_top_1000['admission_rate'].fillna(1,inplace=True)

# LOADING LAYER




