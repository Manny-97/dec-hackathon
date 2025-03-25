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

def read_data(page=0):
    """
    This function reads data from the collegescorecard api
    page: the current page of the api
    """
    # API parameters
    API_KEY = os.environ.get('API_KEY', '')
    params = {
        "api_key": API_KEY,
        "fields": "id,school.name,school.city,school.state,school.zip,school.ownership,school.school_url,school.price_calculator_url,school.men_only,school.women_only,latest.admissions.admission_rate.overall,latest.cost.tuition.in_state,latest.cost.tuition.out_of_state,latest.earnings.10_yrs_after_entry.median,latest.admissions.sat_scores.average.overall,latest.completion.completion_rate_4yr_150nt,latest.student.demographics.student_faculty_ratio,school.carnegie_basic,latest.student.demographics.race_ethnicity.non_resident_alien,latest.school.instructional_expenditure_per_fte,latest.school.tuition_revenue_per_fte,latest.school.endowment.end,latest.student.retention_rate.four_year.full_time_pooled,latest.aid.pell_grant_rate",
        "per_page": 100,  
        "page": page
    }
    # Endpoint
    BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
    data_list = []
    response = session.get(
        url=BASE_URL, params=params, timeout=30
        )
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")  # Log the error message
        return None
    try:
        if response.text.strip():

            results = response.json()['results']
            ownership_mapping = {
                1: "Public",
                2: "Private Nonprofit",
                3: "Private For-Profit"
            }
            research_mapping= {
                15: 1,
                16: 2,
                27: 3,
                18: 4,
                19: 5,
                20: 6"
            }
                  
            for result in results:
                data = {
                    'id': result['id'],
                    'name': result['school.name'],
                    'city': result['school.city'],
                    'state': result['school.state'],
                    'zip': result['school.zip'],
                    'ownership': ownership_mapping.get(result['school.ownership'], 'Unknown'),
                    'school_url': result['school.school_url'],
                    'price_calculator': result['school.price_calculator_url'],
                    'men_only': result['school.men_only'],
                    'women_only': result['school.women_only'],
                    'admission_rate': result['latest.admissions.admission_rate.overall'],
                    'in_state_tution': result['latest.cost.tuition.in_state'],
                    'out_of_state_tution': result['latest.cost.tuition.out_of_state'],
                    'average_sat_scores': result['latest.admissions.sat_scores.average.overall'],
                    'earnings_after_10_yrs_entry': result['latest.earnings.10_yrs_after_entry.median'],
                    'research_output': research_mapping.get(result['school.carnegie_basic'], 7),
                    'graduation_rate':result['latest.completion.completion_rate_4yr_150nt'],
                    'faculty_quality':result['latest.student.demographics.student_faculty_ratio'],
                    'international_outlook':result['latest.student.demographics.race_ethnicity.non_resident_alien'],
                    'revenue_per_student':result['latest.school.tuition_revenue_per_fte'],
                    'spending_per_student':result['latest.school.instructional_expenditure_per_fte'],
                    'endowment':result['latest.school.endowment.end'],
                    'full_time_retention_rate':result['latest.student.retention_rate.four_year.full_time_pooled'],
                    'financial_aid_percent':result['latest.aid.pell_grant_rate']
                    
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
    df.to_csv("../data/college.csv")
