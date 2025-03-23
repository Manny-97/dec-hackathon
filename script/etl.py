import os

import requests


def read_data():
    """This function reads data from the collegescorecard api"""
    #API parameters
    API_KEY = os.environ.get('api_key', '')
    params = {
        "api_key": API_KEY,
        "fields": "id,school.name,school.city,school.state,school.zip,school.ownership,school.school_url,school.price_calculator_url,school.men_only,school.women_only,latest.admissions.admission_rate.overall,latest.cost.tuition.in_state,latest.cost.tuition.out_of_state,latest.earnings.10_yrs_after_entry.median,latest.admissions.sat_scores.average.overall",
        "per_page": 100,  
        "page": 0 
    }
    #API Key and Endpoint
    BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
    data_list = []
    response = requests.get(
        url=BASE_URL, params=params
        )
    if response.status_code == 200:

        results = response.json()['results']
        ownership_mapping = {
            1: "Public",
            2: "Private Nonprofit",
            3: "Private For-Profit"
        }
        for result in results:
            print(result)
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
                'earnings_after_10_yrs_entry': result['latest.earnings.10_yrs_after_entry.median']
            }
            data_list.append(data)
    else:
        result = response.json()
        print(result)
    return data_list



if __name__ == '__main__':
    print(read_data())
