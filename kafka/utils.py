import requests

def fetch_data_by_category(api_key, category=None, country=None):
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "pageSize": 100,
        "page": 1,
        "sortBy": "popularity",
        "apiKey": api_key
    }

    if category:
        params['category'] = category
    if country:
        params['country'] = country

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json().get("articles", [])
    else:
        print(f"Error fetching data: {response.status_code}")
        return []