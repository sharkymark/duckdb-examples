import requests

urls = [
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/customers.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/orders.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/suppliers.csv"
]

for url in urls:
    print(f"\nInspecting {url}:")
    response = requests.get(url)
    print(response.text[:500])