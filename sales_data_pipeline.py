import pandas as pd
from pandasql import sqldf
import requests
import aiohttp
import asyncio
import sqlite3
import matplotlib.pyplot as plt
# import sys

pd.set_option('display.max_columns', None)


# Function to fetch user data from JSONPlaceholder API
def get_user_data(json_uri):
    response = requests.get(json_uri)
    if response.status_code == 200:
        user_json_df = pd.DataFrame(response.json())
        user_json_df = user_json_df[['id', 'name', 'username', 'email', 'address']]
        user_json_df['lat'] = user_json_df['address'].apply(lambda x: x['geo']['lat']).astype(float)
        user_json_df['lng'] = user_json_df['address'].apply(lambda x: x['geo']['lng']).astype(float)
        user_json_df.drop(columns=['address'], inplace=True)
        # print(user_json_df.head(1))
        return user_json_df
    else:
        print("Failed to get data from the JSONPlaceholder")
        return None


# Function to fetch weather data for a given location
def fetch_weather_data(latitude, longitude, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}&units=metric"
    weather_response = requests.get(url)
    if weather_response.status_code == 200:
        weather_data = weather_response.json()
        return weather_data
    else:
        print(f"Failed to fetch weather data for location ({latitude}, {longitude})")
        return None


# Function to fetch weather data asynchronously for a given location
async def fetch_weather_data_async(latitude, longitude, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}&units=metric"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch weather data for location ({latitude}, {longitude})")
                return None


async def process_data(merged_data, api_key):
    tasks = [fetch_weather_data_async(row['lat'], row['lng'], api_key) for index, row in merged_data.iterrows()]
    return await asyncio.gather(*tasks)


def create_required_tables(curr):
    curr.execute('''CREATE TABLE IF NOT EXISTS SalesData (
                        order_id INTEGER PRIMARY KEY,
                        customer_id INTEGER,
                        product_id INTEGER,
                        quantity INTEGER,
                        price FLOAT,
                        order_date DATE,
                        lat FLOAT,
                        lng FLOAT,
                        temp FLOAT,
                        min_temp FLOAT,
                        max_temp FLOAT,
                        weath_cond TEXT,
                        weath_desc TEXT
                    )''')
    curr.execute('''CREATE TABLE IF NOT EXISTS UserData (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        username TEXT,
                        email TEXT
                    )''')


def plot_graph(insight_query, title, x_label, y_label, x_value, y_value):
    insight_df = sqldf(insight_query)
    plt.title(title)
    plt.bar(insight_df[x_value], insight_df[y_value])
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    # Load sales data from the CSV file provided
    # sales_csv_path = "/Users/Vishwa/Downloads/test/sales_data.csv"
    sales_csv_path = "sales_data.csv"  # sys.argv[1] sales_data.csv
    API_KEY = "0a427132ead4ac0a8dbdff7ec2550222"
    uri = "https://jsonplaceholder.typicode.com/users"

    sales_data = pd.read_csv(sales_csv_path)
    # print("\nSales DF: ", sales_data.columns)

    user_df = get_user_data(uri)
    # print("\n User: ", user_df.columns)
    user_sales_merged = pd.merge(sales_data, user_df, left_on='customer_id', right_on='id', how='left')
    unique_coord = user_sales_merged[['lat', 'lng']].drop_duplicates(subset=['lat', 'lng'])
    # print("\n Sales user: ", user_sales_merged.columns)

    # Run the event loop
    weather_data_list = asyncio.run(process_data(unique_coord, API_KEY))
    weather_df = pd.DataFrame(weather_data_list)

    weather_df = weather_df[['coord', 'main', 'weather']]
    weather_df['lat'] = weather_df['coord'].apply(lambda x: x['lat'])
    weather_df['lng'] = weather_df['coord'].apply(lambda x: x['lon'])
    weather_df['temp'] = weather_df['main'].apply(lambda x: x['temp'])
    weather_df['min_temp'] = weather_df['main'].apply(lambda x: x['temp_min'])
    weather_df['max_temp'] = weather_df['main'].apply(lambda x: x['temp_max'])
    weather_df['weath_cond'] = weather_df['weather'].apply(lambda x: x[0]['main'])
    weather_df['weath_desc'] = weather_df['weather'].apply(lambda x: x[0]['description'])
    weather_df.drop(columns=['coord', 'main', 'weather'], inplace=True)
    # print("\n Weather DF: ", weather_df.columns)
    # print(weather_df[:10])
    # print(user_sales_merged.dtypes)

    merged_all_df = pd.merge(user_sales_merged, weather_df, on=['lat', 'lng'], how='left')

    # weather_list = []
    # for index, row in user_sales_merged.iterrows():
    #     weather_list.append(fetch_weather_data(row['lat'], row['lng'], API_KEY))
    # weather_df = pd.DataFrame(weather_list)
    # print(weather_df)

    # Data Manipulation and aggregations (Just flexing my SQL expertise)
    print("\nTotal sales amount per customer.")
    print(sqldf("SELECT customer_id,sum(price) as tot_sales_pr FROM sales_data group by customer_id"))

    print("\nAverage order quantity per product.")
    print(sqldf("SELECT product_id,avg(quantity) as avg_ord_quant FROM sales_data group by product_id"))

    print("\nTop selling products by top customers.")
    print(sqldf("SELECT customer_id, product_id,sum(quantity) as tot_ord_quant FROM sales_data group by customer_id, "
                "product_id order by sum(quantity) desc"))

    print("\nTop selling products ordered.")
    print(sqldf("SELECT product_id,sum(quantity) as tot_ord_quant FROM sales_data group by "
                "product_id order by sum(quantity) desc limit 10"))

    print("\nMonthly total orders and sales.")
    print(sqldf("SELECT substr(order_date,0,8),count(1) as tot_orders, sum(quantity) as tot_sales FROM sales_data"
                " group by substr(order_date,0,8) order by substr(order_date,0,8)"))

    print("\nAverage sales amount per weather condition.")
    print(sqldf("select weath_desc,avg(price) as avg_sales_amt from merged_all_df group by weath_desc"))

    try:
        with sqlite3.connect('sales_data.db') as connect:
            curr = connect.cursor()
            create_required_tables(curr)
            curr.execute("delete from SalesData")
            curr.execute("delete from UserData")
            merged_all_df[['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'order_date', 'lat', 'lng',
                           'temp', 'min_temp', 'max_temp', 'weath_cond', 'weath_desc']]. \
                drop_duplicates(subset=['order_id']).to_sql('SalesData', connect, if_exists='append', index=False)
            user_df.drop(columns=['lat', 'lng']).to_sql('UserData', connect, if_exists='append', index=False)
    except sqlite3.Error as e:
        print("An error has occurred: ", e)

    # Visualize monthly sales trends
    monthly_sales = "select substr(order_date,0,8) as month,sum(price) as tot_sale_amt from sales_data group by substr(" \
                    "order_date,0,8)"
    plot_graph(monthly_sales, 'Monthly Sales', 'Months', 'Sales', 'month', 'tot_sale_amt')
