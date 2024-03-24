**Comprehensive Sales Data Pipeline**  
This project aims to build a comprehensive sales data pipeline for a retail company. 
The pipeline combines generated sales data with data from external sources, performs data transformations and aggregations, 
and stores the final dataset in a database. The goal is to enable analysis and derive insights into customer behavior and sales performance.

**Setup Instructions**  
**Clone the repository**  
git clone https://github.com/your-username/sales-data-pipeline.git 

**Install dependencies**  
pip install -r requirements.txt  

**Run the Pipeline**  
python sales_data_pipeline.py  

**Components and Functionality**  
**Data Fetching:** The project fetches sales data from a CSV file and user data from the JSONPlaceholder API.  

**Data Transformation:**  
**User Data:** Extracts relevant fields and merges them with the sales data based on the customer ID.  
**Weather Data:** Fetches weather data for each sale location and associates it with the sales data.  

**Data Manipulation and Aggregations:**  
1.Calculates total sales amount per customer.  
2.Determines the average order quantity per product.  
3.Identifies top-selling products.  
4.Analyzes sales trends over time(monthly).  
5.Calculates sales amount based on weather condition.  

**Data Storage:**  
Utilizes SQLite to store the transformed and aggregated data.  

**Visualization:**  
Generates visualizations to present insights derived from the data.  

**Database Schema**  
The database schema used in this project is **Star Schema**, that include tables such as SalesData and UserData to store sales and user data, respectively.  
The "Star Schema" is a type of database schema commonly used in data warehousing environments. It is characterized by a central fact table surrounded by dimension tables.  
The star schema makes the analytical queries faster.  
  Queries against star schemas are often optimized for decision support and analytical workloads.  
  Star schemas are scalable and can handle large volumes of data efficiently.  

**Aggregations and Data Manipulation Tasks**  
Total sales amount per customer  
Average order quantity per product  
Top-selling products by top customers  
Monthly total orders and sales  
Average sales amount per weather condition  


****Bonus****  
  1.Visualization has been created on Sales trend over the months.  
  2.Docker image: docker pull blackeminence/sales_pipeline_project:latest  
  
