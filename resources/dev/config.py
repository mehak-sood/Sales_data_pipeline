import os

key = "de_project"
iv = "de_encyptyo"
salt = "Add Salt"

#AWS Access And Secret key
aws_access_key = "Enter your encrypted access key"
aws_secret_key = "Enter your encrypted secret key"
bucket_name = "de-project-mehak-1"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
db_name = "de_project"
url = f"jdbc:mysql://localhost:3306/{db_name}"
properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "/path/Project_1/file_from_s3/"
customer_data_mart_local_file = "//path/Project_1/customer_data_mart/"
sales_team_data_mart_local_file = "/path/Project_1/sales_team_data_mart/"
sales_team_data_mart_partitioned_local_file = "/path/Project_1/sales_partition_data/"
error_folder_path_local = "/path/Project_1/error_files/"
