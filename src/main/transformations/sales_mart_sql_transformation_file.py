# calculation for customer data mart
# find out the customer total purchase every month


from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.utility.logging_config import logger
from src.main.write.database_write import DatabaseWriter

def sales_mart_calculation_table_write(final_sales_data_mart_df):
    window = Window.partitionBy("store_id","sales_person_id","sales_month")
    final_sales_team_data_mart = final_sales_data_mart_df\
                    .withColumn("sales_month",
                                substring(col("sales_date"),1,7))\
                    .withColumn("total_sales_every_month",
                                sum("total_cost").over(window))\
                    .select("store_id", "sales_person_id","sales_month",
                            concat(col("sales_person_first_name"), lit(" "),
                                   col("sales_person_last_name")).alias("full_name"),
                            col("total_sales_every_month"))\
                    .distinct()

    rank_window = window.partitionBy("store_id", "sales_month")\
                .orderBy(col("total_sales_every_month").desc())
    final_sales_team_data_mart_table = final_sales_team_data_mart\
                        .withColumn("rnk", rank().over(rank_window))\
                        .withColumn("incentive", when(col("rnk")==1,
                                col("total_sales_every_month")*0.01).otherwise(lit(0))) \
                        .withColumn("total_sales", col("total_sales_every_month"))\
                        .select("store_id", "sales_person_id", "full_name",
                                "sales_month", "total_sales", "incentive")

    final_sales_team_data_mart.show()
    logger.info("Data mart final df was successfully created")

    #Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_table,config.sales_team_data_mart_table)

