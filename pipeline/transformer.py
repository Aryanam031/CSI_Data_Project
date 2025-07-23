from pyspark.sql.functions import avg, count, col

def join_data(transactions_df, products_df):
    return transactions_df.join(products_df, on="product_id", how="left")

def get_avg_order_value(df):
    return df.groupBy("customer_id").agg(avg("price").alias("avg_order_value"))

def get_popular_products(df):
    return df.groupBy("product_id").agg(count("*").alias("orders")).orderBy(col("orders").desc())

def get_popular_categories(df):
    return df.groupBy("category").agg(count("*").alias("category_orders")).orderBy(col("category_orders").desc())

def get_campaign_impact(df):
    return df.groupBy("campaign_id").agg(avg("price").alias("avg_spend"))
