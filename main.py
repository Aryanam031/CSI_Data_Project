from pipeline.loader import get_spark_session, load_csv_data
from pipeline.transformer import (
    join_data,
    get_avg_order_value,
    get_popular_products,
    get_popular_categories,
    get_campaign_impact
)
from pipeline.validator import validate_data
from pipeline.writer import write_delta, optimize_delta

def main():
    spark = get_spark_session()

    # Load data
    transactions_df = load_csv_data(spark, "data/transactions.csv")
    products_df = load_csv_data(spark, "data/products.csv")

    # Validate data
    print("Transaction Data Quality Report:", validate_data(transactions_df))

    # Transform
    combined_df = join_data(transactions_df, products_df)
    avg_order_value_df = get_avg_order_value(combined_df)
    popular_products_df = get_popular_products(combined_df)
    popular_categories_df = get_popular_categories(combined_df)
    campaign_impact_df = get_campaign_impact(combined_df)

    # Save as Delta
    write_delta(avg_order_value_df, "data/output/avg_order_value")
    write_delta(popular_products_df, "data/output/popular_products")
    write_delta(popular_categories_df, "data/output/popular_categories")
    write_delta(campaign_impact_df, "data/output/campaign_impact")

    # Optimize one output (others optional)
    optimize_delta(spark, "data/output/avg_order_value")

    print("Pipeline executed successfully.")

if __name__ == "__main__":
    main()
