def validate_data(df):
    null_counts = {col: df.filter(df[col].isNull()).count() for col in df.columns}
    negative_price_count = df.filter("price < 0").count()
    assert negative_price_count == 0, "Found transactions with negative price!"
    return null_counts
