from delta.tables import DeltaTable

def write_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)

def optimize_delta(spark, path):
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()
