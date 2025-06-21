from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg

# 🔧 1. Iniciar Spark
spark = SparkSession.builder.appName("ETL_3_Datasets_Demo").getOrCreate()

# 🗃️ 2. Crear datasets dummy

# Dataset 1: usuarios (user_id, name, age)
users = spark.createDataFrame([
    (1, "Alice", 25),
    (2, "Bob", 35),
    (3, "Charlie", None),  # edad faltante
    (4, "Diana", 28)
], ["user_id", "name", "age"])

# Dataset 2: transacciones (txn_id, user_id, amount, status)
transactions = spark.createDataFrame([
    (101, 1, 50.0, "SUCCESS"),
    (102, 2, 200.0, "PENDING"),
    (103, 2, 150.0, "SUCCESS"),
    (104, 3, 300.0, "FAILED"),
    (105, 5, 120.0, "SUCCESS")  # user_id no existe en usuarios
], ["txn_id", "user_id", "amount", "status"])

# Dataset 3: regiones (region_id, user_id, region)
regions = spark.createDataFrame([
    (1, 1, "North"),
    (2, 2, "South"),
    (3, 3, None),   # región faltante
    (4, 4, "East")
], ["region_id", "user_id", "region"])

# 👀 Ver esquemas iniciales
print("Usuarios:")
users.show()
print("Transacciones:")
transactions.show()
print("Regiones:")
regions.show()

# 🔍 3. ETL Procesamiento

# A. Limpieza: imputar edad faltante y región faltante
users_clean = users.withColumn(
    "age",
    when(col("age").isNull(), 18).otherwise(col("age"))
)
regions_clean = regions.withColumn(
    "region",
    when(col("region").isNull(), "Unknown").otherwise(col("region"))
)

# B. Join usuarios + regiones
user_info = users_clean.join(regions_clean, on="user_id", how="left")
print("Usuarios con región:")
user_info.show()

# C. Join con transacciones (solo usuarios existentes)
joined = user_info.join(transactions, on="user_id", how="inner")
print("Usuarios con transacciones:")
joined.show()

# D. Agregaciones: total y promedio por usuario y región
agg_df = joined.groupBy("user_id", "name", "region").agg(
    count("txn_id").alias("num_txn"),
    avg("amount").alias("avg_amount")
)
print("Resumen transaccional por usuario:")
agg_df.show()

# E. Filtrado condicional: usuarios con gasto promedio > 100
high_spenders = agg_df.filter(col("avg_amount") > 100)
print("Usuarios con gasto promedio > 100:")
high_spenders.show()

# 📤 4. Carga (ejemplo: guardar en parquet)
agg_df.write.mode("overwrite").parquet("/Volumes/workspace/default/data/etldemo/output/agg_by_user")
high_spenders.write.mode("overwrite").parquet("/Volumes/workspace/default/data/etldemo/output/high_spenders")

#
