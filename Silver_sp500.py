# Databricks notebook source
from pyspark.sql.functions import *

df = spark.table("bronze_sp500")

df = (
df
.dropDuplicates()
.filter(col("Close").isNotNull())
)

df = df.withColumn(
"Date",
to_date(col("Date"))
)

df.write.mode("overwrite").saveAsTable("silver_sp500")


#Completude
spark.sql("""
SELECT
COUNT(*) total,
COUNT(Close) close_preenchido
FROM silver_sp500
""")

#Duplicados
spark.sql("""
SELECT
Date,
COUNT(*)
FROM silver_sp500
GROUP BY Date
HAVING COUNT(*) > 1
""")

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# ============================================
# SILVER - Limpeza e padronização
# ============================================

# 1. Leitura da camada Bronze
df = spark.table("bronze_sp500")

# 2. Remoção de registros duplicados
df = df.dropDuplicates()

# 3. Remoção de registros sem valor de fechamento
df = df.filter(col("Close").isNotNull())

# 4. Conversão da coluna Date para o tipo date
df = df.withColumn(
    "Date",
        to_date(col("Date"))
        )

# 5. Persistência da camada Silver
df.write.mode("overwrite").saveAsTable("silver_sp500")

# ============================================
# TESTES DE QUALIDADE
# ============================================

# 6. Completude - verificar valores preenchidos em Close
spark.sql("""
    SELECT
        COUNT(*) AS total_registros,
        COUNT(Close) AS close_preenchido,
        COUNT(*) - COUNT(Close) AS close_nulo
    FROM silver_sp500
""")

# 7. Duplicidade por data
display(spark.sql("""
    SELECT
        Date,
        COUNT(*) AS quantidade
                                                                 FROM silver_sp500
                                                                 GROUP BY Date
                                                                 HAVING COUNT(*) > 1
                                                                 ORDER BY Date
                                                                            """))

display(spark.sql("""
    SELECT
            COUNT(*) AS total_registros,
                    COUNT(Close) AS close_preenchido,
                            COUNT(*) - COUNT(Close) AS close_nulo
                                FROM silver_sp500
                                """))

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_registros,
            COUNT(Date) AS date_preenchido,
            COUNT(Open) AS open_preenchido,
            COUNT(High) AS high_preenchido,
            COUNT(Low) AS low_preenchido,
            COUNT(Close) AS close_preenchido,
            COUNT(Adj_Close) AS adj_close_preenchido,
            COUNT(Volume) AS volume_preenchido
        FROM silver_sp500
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            COUNT(*) AS registros_inconsistentes
        FROM silver_sp500
        WHERE
            High < Low
            OR High < Open
            OR High < Close
            OR Low > Open
            OR Low > Close
    """)
)