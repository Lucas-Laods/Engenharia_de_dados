# Databricks notebook source
from pyspark.sql.functions import col, lag, year, avg, min, max, stddev
from pyspark.sql.window import Window

# ============================================
# GOLD - Modelagem para análise
# ============================================

# 1. Leitura da camada Silver
df = spark.table("silver_sp500")

# 2. Definição da janela temporal
# Uma única série temporal: S&P 500
window = Window.orderBy("Date")

# 3. Cálculo do retorno diário
df = df.withColumn(
    "Daily_Return",
        (col("Close") / lag("Close", 1).over(window)) - 1
        )

# 4. Criação do ano
df = df.withColumn(
    "Year",
    year(col("Date"))
                )

# ============================================
# GOLD - Dados diários para análise
# ============================================

gold_daily_sp500 = df

gold_daily_sp500.write \
    .mode("overwrite") \
    .saveAsTable("gold_daily_sp500")


# ============================================
# GOLD 1 - Performance anual
# ============================================

gold_annual_performance = (
    df.groupBy("Year")
        .agg(
            avg("Close").alias("Avg_Close"),
            min("Close").alias("Min_Close"),
            max("Close").alias("Max_Close")
                                                              )
                                                                     .orderBy("Year")
                                                                    )

# 5. Persistência
                                                        
gold_annual_performance.write \
                                                                 .mode("overwrite") \
                                                                 .saveAsTable("gold_annual_performance")


# ============================================               # GOLD 2 - Volatilidade anual
# ============================================

gold_annual_volatility = (
                                                                 df.groupBy("Year")
                                                                     .agg(
                                                                         avg("Daily_Return").alias("Avg_Return"),
                                                                         stddev("Daily_Return").alias("Volatility")
                                                                                                                )
                                                                     .orderBy("Year")
                                                                                                                      )

# 6. Persistência

gold_annual_volatility.write \
                                                                                                                        .mode("overwrite") \
                                                                                                                        .saveAsTable("gold_annual_volatility")

# COMMAND ----------

display(
        df.select(
                "Date",
                "Close",
                "Daily_Return",
                "Year"
                                            )
        .orderBy("Date")
        .limit(10)
                                                    )

# COMMAND ----------

# ============================================
# GOLD - Dados diários para análise
# ============================================

display(
        spark.sql("""
            SELECT *
            FROM gold_daily_sp500
            ORDER BY Date
            LIMIT 10
                                            """)
                                            )

# COMMAND ----------

# ============================================
# Performance anual
# ============================================

display(
        spark.sql("""
                SELECT *
                FROM gold_annual_performance
                ORDER BY Year
                                    """)
                                    )

# ============================================
# Volatilidade anual
# ============================================

display(
        spark.sql("""
                SELECT *
                FROM gold_annual_volatility
                                ORDER BY Year
                                    """)
                                    )

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_registros,
            COUNT(Daily_Return) AS retornos_calculados,
            COUNT(*) - COUNT(Daily_Return) AS retornos_nulos
        FROM (
            SELECT
                Date,
                Close,
                (Close / LAG(Close, 1) OVER (ORDER BY Date)) - 1 AS Daily_Return
                                                                         FROM silver_sp500
)
        """)
                )

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            MIN(Year) AS primeiro_ano,
            MAX(Year) AS ultimo_ano,
            COUNT(DISTINCT Year) AS quantidade_anos
        FROM (
                                                                        SELECT
                                                                             year(Date) AS Year
                                                                         FROM silver_sp500
    )
                """))
                                                                                                                