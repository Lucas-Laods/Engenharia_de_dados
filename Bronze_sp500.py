# Databricks notebook source
# ============================================
# BRONZE - Carga dos dados brutos
# ============================================

# 1. Leitura do arquivo CSV
df = spark.read.csv(
    "/Volumes/workspace/default/engenharia_de_dados/SPX.csv",
        header=True,
            inferSchema=True
            )

# 2. Visualização inicial dos dados
display(df)

# 3. Padronização do nome da coluna
df = df.withColumnRenamed("Adj Close", "Adj_Close")

# 4. Persistência da camada Bronze
df.write.mode("overwrite").saveAsTable("bronze_sp500")

# 5. Validação da tabela criada
spark.sql("""
    SELECT *
        FROM bronze_sp500
            LIMIT 10
                """)