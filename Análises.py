# Databricks notebook source
# Perguntas de negócio 

# Como o S&P 500 evoluiu ao longo do período de 1927 a 2020?

# Quais foram os anos de maior alta e de maior queda do S&P 500 no pperíodo analisado?

# Como a volatilidade do S&P 500 variou entre 1927 e 2020?

# Como períodos de grandes crises históricas se refletiram nos preços, retornos e volatilidade do S&P 500?

# COMMAND ----------

# ===========================================================
# PERGUNTA 1 — EVOLUÇÃO HISTÓRICA DO S&P 500
# ===========================================================

display(
    spark.sql("""
        SELECT
            Year,
            Avg_Close,
            Min_Close,
            Max_Close
                                                                     FROM gold_annual_performance
        ORDER BY Year                                                                              """))

# COMMAND ----------

# ===========================================================
# GRÁFICO — EVOLUÇÃO ANUAL DO S&P 500
# ===========================================================

import matplotlib.pyplot as plt

df_grafico = spark.sql("""
    SELECT
        Year,
        Avg_Close
    FROM gold_annual_performance
    ORDER BY Year
                            """).toPandas()

plt.figure(figsize=(14, 6))

plt.plot(
    df_grafico["Year"],
    df_grafico["Avg_Close"]
                                    )

plt.title("Evolução do preço médio anual do S&P 500")
plt.xlabel("Ano")
plt.ylabel("Preço médio de fechamento")

plt.grid(True)
plt.show()

# COMMAND ----------

# ===========================================================
# ANOS COM MENOR E MAIOR PREÇO MÉDIO
# ===========================================================

display(
    spark.sql("""
        SELECT
            MIN(Year) AS primeiro_ano,
            MAX(Year) AS ultimo_ano,

            ROUND(
                MAX(CASE WHEN Year = 1927 THEN Avg_Close END),
                2
            ) AS preco_medio_inicial,

            ROUND(
                MAX(CASE WHEN Year = 2020 THEN Avg_Close END),
                2
            ) AS preco_medio_final,

            ROUND(MIN(Avg_Close), 2) AS menor_preco_medio,
            ROUND(MAX(Avg_Close), 2) AS maior_preco_medio

        FROM gold_annual_performance
    """)
)

# COMMAND ----------

import matplotlib.pyplot as plt

df_retornos = spark.sql("""
    SELECT
        Year,
            Avg_Return
        FROM gold_annual_volatility
        WHERE Avg_Return IS NOT NULL
        ORDER BY Year
            """).toPandas()

plt.figure(figsize=(14, 6))

plt.bar(
    df_retornos["Year"],
    df_retornos["Avg_Return"]
                                        )

plt.title("Retorno médio diário por ano do S&P 500")
plt.xlabel("Ano")
plt.ylabel("Retorno médio diário")

plt.axhline(0)
plt.grid(True, axis="y")

plt.show()

# COMMAND ----------

# ===========================================================
# PERGUNTA 2 — RETORNO ANUAL EFETIVO
# ===========================================================

display(
spark.sql("""
WITH fechamento_anual AS (
SELECT
YEAR(Date) AS Year,
Date,
Close,
ROW_NUMBER() OVER (
PARTITION BY YEAR(Date)
ORDER BY Date DESC
) AS rn
FROM gold_daily_sp500
),

ultimo_dia AS (
SELECT
Year,
Close
FROM fechamento_anual
WHERE rn = 1
),

retornos AS (
SELECT
Year,
Close,
LAG(Close) OVER (ORDER BY Year) AS Close_Ano_Anterior
FROM ultimo_dia
)

SELECT
Year,
Close_Ano_Anterior,
Close,
(Close / Close_Ano_Anterior - 1) AS Annual_Return
FROM retornos
WHERE Close_Ano_Anterior IS NOT NULL
ORDER BY Annual_Return DESC
LIMIT 1
""")
)

display(
spark.sql("""
WITH fechamento_anual AS (
SELECT
YEAR(Date) AS Year,
Date,
Close,
ROW_NUMBER() OVER (
PARTITION BY YEAR(Date)
ORDER BY Date DESC
) AS rn
FROM gold_daily_sp500
),

ultimo_dia AS (
SELECT
Year,
Close
FROM fechamento_anual
WHERE rn = 1
),

retornos AS (
SELECT
Year,
Close,
LAG(Close) OVER (ORDER BY Year) AS Close_Ano_Anterior
FROM ultimo_dia
)

SELECT
Year,
Close_Ano_Anterior,
Close,
(Close / Close_Ano_Anterior - 1) AS Annual_Return
FROM retornos
WHERE Close_Ano_Anterior IS NOT NULL
ORDER BY Annual_Return ASC
LIMIT 1
""")
)









# COMMAND ----------

# Para esta análise, a alta ou queda anual foi definida como a variação percentual entre o fechamento do último pregão de um ano e o fechamento do último pregão do ano anterior.

# COMMAND ----------

# Em 1954 ocorreu a maior alta anual da série analisada, com retorno de aproximadamente 45,02% em relação ao fechamento do ano anterior. A maior queda ocorreu em 1931, com retorno de aproximadamente −47,07%.


# COMMAND ----------

display(
        spark.sql("""
                SELECT
                        Year,
                        Avg_Return
                FROM gold_annual_volatility
                WHERE Avg_Return IS NOT NULL
                ORDER BY Avg_Return DESC
                LIMIT 1
                                """))

# COMMAND ----------

# 1933 teve o maior retorno diário médio, mas isso não significa que tenha tido a maior valorização entre o fechamento de um ano e o fechamento do ano anterior.

# COMMAND ----------

# ==================≈========================================
# Como a volatilidade do S&P 500 variou entre 1927 e 2020?
# ===========================================================

# COMMAND ----------

# ===========================================================
# GRÁFICO — VOLATILIDADE ANUAL
#============================================================

import matplotlib.pyplot as plt

df_volatilidade = spark.sql("""
SELECT
Year,
Volatility
FROM gold_annual_volatility
WHERE Volatility IS NOT NULL
ORDER BY Year
""").toPandas()

plt.figure(figsize=(14, 6))

plt.plot(
df_volatilidade["Year"],
df_volatilidade["Volatility"]
)

plt.title("Volatilidade anual do S&P 500")
plt.xlabel("Ano")
plt.ylabel("Volatilidade")

plt.grid(True)

plt.show()


# COMMAND ----------

# A volatilidade foi particularmente elevada no início da série;
# Houve períodos de volatilidade relativamente baixa nas décadas seguintes;
# Existem novos picos em determinados períodos posteriores;
# 2020 apresenta uma elevação importante em relação aos anos imediatamente anteriores.

# COMMAND ----------

# 5 maiores níveis de volatilidade

display(
spark.sql("""
SELECT
Year,
Volatility
FROM gold_annual_volatility
WHERE Volatility IS NOT NULL
ORDER BY Volatility DESC
LIMIT 5
""")
)




# 5 menores níveis de volatilidade

display(
spark.sql("""
SELECT
Year,
Volatility
FROM gold_annual_volatility
WHERE Volatility IS NOT NULL
ORDER BY Volatility ASC
LIMIT 5
""")
)

# Volatilidade inicial, final, mínima e máxima

display(
spark.sql("""
SELECT
MIN(Year) AS primeiro_ano,
MAX(Year) AS ultimo_ano,
MIN(Volatility) AS menor_volatilidade,
MAX(Volatility) AS maior_volatilidade
FROM gold_annual_volatility
WHERE Volatility IS NOT NULL
""")
)


# COMMAND ----------

#| Indicador          |          Resultado |
#| ------------------ | -----------------: |
#| Menor volatilidade | **1964 — 0,00331** |
#| Maior volatilidade | **1932 — 0,03371** |
#| 2ª maior           | **1933 — 0,03073** |
#| 3ª maior           | **1931 — 0,02620** |
#| 4ª maior           | **2008 — 0,02581** |
#| 5ª maior           | **1929 — 0,02349** |


# COMMAND ----------

# A volatilidade anual do S&P 500 apresentou variações significativas ao longo do período analisado. O maior nível de volatilidade ocorreu em 1932, com 0,03371, seguido por 1933 (0,03073) e 1931 (0,02620). Outro período de elevada volatilidade foi 2008, com 0,02581. Em contraste, o menor nível foi observado em 1964, com 0,00331. Esses resultados indicam que o comportamento do mercado apresentou períodos distintos de maior e menor instabilidade ao longo da série histórica.

# COMMAND ----------

# A volatilidade utilizada nesta análise corresponde ao desvio-padrão dos retornos diários dentro de cada ano e não representa uma volatilidade anualizada.

# COMMAND ----------

# ===========================================================
# PERGUNTA 4 - COMPORTAMENTO DO S&P 500 EM PERÍODOS ASSOCIADOS A GRANDES CRISES
# ===========================================================

from pyspark.sql.functions import col, lag, row_number
from pyspark.sql.window import Window

# -----------------------------------------------------------
# 1. Calcular o retorno anual
# -----------------------------------------------------------

df_daily = spark.table("gold_daily_sp500")

window_ano = Window.partitionBy("Year").orderBy("Date")

# Último fechamento disponível de cada ano
df_fechamento_anual = (
    df_daily
    .withColumn(
        "rn",
        row_number().over(
            Window.partitionBy("Year").orderBy(col("Date").desc())
        )
    )
    .filter(col("rn") == 1)
    .select(
        "Year",
        col("Close").alias("Close_Ano")
    )
)

# Fechamento do ano anterior
window_retorno = Window.orderBy("Year")

df_retorno_anual = (
    df_fechamento_anual
    .withColumn(
        "Close_Ano_Anterior",
        lag("Close_Ano", 1).over(window_retorno)
    )
    .withColumn(
        "Annual_Return",
        (col("Close_Ano") / col("Close_Ano_Anterior")) - 1
    )
)

# -----------------------------------------------------------
# 2. Juntar preço, retorno e volatilidade
# -----------------------------------------------------------

df_crises = (
    spark.table("gold_annual_performance")
    .select(
        "Year",
        "Avg_Close"
    )
    .join(
        df_retorno_anual.select(
            "Year",
            "Annual_Return"
        ),
        on="Year",
        how="left"
    )
    .join(
        spark.table("gold_annual_volatility")
        .select(
            "Year",
            "Volatility"
        ),
        on="Year",
        how="left"
    )
)

# -----------------------------------------------------------
# 3. Selecionar os períodos de crise
# -----------------------------------------------------------

periodos_crise = [1929, 1930, 1931, 1932,
                  2007, 2008, 2009,
                  2020]

resultado_crises = (
    df_crises
    .filter(col("Year").isin(periodos_crise))
    .orderBy("Year")
)

display(resultado_crises)

# COMMAND ----------

import matplotlib.pyplot as plt

df_crises_grafico = resultado_crises.toPandas()

plt.figure(figsize=(12, 6))
plt.plot(
    df_crises_grafico["Year"],
    df_crises_grafico["Volatility"],
    marker="o"
)

plt.title("Volatilidade do S&P 500 em períodos selecionados")
plt.xlabel("Ano")
plt.ylabel("Volatilidade")
plt.grid(True)
plt.show()

# COMMAND ----------

plt.figure(figsize=(12, 6))
plt.bar(
    df_crises_grafico["Year"].astype(str),
    df_crises_grafico["Annual_Return"] * 100
)

plt.title("Retorno anual do S&P 500 em períodos selecionados")
plt.xlabel("Ano")
plt.ylabel("Retorno anual (%)")
plt.grid(axis="y")
plt.show()

# COMMAND ----------

# Os períodos selecionados apresentam comportamentos distintos em relação aos preços, retornos e volatilidade. Entre 1929 e 1932, os retornos anuais foram negativos, com a maior queda ocorrendo em 1931 (-47,07%) e a maior volatilidade em 1932 (0,03371). No período de 2007 a 2009, 2008 apresentou retorno negativo de -38,49% e volatilidade elevada (0,02581), seguido por um retorno positivo de 23,45% em 2009. Em 2020, o retorno anual foi positivo em 6,58%, enquanto a volatilidade permaneceu relativamente elevada (0,02338). Os resultados demonstram que retorno e volatilidade são métricas distintas e que o ano de maior queda não necessariamente coincide com o ano de maior volatilidade.