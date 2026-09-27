# MVP — Construção de um Pipeline de Dados na Nuvem

## Análise Histórica do S&P 500

Este repositório apresenta o desenvolvimento do MVP de Engenharia de Dados, cujo objetivo foi construir um pipeline de dados em ambiente de nuvem para tratamento, organização, validação e análise histórica do índice S&P 500.

O projeto foi desenvolvido utilizando Databricks, PySpark e SQL, seguindo uma arquitetura Medallion composta pelas camadas Bronze, Silver e Gold.

---

# 1. Contexto de Negócios e Perguntas

## 1.1 Contexto

O projeto utiliza dados históricos do índice S&P 500 para construir um pipeline de dados capaz de transformar dados brutos em informações estruturadas para análise.

A proposta envolve as etapas de carga, tratamento, modelagem, validação e análise dos dados, permitindo observar o comportamento histórico do índice entre 1927 e 2020.

## 1.2 Objetivo

Construir um pipeline de dados em ambiente de nuvem utilizando a arquitetura Bronze, Silver e Gold, garantindo que os dados sejam carregados, tratados, organizados e disponibilizados para análise.

## 1.3 Perguntas de Negócio

As análises realizadas buscaram responder às seguintes perguntas:

1. Como o S&P 500 evoluiu ao longo do período de 1927 a 2020?
2. Quais foram os anos de maior alta e de maior queda do S&P 500 no período analisado?
3. Como a volatilidade do S&P 500 variou entre 1927 e 2020?
4. Como períodos históricos selecionados se refletiram nos preços, retornos e volatilidade do S&P 500?

## 1.4 Dados Utilizados

O conjunto de dados utilizado contém informações históricas diárias do S&P 500, incluindo:

- Date
- Open
- High
- Low
- Close
- Adj Close
- Volume

O período analisado compreende os anos de 1927 a 2020.

## 1.5 Fonte e Licença

Fonte dos dados:

S&P 500 Historical Data — Kaggle

https://www.kaggle.com/datasets/henryhan117/sp-500-historical-data

Licença informada para os dados:

Database Contents License (DbCL) v1.0

https://opendatacommons.org/licenses/dbcl/1-0/

---

# 2. Carga dos Dados

O arquivo `SPX.csv` foi disponibilizado no Databricks Volume e utilizado como fonte para a primeira etapa do pipeline.

Caminho utilizado no Databricks:

`/Volumes/workspace/default/engenharia_de_dados/SPX.csv`

A leitura foi realizada utilizando Spark com cabeçalho e inferência de tipos.

Durante a carga, a coluna `Adj Close` foi padronizada para `Adj_Close`.

Após a leitura e padronização, os dados foram persistidos na tabela:

`bronze_sp500`

A camada Bronze representa os dados carregados e armazenados no ambiente de nuvem antes das transformações de limpeza e preparação.

---

# 3. Modelagem e Catálogo de Dados

O projeto utiliza a arquitetura Medallion:

```text
SPX.csv
   ↓
Bronze
   ↓
Silver
   ↓
Gold
````

## 3.1 Bronze

Tabela:

`bronze_sp500`

Principais campos:

| Campo     | Tipo   | Descrição                    |
| --------- | ------ | ---------------------------- |
| Date      | date   | Data do registro             |
| Open      | double | Preço de abertura            |
| High      | double | Maior preço do período       |
| Low       | double | Menor preço do período       |
| Close     | double | Preço de fechamento          |
| Adj_Close | double | Preço de fechamento ajustado |
| Volume    | long   | Volume negociado             |

## 3.2 Silver

Tabela:

`silver_sp500`

A camada Silver recebe os dados da Bronze e aplica tratamentos de qualidade, incluindo:

* remoção de registros duplicados;
* remoção de registros sem valor em `Close`;
* conversão da coluna `Date` para o tipo `date`.

Após o tratamento, a tabela possui 23.323 registros.

## 3.3 Gold

A camada Gold disponibiliza os dados preparados para análise.

Tabela:

`gold_daily_sp500`

Além dos campos originais, foram adicionadas:

* `Daily_Return`: retorno diário calculado a partir do preço de fechamento;
* `Year`: ano extraído da data.

Também foram criadas as tabelas:

`gold_annual_performance`

Contém as métricas anuais:

* `Year`
* `Avg_Close`
* `Min_Close`
* `Max_Close`

`gold_annual_volatility`

Contém:

* `Year`
* `Avg_Return`
* `Volatility`

As tabelas anuais possuem 94 registros, correspondentes aos anos de 1927 a 2020.

## 3.4 Linhagem dos Dados

```text
SPX.csv
   ↓
bronze_sp500
   ↓
silver_sp500
   ↓
gold_daily_sp500
   ├── gold_annual_performance
   └── gold_annual_volatility
```

---

# 4. Pipeline de Dados

O pipeline foi desenvolvido no Databricks utilizando PySpark e SQL.

## 4.1 Etapas

O processo pode ser resumido nas seguintes etapas:

1. Disponibilização do arquivo `SPX.csv` no Databricks Volume.
2. Leitura do arquivo utilizando Spark.
3. Padronização do nome da coluna `Adj Close`.
4. Persistência na camada Bronze.
5. Remoção de duplicidades.
6. Tratamento de valores nulos.
7. Padronização do tipo da coluna `Date`.
8. Cálculo do retorno diário e criação da coluna `Year`.
9. Criação das métricas anuais de desempenho.
10. Criação das métricas anuais de volatilidade.

## 4.2 Arquitetura

```text
                    ┌──────────────────────────┐
                    │       SPX.csv            │
                    └────────────┬─────────────┘
                                   ↓
                    ┌──────────────────────────┐
                    │      bronze_sp500        │
                    └────────────┬─────────────┘
                                   ↓
                    ┌──────────────────────────┐
                    │       silver_sp500       │
                    └────────────┬─────────────┘
                                   ↓
                    ┌──────────────────────────┐
                    │     gold_daily_sp500     │
                    └────────────┬─────────────┘
                                   ↓
                  ┌──────────────┴──────────────┐
                  ↓                                   ↓
     ┌──────────────────────────┐   ┌──────────────────────────┐
     │ gold_annual_performance   │   │ gold_annual_volatility   │
     └──────────────────────────┘   └──────────────────────────┘
```

O pipeline foi executado e validado no ambiente Databricks.

---

# 5. Qualidade de Dados

Foram realizados testes de qualidade na camada Silver e validações adicionais na camada Gold.

## 5.1 Completude

Foram verificadas as colunas:

* Date
* Open
* High
* Low
* Close
* Adj_Close
* Volume

Todos os campos apresentaram 23.323 registros preenchidos.

## 5.2 Duplicidade

Foi realizada uma verificação de registros duplicados por data.

Resultado:

* Registros com datas duplicadas: 0

## 5.3 Consistência

Foram verificadas inconsistências entre os valores de abertura, máxima, mínima e fechamento.

Resultado:

* Registros inconsistentes: 0

## 5.4 Validação da Camada Gold

Na tabela `gold_daily_sp500`:

* Total de registros: 23.323
* Retornos diários calculados: 23.322
* Retornos diários nulos: 1

O único valor nulo de `Daily_Return` corresponde ao primeiro registro cronológico, pois não existe um fechamento anterior para calcular seu retorno.

Os testes indicaram que os principais campos utilizados na análise apresentam completude e consistência adequadas.

---

# 6. Análise de Dados

Foram utilizadas consultas SQL, PySpark e Python para responder às perguntas de negócio.

## 6.1 Evolução do S&P 500

A análise do preço médio anual apresentou:

* Preço médio em 1927: 17,66
* Preço médio em 2020: 3.140,02
* Menor preço médio anual: 6,91 em 1932
* Maior preço médio anual: 3.140,02 em 2020

A análise foi realizada a partir da tabela `gold_annual_performance`.

## 6.2 Maior Alta e Maior Queda Anual

Para esta análise, a variação anual foi definida como a diferença percentual entre o fechamento do último pregão de um ano e o fechamento do último pregão do ano anterior.

Resultados:

* Maior alta anual: 1954, com aproximadamente +45,02%
* Maior queda anual: 1931, com aproximadamente -47,07%

## 6.3 Volatilidade

A volatilidade anual foi calculada como o desvio padrão dos retornos diários dentro de cada ano.

O maior valor observado ocorreu em 1932:

* Volatilidade: aproximadamente 0,03371

O menor valor observado ocorreu em 1964:

* Volatilidade: aproximadamente 0,00331

A medida utilizada representa a dispersão dos retornos diários dentro de cada ano e não corresponde a uma volatilidade anualizada.

## 6.4 Períodos Históricos Selecionados

Foram analisados os anos de 1929 a 1932, 2007 a 2009 e 2020, observando preço médio, retorno anual e volatilidade.

Entre 1929 e 1932, os retornos anuais observados foram negativos, com a maior queda anual do período analisado ocorrendo em 1931.

Em 2008, foi observado retorno anual de aproximadamente -38,49% e volatilidade de aproximadamente 0,02581. Em 2009, o retorno anual foi de aproximadamente +23,45%.

Em 2020, o retorno anual observado foi de aproximadamente +6,58%, com volatilidade de aproximadamente 0,02338.

Esses resultados permitem comparar diferentes períodos utilizando simultaneamente indicadores de preço, retorno e volatilidade.

---

# 7. Autoavaliação

Durante a realização deste MVP, foi desenvolvido um pipeline de dados em ambiente de nuvem utilizando o Databricks, passando pelas etapas de carga, tratamento, modelagem, validação e análise dos dados.

Os objetivos definidos no início do projeto foram atendidos de forma geral, principalmente na construção da arquitetura Bronze, Silver e Gold e na utilização dos dados tratados para responder às perguntas de negócio propostas.

Também foram aplicados conceitos de Engenharia de Dados, como organização das camadas, transformação dos dados, criação de tabelas para análise e realização de testes de qualidade.

Durante o desenvolvimento, algumas etapas exigiram ajustes, principalmente na definição das métricas utilizadas para responder às perguntas de negócio. Esses ajustes contribuíram para melhorar a consistência da análise final.

De forma geral, o projeto atingiu seu objetivo de construir um pipeline funcional de dados na nuvem e transformar os dados históricos do S&P 500 em informações estruturadas para análise.

---

# Tecnologias Utilizadas

* Databricks
* Apache Spark
* PySpark
* SQL
* Python
* Matplotlib
* GitHub

# Estrutura do Projeto

Os notebooks utilizados no desenvolvimento do pipeline e das análises serão disponibilizados neste repositório.

O dataset original não é disponibilizado no repositório. Os dados foram armazenados e processados no ambiente Databricks.

# Autor

Lucas-Laods
