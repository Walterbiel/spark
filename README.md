# Spark para ETL em Databricks e Synapse Analytics

Este documento é um guia completo sobre como utilizar Apache Spark para processos de ETL (Extract, Transform, Load), focando nas plataformas **Azure Databricks** e **Azure Synapse Analytics**. Ele cobre conceitos essenciais, tipos de arquivos, práticas recomendadas, funções úteis e exemplos de código.

---

## ✨ Visão Geral do Spark para ETL

Apache Spark é uma engine distribuída de processamento de dados em larga escala, ideal para workloads de ETL devido à sua performance e escalabilidade.

**Principais Benefícios:**
- Processamento paralelo e distribuído.
- Suporte a diversos formatos de arquivo (Parquet, CSV, JSON, Delta, Avro, ORC).
- Integração nativa com Databricks e Synapse.
- APIs em Python, Scala, SQL, R.

### 🔍 Comparativo entre Plataformas

| Recurso                     | Databricks                         | Synapse Spark                    |
|----------------------------|------------------------------------|----------------------------------|
| Tipo de cluster            | Permanente ou job cluster          | Pool dinâmico                    |
| Linguagens suportadas     | Python, SQL, Scala, R              | Python, SQL, Scala               |
| Suporte Delta Lake         | Nativo                             | Compatível com configurações     |
| Integração com Data Lake   | Alta                               | Alta                             |
| Performance tuning         | Avançado (Photon, AQE)             | Limitado                         |

## 📚 Tipos de Arquivos Suportados

| Tipo     | Leitura | Escrita | Observações                                            |
|----------|---------|---------|--------------------------------------------------------|
| CSV      | ✅      | ✅      | Formato texto simples, bom para interoperabilidade. Lento para grandes volumes. |
| JSON     | ✅      | ✅      | Suporta estruturas aninhadas. Pode ser pesado para leitura e parsing.          |
| Parquet  | ✅      | ✅      | Colunar, altamente eficiente em leitura e compressão. Recomendado para análise. |
| Delta    | ✅      | ✅      | Extensão do Parquet com suporte a transações ACID, versionamento e time travel. Ideal para pipelines. |
| Avro     | ✅      | ✅      | Compactado, schema embutido. Ótimo para troca de dados entre sistemas.         |
| ORC      | ✅      | ✅      | Semelhante ao Parquet, mas mais usado no ecossistema Hadoop. Alta compressão.  |

> 💡 **Recomendação**: Prefira Parquet ou Delta para grandes volumes de dados em ambientes analíticos, especialmente se houver necessidade de consultas otimizadas.
---

## ⚙️ Iniciando o Spark (PySpark)

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("ETL Spark")
    .getOrCreate())
```

## 🧱 Conceitos Iniciais com RDD

Antes do uso de DataFrames, o Spark operava com RDDs (Resilient Distributed Datasets), uma estrutura de dados distribuída e tolerante a falhas.

```python
# Criando RDD a partir de lista
rdd = spark.sparkContext.parallelize([("cliente1", 25), ("cliente2", 30)])

# Transformações
rdd_filtrado = rdd.filter(lambda x: x[1] > 26)

# Ações
print(rdd_filtrado.collect())
```

Principais operações:
- `map`, `flatMap`, `filter`
- `reduce`, `collect`, `count`
- `groupByKey`, `reduceByKey`, `join`

---

## 🚀 ETL com Spark: Etapas Fundamentais

### 1. Extração (Extract)
Obter dados de fontes diversas:
- Arquivos (CSV, JSON, Parquet, Delta, etc.)
- Bancos de dados (JDBC)
- APIs
- Armazenamento em nuvem (Azure Data Lake, Blob Storage)

### 2. Transformação (Transform)
- Limpeza de dados (nulls, duplicatas)
- Conversão de tipos
- Normalização e agregados
- Joins e filtros

### 3. Carga (Load)
- Salvar dados transformados em:
  - Data Lakes (Parquet/Delta)
  - Data Warehouses (Synapse SQL Pools)
  - Outros sistemas analíticos

---

## 🧪 50 Funções Úteis para Transformação com PySpark

### 🔡 Manipulação de Strings
```python
upper(col("nome"))
lower(col("nome"))
trim(col("campo"))
substring("cpf", 1, 3)
concat_ws("-", "ano", "mes")
split(col("nome"), " ")
regexp_replace("tel", "[^0-9]", "")
initcap(col("nome"))
length(col("descricao"))
instr(col("descricao"), "abc")
```

### 🧮 Manipulação Numérica
```python
round(col("valor"), 2)
abs(col("saldo"))
pow(col("base"), 2)
sqrt(col("valor"))
log(col("valor"))
exp(col("valor"))
bround(col("valor"), 1)
```

### 📆 Manipulação de Datas
```python
to_date("data", "yyyy-MM-dd")
to_timestamp("data_hora")
current_date()
current_timestamp()
datediff("data_fim", "data_ini")
months_between("fim", "inicio")
add_months("data", 2)
next_day("data", "Monday")
last_day("data")
```

### 🔄 Condicionais e Nulos
```python
when(col("idade") > 18, "adulto").otherwise("menor")
coalesce("email", "email_backup")
isnull("campo")
isnotnull("campo")
```

### 🧠 Funções Analíticas (Janela)
```python
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("cliente").orderBy("data")

row_number().over(windowSpec)
rank().over(windowSpec)
lag("valor", 1).over(windowSpec)
lead("valor", 1).over(windowSpec)
sum("valor").over(windowSpec)
avg("valor").over(windowSpec)
```

### 🔧 Outras funções úteis
```python
col("campo")
lit("valor")
explode(col("lista"))
array_contains(col("categorias"), "x")
countDistinct("cliente_id")
sha2(col("id"), 256)
broadcast(df2)
approx_count_distinct("id")
```

---

## 🔍 Extras

### Schema Explícito
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True)
])

df = spark.read.schema(schema).csv("/mnt/raw/clientes.csv")
```

### Particionamento ao Salvar
```python
(df.write
    .partitionBy("ano", "mes")
    .mode("overwrite")
    .format("delta")
    .save("/mnt/gold/vendas"))
```

### Carga Incremental
```python
(df.filter(col("data") >= lit("2023-01-01"))
   .write
   .mode("append")
   .format("delta")
   .save("/mnt/gold/vendas"))
```

---

## 🔗 Referências

- [Documentação oficial do Apache Spark](https://spark.apache.org/docs/latest/)
- [Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/)
- [Azure Synapse Analytics](https://learn.microsoft.com/en-us/azure/synapse-analytics/)
- [Delta Lake](https://delta.io/)

---

> Desenvolvido por Walter Gonzaga - [WGG Digital Solutions](https://www.linkedin.com/in/waltergonzaga/)
