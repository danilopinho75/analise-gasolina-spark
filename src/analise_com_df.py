#%%
# Importar bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# %%
# Criação da Sessão Spark
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("spark_dataframe_api")
    .getOrCreate()
)
# %%
# Leitura de arquivo
df = (
    spark.
    read
    .option('delimiter', ';')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .option('encoding', 'ISO-8859-1')
    .csv('./dataset/precos-gasolina-etanol-11.csv')
)
#%%
# Conferir Schema
df.printSchema()
# %%
# Selecionando as colunas
df_precos = (
    df.select(
        'Estado - Sigla',
        'Produto',
        'Valor de Compra',
        'Valor de Venda',
        'Unidade de Medida'
    )
)
df_precos.show(5)
# %%
# Verificar dados nulos
(
    df_precos.where(
        F.col('Valor de Compra').isNotNull()
    )
    .show()
)
# %%
# Transforma coluna string para float
df_precos = (
    df
    .select(
        'Estado - Sigla',
        'Produto',
        'Valor de Venda',
        'Unidade de Medida'
    )
    .withColumn(
        'Valor de Venda', 
        F.regexp_replace(F.col('Valor de Venda'), ',', '.')
        .cast('float')    
    )

)
df_precos.show(5)
# %%
# Análise do preço dos produtos por Estado e a diferença de variação
df_precos_analise = (
    df_precos
    .groupBy(
        F.col('Estado - Sigla'),
        F.col('Produto'),
        F.col('Unidade de Medida')
    )
    .agg(
        F.min(F.col("Valor de Venda")).alias('menor_valor'),
        F.max(F.col("Valor de Venda")).alias('maior_valor')
    )
    .withColumn(
        "diferenca",
        F.col('maior_valor') - F.col('menor_valor')
    )
    .orderBy("diferenca", ascending=False)
)
df_precos_analise.show()
# %%
