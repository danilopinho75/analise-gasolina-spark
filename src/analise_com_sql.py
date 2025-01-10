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
    .appName("spark_sql")
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
# Conferir o Schema
df.printSchema()
# %%
# Criando uma View temporária
df.createOrReplaceTempView('combustiveis')
# Realizando consulta sql
spark.sql("""
    SELECT
        `Estado - Sigla`,
        `Produto`,
        `Valor de Compra`,
        `Valor de Venda`,
        `Unidade de Medida`
    FROM combustiveis
""").show(5)
# %%
# Conferindo dados nulos
spark.sql("""
    SELECT * FROM combustiveis
    WHERE `Valor de Compra` IS NOT NULL
""").show()
# %%
# Transformando coluna string em float
view_precos = (
    spark.sql("""
        SELECT
            `Estado - Sigla`,
            `Produto`,
            CAST(regexp_replace(`Valor de Venda`, ",", ".") AS FLOAT) as `Valor de Venda`,
            `Unidade de Medida`
        FROM combustiveis
""")
)
view_precos.printSchema()
view_precos.show()
# %%
# Criar view temporária dos preços
view_precos.createOrReplaceTempView('view_precos')
view_precos.show(5)
# %%
# Análise dos preços dos combustíveis
df_diferenca_precos = spark.sql("""
    SELECT
        `Estado - Sigla`,
        `Produto`,
        `Unidade de Medida`,
        MIN(`Valor de Venda`) AS preco_minimo,
        MAX(`Valor de Venda`) AS preco_maximo,
        ROUND(MAX(`Valor de Venda`) - MIN(`Valor de Venda`), 2) AS diferenca
    FROM view_precos
    GROUP BY ALL
    ORDER BY diferenca DESC
    ;
""")

df_diferenca_precos.show()