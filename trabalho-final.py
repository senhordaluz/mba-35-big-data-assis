from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = SparkContext()
sc.setLogLevel('ERROR')

sqlContext = SQLContext(sc)
spark = SparkSession(sc)
spark.conf.set('spark.sql.crossJoin.enabled', 'true')

# Carrega CSV com Schema definido

schema = StructType([
    StructField('DataExtracao', StringType(), False),
    StructField('Cliente', IntegerType(), False),
    StructField('Contrato', DoubleType(), False),
    StructField('DataContrato', DateType(), False),
    StructField('Produto', StringType(), False),
    StructField('Banco', IntegerType(), False),
    StructField('NomeBanco', StringType(), False),
    StructField('Moeda', StringType(), False),
    StructField('TaxaCambio', DoubleType(), False),
    StructField('Parcela', IntegerType(), False),
    StructField('DataVencimento', DateType(), False),
    StructField('ValorParcela', DoubleType(), False),
    StructField('ValorPago', DoubleType(), False),
    StructField('DataPagamento', DateType(), False),
    StructField('ValorCancelado', DoubleType(), False),
    StructField('ValorDesconto', DoubleType(), False),
    StructField('Localidade', StringType(), False)
    ])

positivo = (sqlContext.read
    .schema(schema)
    .option('mode', 'DROPMALFORMED')
    .option('delimiter', ';')
    .option('charset', 'UTF-8')
    .option('header', 'true')
    .csv('PositivoV02.csv'))

positivo.createOrReplaceTempView("positivo")

primaria = (sqlContext.sql("""
    SELECT
        Contrato, Cliente, DataContrato, Produto, Banco, NomeBanco, Moeda, TaxaCambio, 
        COUNT(Parcela) AS Qtd_Prestacao, 
        SUM(ValorParcela * TaxaCambio) AS V_Contrato, 
        SUM(ValorPago * TaxaCambio) AS T_Pago, 
        SUM(ValorCancelado * TaxaCambio) AS T_Cancelado,
        SUM(ValorDesconto * TaxaCambio) AS T_Desconto,
        SUM(ValorParcela * TaxaCambio - ValorPago * TaxaCambio - ValorCancelado * TaxaCambio - ValorDesconto * TaxaCambio) AS Saldo,
        MAX(DataVencimento) AS DataUltPrestacao
    FROM positivo AS primaria
    GROUP BY Cliente, Contrato, DataContrato, Produto, Banco, NomeBanco, Moeda, TaxaCambio
    ORDER BY Contrato
"""))

secundaria = (sqlContext.sql("""
    SELECT
        Contrato AS Contrato2,
        COUNT(Parcela) AS QtdPrestAbertas
	FROM positivo
	WHERE Contrato = Contrato
	AND ValorPago = 0.00
	GROUP BY Contrato
"""))

terciaria = (sqlContext.sql("""
    SELECT
        Contrato AS Contrato3,
        MAX(DataVencimento) AS DataUltParcela 
    FROM positivo
    GROUP BY Contrato
"""))

quaternaria = (sqlContext.sql("""
    SELECT
        Contrato AS Contrato4,
        COUNT(Contrato) AS QtdPrestVencer
	FROM positivo
	WHERE Contrato = 9010000113
	AND DataVencimento > DATE( SUBSTRING( DataExtracao, 0, 10 ) )
	GROUP BY Contrato
"""))

join1 = (
    primaria.join(secundaria, primaria.Contrato == secundaria.Contrato2, how='left')
    .drop('Contrato2'))

join2 = (
    join1.join(terciaria, join1.Contrato == terciaria.Contrato3, how='left')
    .drop('Contrato3'))

resultado_final = (
    join2.join(quaternaria, join2.Contrato == quaternaria.Contrato4, how='left')
    .drop('Contrato4'))

resultado_final.cache()

(resultado_final
    .repartition(1)
    .write.mode('overwrite')
    .option('delimiter', ';')
    .format('com.databricks.spark.csv')
    .save('trabalho-final.csv', header = 'true'))

(resultado_final
    .repartition(1)
    .write.mode('overwrite')
    .format('com.databricks.spark.csv')
    .save('trabalho-final-com-virgula.csv', header = 'true'))