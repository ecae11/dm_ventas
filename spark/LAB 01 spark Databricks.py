# Laboratorio SPARK Databricks
# Profesor : Arturo Rojas
# Curso Ingenieria de datos

# 
df1 = spark.sql('SELECT * FROM default.factura')

# COMANDO ----------

df1.show()

# COMANDO ----------

df1.printSchema()

# COMANDO ----------

df2 = spark.sql('select * from default.factura where Venta> 20000')

# COMANDO ----------

df2.show()

# COMANDO ----------

df3 = spark.sql('select count(*) from default.factura')

# COMANDO ----------

df3.show()

# COMANDO ----------

dfData = spark.sql('select * from default.factura')

# COMANDO ----------

dfData.show()

# COMANDO ----------

df1 = dfData.select(dfData["Fecha"], dfData["CodigoPais"], dfData["Cantidad"])

# COMANDO ----------

df1.show()

# COMANDO ----------

df2 = dfData.filter(dfData["precio"]> 1000)

# COMANDO ----------

df2.show()

# COMANDO ----------

#SELECT * FROM dfData WHERE PRECIO > 1000 AND Cantidad > 50
df3 = dfData.filter((dfData["PRECIO"] > 1000) & (dfData["Cantidad"] > 50))

# COMANDO ----------

df3.show()

# COMANDO ----------

#SELECT * FROM dfData WHERE PRECIO > 1000 OR Cantidad < 20
df4 = dfData.filter((dfData["PRECIO"] > 1000) | (dfData["Cantidad"] < 20))

# COMANDO ----------

#OPERACION EQUIVALENTE EN SQL:
#SELECT
# CodigoModelo
# COUNT(cantidad)
# MIN(venta)
# SUM(venta)
# MAX(objetivo)
#FROM
# dfData
#GROUP BY
# CodigoModelo

# COMANDO ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMANDO ----------

df5 = dfData.groupBy(dfData["CodigoModelo"]).agg(
        f.count(dfData["cantidad"]),
        f.min(dfData["venta"]),
        f.sum(dfData["venta"]),
        f.max(dfData["objetivo"])
)


# COMANDO ----------

df5.show()

# COMANDO ----------

df6 = dfData.groupBy(dfData["CodigoModelo"]).agg(
f.count(dfData["cantidad"]).alias("CANTIDAD_VENTA"),
f.min(dfData["venta"]).alias("MINIMA_venta"),
f.sum(dfData["venta"]).alias("SUMA_VENTA"),
f.max(dfData["objetivo"]).alias("MAXIMO_OBJETIVO")
)


# COMANDO ----------

df6.show()

# COMANDO ----------


df7 = dfData.sort(dfData["cantidad"].asc())

# COMANDO ----------

df7.show()

# COMANDO ----------

df8 = dfData.sort(dfData["venta"].desc(), dfData["cantidad"].asc())


# COMANDO ----------

df8.show()

# COMANDO ----------

dfModelo = spark.sql("SELECT * FROM default.MODELO")


# COMANDO ----------

dfModelo.show()

# COMANDO ----------

dfMarca = spark.sql("SELECT * FROM default.MARCA")

# COMANDO ----------

dfMarca.show()

# COMANDO ----------

dfJoin = dfModelo.alias("MO").join(
dfMarca.alias("MA"),
f.col("MO.CodigoMarca") == f.col("MA.CodigoMarca")
).select(
"MO.CodigoModelo",
"MO.DescripcionModelo",
"MO.Precio",
"MO.CodigoMarca",
"MA.DescripcionMarca"
)

# COMANDO ----------

dfJoin.show()

# COMANDO ----------


