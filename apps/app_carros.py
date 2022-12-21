from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "carros_mongodb")

spark = SparkSession(sc)

spark.sparkContext.setLogLevel('WARN')

carros = spark.read.format("mongodb").option("database", "venda").option(
    "collection", "carros").load()

#Modelo de carro que mais aparece
modelo_predominante = carros.groupBy(['model_name']).count()
modelo_predominante = modelo_predominante.sort("count", ascending=False)

# Automaticos mais rodados
carros_automaticos_rodados = carros.filter(
    carros['transmission'] == 'automatic').select(
        "model_name", "odometer_value", "year_produced",
        "engine_fuel").sort("odometer_value", ascending=False)

# Manuais mais rodados
carros_manuais_rodados = carros.filter(
    carros['transmission'] == 'mechanical').select(
        "model_name", "odometer_value", "year_produced",
        "engine_fuel").sort("odometer_value", ascending=False)

# Tipos de carroceria que mais aparecem
carrocerias_predominantes = carros.groupBy(['body_type']).count()
carrocerias_predominantes = carrocerias_predominantes.sort("count",
                                                           ascending=False)

# Cores mais utilizadas
cor_predominante = carros.groupBy(['color']).count()
cor_predominante = cor_predominante.sort("count", ascending=False)

# Salvar resultado em csv
modelo_predominante.write.mode("overwrite").options(
    header='True', delimiter=',').csv("analises/modelo_predominante")
carros_automaticos_rodados.write.mode("overwrite").options(
    header='True', delimiter=',').csv("analises/carros_automaticos_rodados")
carros_manuais_rodados.write.mode("overwrite").options(
    header='True', delimiter=',').csv("analises/carros_manuais_rodados")
carrocerias_predominantes.write.mode("overwrite").options(
    header='True', delimiter=',').csv("analises/carrocerias_predominantes")
cor_predominante.write.mode("overwrite").options(
    header='True', delimiter=',').csv("analises/cor_predominate")
