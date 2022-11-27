import sys
from pyspark import SparkContext

from pyspark.sql import SparkSession

try:
    print(len(sys.argv))
    if len(sys.argv) < 2:
        raise Exception("Insira o numero da linha quando executar o programa")
except Exception as ex:
    print(ex)
    sys.exit(1)

n = sys.argv[1]

spark = SparkSession.builder \
                            .master("local") \
                            .appName("Onibus App") \
                            .config("spark.some.config.option", "some-value") \
                            .getOrCreate()

linhas_ds = spark.read.option("multiline", "true").json(
    "hdfs://node-master:9000/user/root/onibus/2018_04_21_linhas.json")
tabela_linhas_ds = spark.read.option("multiline", "true").json(
    "hdfs://node-master:9000/user/root/onibus/2018_04_21_tabelaLinha.json")
trechos_ds = spark.read.option("multiline", "true").json(
    "hdfs://node-master:9000/user/root/onibus/2018_04_21_trechosItinerarios.json"
)

joined = linhas_ds.join(tabela_linhas_ds,
                        linhas_ds.COD == tabela_linhas_ds.COD)

joined_rdd = joined.rdd


def map_func(line):
    return {
        "num_linha": line.COD,
        "nome": line.NOME,
        "hora": line.HORA,
        "ponto": line.PONTO
    }


def red_func(x, y):
    if x["num_linha"] == y["num_linha"]:
        return {"num_linha": x["num_linha"]}
    return x


mapped = joined_rdd.map(map_func)


def map2(line):
    return {"nome_linha": line["nome"], "ponto": line["ponto"], "horario": line["hora"]}


resultado = mapped.filter(lambda x: x["num_linha"] == str(n)).map(
    map2).collect()

if (len(resultado) == 0):
    print("linha nao encontrada")
else:
    print(f"linha {n}: ")
    for entry in resultado:
        print(entry)
