from pyspark.sql import SparkSession
from pathlib import Path
import time



def challenge(path):
    
    start = time.time()
    
    spark = SparkSession.builder \
        .appName('challenge') \
        .getOrCreate()

    rdd = spark.sparkContext.textFile(path)

    def parse_line(line):
        station, temp = line.split(';')
        return station, float(temp) /10

    parsed_rdd = rdd.map(parse_line)

    stats_rdd = parsed_rdd.groupByKey().mapValues(lambda temps: (
        min(temps),
        max(temps),
        sum(temps) / len(temps),
        len(temps)
    ))

    stats_rdd.map(lambda x: f"{x[0]};{x[1][0]};{x[1][1]};{x[1][2]:.1f};{x[1][3]}") \
        .saveAsTextFile("resultado1")

    finish = time.time()
    
    print(f"Processamento concluído em {finish - start:.2f} segundos.")


if __name__ == '__main__':
    FILE_PATH = 'data/measurements.txt'
    
    challenge(FILE_PATH)