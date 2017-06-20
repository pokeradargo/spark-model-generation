
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row

from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.classification import LogisticRegressionWithSGD

# Load Spark Context
sc = SparkContext()
# Load SQL Context
sqlContext = SQLContext(sc)
# Load data
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter=",").load('data/cooc_appears_processed.sample.csv')
### Example of Pokemon 1 model

def is_pokemon(row):
    print row
    if (row.pokemonId == '16'):
        return Row({
            "appearedTimeOfDay": row.appearedTimeOfDay,
            "terrainType": row.terrainType,
            "closeToWater": row.closeToWater,
            "continent": row.continent,
            "temperature",
            "windSpeed",
            "pressure",
            "weatherIcon",
            "urbanization",
            "gymDistance",
            "pokestopDistance"
        })
    else:
        return Row(
            "appearedTimeOfDay",
            "terrainType",
            "closeToWater",
            "continent",
            "temperature",
            "windSpeed",
            "pressure",
            "weatherIcon",
            "urbanization",
            "gymDistance",
            "pokestopDistance"
        )

appears = sc.parallelize(df.collect()).map(is_pokemon)

print appears.collect()

assembler = VectorAssembler(
    inputCols=[
        "appearedTimeOfDay",
        "terrainType",
        "closeToWater",
        "continent",
        "temperature",
        "windSpeed",
        "pressure",
        "weatherIcon",
        "urbanization",
        "gymDistance",
        "pokestopDistance"
    ],
    outputCol="isPokemon"
)


#lrm = LogisticRegressionWithSGD.train(df, iterations=10)
