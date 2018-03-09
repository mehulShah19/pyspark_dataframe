
%pyspark



from pyspark.sql.functions import col, expr, when
from pyspark.sql.types import IntegerType,StringType, DoubleType, FloatType, StructType, StructField
from pyspark.sql.functions import udf


def splitWithStructType(eventType):
    # name = String
    # age = Float
    # Array = []
    return {"name1":"Mehul", "age1": float(20), "array1" : [1,2,3]}
    
    

func_udf = udf(splitWithStructType, StructType([StructField("name1", StringType()),
                                                 StructField("age1", FloatType()),
                                                 StructField("array1", ArrayType(IntegerType()))
                                                 ]))
                                                 
df = df.withColumn('event_new_time',func_udf(df["name"]))
df = df.withColumn("Name1", df['event_new_time']['age1'])

