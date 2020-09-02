from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql import Window
from fuzzywuzzy import fuzz
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()


def name_similarity(name_a, name_b):
  return fuzz.token_set_ratio(name_a, name_b)

name_similarity_udf = func.udf(name_similarity, IntegerType())


def get_similar_names(df, name_col):
    """
    Given a Spark DataFrame and name columns, returns the most similar pairs
    of names. These results can be used to deduplicate a dataset.

    Arguments:

    df: (Spark DataFrame) - The Spark DataFrame to test for duplicate names
    name_col: (str) - The DataFrame column that contains the names

    Returns as Spark DataFrame containing the most similar pairs of names, 
    including a similarity score.
    """

    unique_id_window = Window.orderBy(col(name_col))
    df_unique_id = (df.withColumn('id', func.row_number().over(unique_id_window)))

    cols = df_unique_id.columns
    _a = [f'{col}_a' for col in cols]
    _b = [f'{col}_b' for col in cols]

    cols_a = zip(cols, _a)
    cols_b = zip(cols, _b)

    expr_a = [f'{old_name} as {new_name}' for old_name, new_name in cols_a]
    expr_b = [f'{old_name} as {new_name}' for old_name, new_name in cols_b]

    df_a = df_unique_id.selectExpr(expr_a)
    df_b = df_unique_id.selectExpr(expr_b)

    most_similar_window = Window.partitionBy(col('id_a')).orderBy(col('similarity').desc())

    most_similar = (df_a.crossJoin(df_b)
                        .filter(col('id_a') != col('id_b'))
                        .withColumn('similarity', name_similarity_udf(col(f'{name_col}_a'), col(f'{name_col}_b')))
                        .withColumn('similarity_rank', func.row_number().over(most_similar_window))
                        .filter(col('similarity_rank') == 1)
                        .drop('similarity_rank')
                        .orderBy(col('similarity').desc()))

    return most_similar