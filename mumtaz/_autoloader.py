import os
from delta.tables import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.dbutils import DBUtils

global spark 
global dbutils

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
dbutils = DBUtils(spark)
class Upsert:


    def __init__(self, **kwargs):
        self.kwargs = kwargs
        
    def _upsert_to_delta(self,microBatchOuptuDF,batchId):
        deltadf=DeltaTable.forName(spark,self.kwargs['targettable'])
        (
             deltadf.alias('t')
            .merge(
                microBatchOuptuDF.alias('s'),
                self.kwargs['MergeConstraints']
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
   
    def _sourcedata(self):
        
        df = spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format', 'csv') \
            .option('header', 'true') \
            .schema(self.kwargs['SourceSchema'])\
            .load(self.kwargs['upload_path'])
        return df
    
