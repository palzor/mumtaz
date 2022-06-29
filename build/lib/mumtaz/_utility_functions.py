from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from ._autoloader import *

class utiltyfunctions(Upsert):

    def validate_path(self,expected, path):

        files = dbutils.fs.ls(path)
        message = f"Expected {expected} files, found {len(files)} in {path}"
        for file in files:
            message += f"\n{file.path}"

        if len(files) != expected:
            display(files)
        raise AssertionError(message)

    def castDataTypeToDW (dataType):
        if dataType in ['varchar','char','nchar','nvarchar','sql_variant','image','uniqueidentifier','xml']:
            _result='STRING'
        elif dataType in ['datetime'] :
            _result='TIMESTAMP'
        elif dataType in ['real','money'] :
            _result='float'
        elif dataType in ['varbinary'] :
                _result='BINARY'
        elif dataType in ['bit'] :
            _result='BOOLEAN'
        else:
            _result = dataType
        return _result
    spark.udf.register("castDataTypeToDW", castDataTypeToDW)
    ##################### create functions###################
    ### create Metadat.loadsourceschema Table ######
    def _create_metadata_loadsourceschema_table(self):
        import time
        start = int(time.time())
        print(f"Creating metadata.loadsourceschema", end="...")
        spark.sql("""CREATE TABLE IF NOT EXISTS metadata.loadsourceschema (
                        PipelineName STRING,
                        DataFactory STRING,
                        SourceName STRING,
                        SourceType STRING,
                        PipelineTriggerId STRING,
                        PipelineTriggerName STRING,
                        PipelineTriggerTime STRING,
                        PipelineTriggerType STRING,
                        TableCatalog STRING,
                        TableSchema STRING,
                        TableName STRING,
                        ColumnName STRING,
                        OrdinalPosition STRING,
                        ColumnDefault STRING,
                        IsNullable STRING,
                        DataType STRING,
                        CharacterMaximumLength STRING,
                        CharacterOctetLength STRING,
                        NumericPrecision STRING,
                        NumericPrecisionRadix STRING,
                        NumericScale STRING)
                        USING delta
                        TBLPROPERTIES (
                        'Type' = 'MANAGED')
                        """
                )

        print(f"({int(time.time())-start} seconds records)")

    ### create metadata.loadsourceconstraints Table ######
    def _create_metadata_loadsourceconstraints_table(self):
        import time
        start = int(time.time())
        print(f"Creating metadata.loadsourceconstraints", end="...")
        spark.sql("""CREATE TABLE IF NOT EXISTS metadata.loadsourceconstraints 
                            (
                            PipelineName STRING,
                            DataFactory STRING,
                            SourceName STRING,
                            SourceType STRING,
                            PipelineTriggerId STRING,
                            PipelineTriggerName STRING,
                            PipelineTriggerTime STRING,
                            PipelineTriggerType STRING,
                            tableName STRING,
                            ColumnName STRING)
                            USING delta
                            TBLPROPERTIES (
                            'Type' = 'MANAGED'
                            )"""
                )

        print(f"({int(time.time())-start} seconds records)")

    ### create Metadata.tables Table ######
    def _create_metadata_tables_table(self):
        import time
        start = int(time.time())
        print(f"Creating metadata.tables", end="...")
        spark.sql("""CREATE TABLE IF NOT EXISTS metadata.tables 
                        (
                            pipelineName STRING,
                            tableCatalog STRING,
                            tableSchema STRING,
                            tableName STRING,
                            isActive STRING,
                            generate STRING,
                            bsFileType STRING,
                            bsFolderName STRING,
                            bsFileName STRING,
                            bsArcFolderName STRING,
                            bsArcFileName STRING,
                            bronzeTableSchema STRING,
                            silverTableSchema STRING,
                            bronzeTableName STRING,
                            bronzePreparedTableName STRING,
                            silverTableName STRING,
                            bronzeTableLocation STRING,
                            bronzePreparedTableLocation STRING,
                            silverTableLocation STRING,
                            sourceQuery STRING,
                            sourceSchema STRING,
                            mergeConstraints STRING,
                            ddlBronzeRawCreateTable STRING,
                            ddlBronzePreparedCreateTable STRING,
                            ddlSilverCreateTable STRING,
                            sourceType STRING,
                            bronzePreCopyScript STRING,
                            silverPreCopyScript STRING
                        )
                    USING delta
                    TBLPROPERTIES (
                                'Type' = 'MANAGED'
                                )
                    """
                )

        print(f"({int(time.time())-start} seconds records)")

    ### create Metadata.Columns Table ######
    def _create_metadata_columns_table(self):
        import time
        start = int(time.time())
        print(f"Creating metadata.columns", end="...")
        spark.sql("""    
                CREATE TABLE IF NOT EXISTS metadata.columns (
                pipelineName STRING,
                tableCatalog STRING,
                tableSchema STRING,
                tableName STRING,
                columnName STRING,
                generate STRING,
                ordinalPosition BIGINT,
                columnDefault STRING,
                isNullable STRING,
                dataType STRING,
                characterMaximumLength DOUBLE,
                characterOctetLength DOUBLE,
                numericPrecision DOUBLE,
                numericPrecisionRadix DOUBLE,
                numericScale DOUBLE,
                dwColumnName STRING,
                dataTypeDW STRING)
                USING delta
                TBLPROPERTIES (
                'Type' = 'MANAGED')"""
                )

        print(f"({int(time.time())-start} seconds records)")

    ### create Metadata.tableList Table ######
    def _create_metadata_tableList_table(self):
        import time
        start = int(time.time())
        print(f"Creating metadata.tableList", end="...")
        spark.sql("""    
                CREATE TABLE IF NOT EXISTS metadata.tableList (
                PartitionKey STRING,
                RowKey STRING,
                TimeStamp TIMESTAMP,
                sourceDB STRING,
                sourceTable STRING)
                USING delta
                TBLPROPERTIES (
                'Type' = 'MANAGED')"""
                )

        print(f"({int(time.time())-start} seconds records)")
    
    ### create Metadata.ExcludedColumns Table ######
    def _create_metadata_excludedColumns_table(self):
        import time
        start = int(time.time())
        print(f"Creating metadata.ExcludedColumns", end="...")
        spark.sql("""    
                CREATE TABLE IF NOT EXISTS metadata.ExcludedColumns (
                    PartitionKey STRING,
                    RowKey STRING,
                    TimeStamp TIMESTAMP,
                    catalog	STRING,
                    excludedColumn	STRING,
                    sourceTable	STRING
                )
                USING delta
                TBLPROPERTIES ('Type' = 'MANAGED')"""
                )

        print(f"({int(time.time())-start} seconds records)")
   
    ##################### process functions###################
    ### load data into Metadata.tables ####
    def process_metadata_tables(self):
        import time
        from pyspark.sql.utils import AnalysisException

        #### loading metadata.tables ######
        #oad data into the metadata.tables
        def execute_tables():
        
            query=  'SELECT DISTINCT \
                            pipelineName \
                            ,tableCatalog \
                            ,tableSchema \
                            ,tableName \
                            ,"N" as isActive \
                            ,"N" as generate\
                            ,"csv" as bsFileType\
                            ,concat("landing/" , tableName ,"/")  as bsFolderName\
                            ,concat(tableName , ".csv") as bsFileName\
                            ,concat("archive/landing/" , sourceName , "/") as bsArcFolderName\
                            ,concat(tableName , ".csv") as bsArcFileName\
                            ,concat("bronze_" , sourceName,"Reporting") as bronzeTableSchema\
                            ,concat("silver_" , sourceName,"Reporting") as silverTableSchema\
                            ,concat("raw_",tableName) as bronzeTableName\
                            ,concat("prepared_",tableName) as bronzePreparedTableName\
                            ,tableName as silverTableName\
                            ,concat("/mnt/dbfs/bronze/raw_",tableName) as bronzeTableLocation\
                            ,concat("/mnt/dbfs/bronze/prepared_",tableName) as bronzePreparedTableLocation\
                            ,concat("/mnt/dbfs/silver/",tableName) as silverTableLocation\
                            ,"" as SourceQuery\
                            ,"" as SourceSchema\
                            ,"" as MergeConstraints\
                            ,"" as ddlBronzeRawCreateTable\
                            ,"" as ddlBronzePreparedCreateTable\
                            ,"" as ddlSilverCreateTable\
                            ,SourceType \
                            ,"" as bronzePreCopyScript \
                            ,"" as silverPreCopyScript \
                            FROM METADATA.LoadSourceSchema \
                            WHERE pipelineName ="{}"'.format(self.pipelinename)
            
            df_table=spark.sql(query)
            df_table.createOrReplaceTempView("temp_metadatatables")
            spark.sql("""MERGE INTO metadata.tables t
            USING temp_metadatatables s
                ON s.tableName = t.tableName
                and s.pipelinename=t.pipelinename
            WHEN NOT MATCHED THEN
            INSERT *
            """)
        
        ### processing of Metadata.tables
        start = int(time.time())
        print(f"Processing the Metadata.tables table", end="...")
        ### create the table first
        try: self._create_metadata_tables_table()
        except AnalysisException: self._create_metadata_tables_table()
        ###insert the data 
        try: execute_tables()
        except AnalysisException: execute_tables()
        
        print(f"({int(time.time())-start} seconds)")
        total = spark.read.table("Metadata.tables").count()
        print(f"...Metadata.tables: {total} records)")

    ### load data into Metadata.Columns ####
    def process_metadata_columns(self):
        import time
        from pyspark.sql.utils import AnalysisException
        
        #### loading metadata.tables ######
        def execute_columns():
            #qery to extract column information from loadschema table
            query='SELECT \
                    l.pipelineName \
                    ,l.tableCatalog \
                    ,l.tableSchema \
                    ,l.tableName \
                    ,l.columnName \
                    ,"N" as generate \
                    ,l.ordinalPosition \
                    ,l.columnDefault \
                    ,l.isNullable \
                    ,l.dataType \
                    ,l.characterMaximumLength \
                    ,l.characterOctetLength \
                    ,l.numericPrecision \
                    ,l.numericPrecisionRadix \
                    ,l.numericScale \
                    ,l.ColumnName as dwColumnName \
                    ,castDataTypeToDW(dataType) as  dataTypeDW \
                FROM METADATA.LoadSourceSchema l\
                where pipelineName ="{}"'.format(self.pipelinename)
            df_columns=spark.sql(query)
            #load the data into the temp table
            df_columns.createOrReplaceTempView("temp_metadatacolumns")
            spark.sql("MERGE INTO metadata.columns t \
            USING temp_metadatacolumns s \
                ON s.tableName = t.tableName \
            WHEN NOT MATCHED THEN \
            INSERT * \
            ")
        
        #### loading metadata.tables ######
        #oad data into the metadata.tables
        
        # processing of Metadata.columns
        start = int(time.time())
        print(f"Processing the Metadata.columns table", end="...")
        ### create the table first
        try: self._create_metadata_columns_table()
        except AnalysisException: self._create_metadata_columns_table()
        #insert the data
        try: execute_columns()
        except AnalysisException: execute_columns()
        
        print(f"({int(time.time())-start} seconds)")
        
        total = spark.read.table("Metadata.columns").count()
        print(f"...Metadata.columns: {total} records)")
        
    #process metadata.loadSourceSchema tables
    def process_metadata_loadSourceSchema(self):

        import time
        from pyspark.sql.utils import AnalysisException
        
        def execute_loadSourceSchema():

            df_loadSourceSchema=spark.read.format('csv')\
                .option("header","true")\
                .load('/mnt/sourceschema/LoadSourceSchema')\
                
            
            df_EC=spark.sql("select  * from metadata.ExcludedColumns")
            df_tableList=spark.sql("select  distinct sourceTable from metadata.tableList")

            #only few tables from storage tables
            df_loadSourceSchema=df_loadSourceSchema.join(df_tableList,df_tableList.sourceTable==df_loadSourceSchema.TableName,"inner")

            cond = [df_loadSourceSchema.TableName==df_EC.sourceTable , df_loadSourceSchema.ColumnName==df_EC.excludedColumn]
            ## exclude some sensitive columns
            df_loadSourceSchema=df_loadSourceSchema.join(df_EC,cond,"left_outer")\
                            .where(df_EC.sourceTable.isNull())
            df_loadSourceSchema.createOrReplaceTempView("temp_loadSourceSchema")



            spark.sql("MERGE INTO metadata.LoadSourceSchema t \
                        USING temp_loadSourceSchema s \
                            ON s.tableName = t.tableName \
                        WHEN NOT MATCHED THEN \
                        INSERT * \
                        ")
        
        start = int(time.time())
        print(f"Processing the Metadata.LoadSourceSchema table", end="...")
        ### create the table first
        try: self._create_metadata_loadsourceschema_table()
        except AnalysisException: self._create_metadata_loadsourceschema_table()
        #insert the data
        try: execute_loadSourceSchema()
        except AnalysisException: execute_loadSourceSchema()

        print(f"({int(time.time())-start} seconds)")

        total = spark.read.table("Metadata.LoadSourceSchema").count()
        print(f"...Metadata.LoadSourceSchema: {total} records)")
    
    #process metadata.loadSourceConstraints tables
    def process_metadata_loadSourceConstraints(self):

        import time
        from pyspark.sql.utils import AnalysisException
        
        def execute_loadSourceConstraints():

            df_LoadSourceConstraints=spark.read.format('csv')\
                .option("header","true")\
                .load('/mnt/sourceschema/LoadSourceConstraints')\
                
            df_LoadSourceConstraints.createOrReplaceTempView("temp_loadSourceConstraints")

            spark.sql("MERGE INTO metadata.loadSourceConstraints t \
                        USING temp_loadSourceConstraints s \
                            ON s.tableName = t.tableName \
                            AND s.columnName=t.columnName\
                        WHEN MATCHED THEN \
                        UPDATE SET *\
                        WHEN NOT MATCHED THEN \
                        INSERT * \
                        ")
        
        start = int(time.time())
        print(f"Processing the Metadata.loadSourceConstraints table", end="...")
        ### create the table first
        try: self._create_metadata_loadsourceconstraints_table()
        except AnalysisException: self._create_metadata_loadsourceconstraints_table()
        #insert the data
        try: execute_loadSourceConstraints()
        except AnalysisException: execute_loadSourceConstraints()

        print(f"({int(time.time())-start} seconds)")

        total = spark.read.table("Metadata.loadSourceConstraints").count()
        print(f"...Metadata.loadSourceConstraints: {total} records)")

    #process metadata.tableList tables
    def process_metadata_tableList(self):

        import time
        from pyspark.sql.utils import AnalysisException
        from pyspark.sql.types import StringType
        from pyspark.sql.functions import udf

        def execute_tableList():

            df_tableList=spark.read.format('csv')\
                .option("header","true")\
                .load('/mnt/sourceschema/tableList')\
            
            spaceDeleteUDF = udf(lambda s: s.replace(" ", ""), StringType())
            df_tableList=df_tableList.withColumn("sourceTable",spaceDeleteUDF("sourceTable"))
               
            df_tableList.createOrReplaceTempView("temp_tableList")
            spark.sql("MERGE INTO metadata.tableList t \
                        USING temp_tableList s \
                            ON s.sourceTable = t.sourceTable \
                        WHEN NOT MATCHED THEN \
                        INSERT * \
                        ")
        
        start = int(time.time())
        print(f"Processing the Metadata.tableList table", end="...")
        ### create the table first
        try: self._create_metadata_tableList_table()
        except AnalysisException: self._create_metadata_tableList_table()
        #insert the data
        try: execute_tableList()
        except AnalysisException: execute_tableList()

        print(f"({int(time.time())-start} seconds)")

        total = spark.read.table("Metadata.tableList").count()
        print(f"...Metadata.tableList: {total} records)")
    
    #process metadata.excludedColumns tables
    def process_metadata_excludedColumns(self):

        import time
        from pyspark.sql.utils import AnalysisException
        from pyspark.sql.types import StringType
        from pyspark.sql.functions import udf
        
        def execute_excludedColumns():

            df_excludedColumns=spark.read.format('csv')\
                .option("header","true")\
                .load('/mnt/sourceschema/excludedColumns')\

                         #remove trailing spaces
            spaceDeleteUDF = udf(lambda s: s.replace(" ", ""), StringType())
            df_excludedColumns=df_excludedColumns.withColumn("sourceTable",spaceDeleteUDF("sourceTable"))\
                                     .withColumn("excludedColumn",spaceDeleteUDF("excludedColumn"))   
            df_excludedColumns.createOrReplaceTempView("temp_excludedColumns")

            spark.sql("MERGE INTO metadata.excludedColumns t \
                        USING temp_excludedColumns s \
                            ON s.excludedColumn = t.excludedColumn \
                            and s.sourceTable = t.sourceTable \
                            and s.catalog = t.catalog \
                        WHEN NOT MATCHED THEN \
                        INSERT * \
                        ")
        
        start = int(time.time())
        print(f"Processing the Metadata.excludedColumns table", end="...")
        ### create the table first
        try: self._create_metadata_excludedColumns_table()
        except AnalysisException: self._create_metadata_excludedColumns_table()
        #insert the data
        try: execute_excludedColumns()
        except AnalysisException: execute_excludedColumns()

        print(f"({int(time.time())-start} seconds)")

        total = spark.read.table("Metadata.excludedColumns").count()
        print(f"...Metadata.excludedColumns: {total} records)")
    
    ##################### update functions###################
    #update the metadata.table with neccessary info
    def updateMetadataTables(self):
        
        import time
        from pyspark.sql.utils import AnalysisException
        #create frame for table
        df_dd_metadatatables=spark.sql(f"select * from metadata.tables where pipelinename='{self.pipelinename}'")
        df_dd_columns=spark.sql(f"select * from metadata.columns where pipelinename='{self.pipelinename}'")
        df_dd_constraints=spark.sql(f"select * from metadata.loadsourceconstraints where pipelinename='{self.pipelinename}'")
        
        #create frame for column
        pdf_columns=df_dd_columns.toPandas()
        pdf_metadatatables=df_dd_metadatatables.toPandas()
        pdf_constraints=df_dd_constraints.toPandas()
        #create frame for constraints
        
        for i,row in pdf_metadatatables.iterrows():
            sql1='select '
            ipdf_dd_columns=pdf_columns[pdf_columns['tableName']==row['tableName']]
            ipdf_dd_constraint_columns=pdf_constraints[pdf_constraints['tableName']==row['tableName']]
            sql2=''
            sql4=''
            sql5=''


            for ii,iirow in ipdf_dd_columns.iterrows():
                sep = "," if ii < ipdf_dd_columns.index[-1] else ""
                sql2+=iirow['dwColumnName']+sep

                sql4+=iirow['dwColumnName']+" "+iirow['dataTypeDW']+sep

                sql3 = " from "+ row['tableSchema'] +"."+ row['tableName']
        
            for j,jrow in ipdf_dd_constraint_columns.iterrows():
                sep = " AND " if j < ipdf_dd_constraint_columns.index[-1] else ""
                sql5+="s."+jrow['ColumnName']+"=t."+jrow['ColumnName']+" "+sep

            row['sourceQuery']=sql1+sql2+sql3 
            row['sourceSchema']=sql4
            row['mergeConstraints']=sql5
        
        for i,row in pdf_metadatatables.iterrows():
            sql1_1='CREATE TABLE IF NOT EXISTS '+row['bronzeTableSchema']+'.'+row['bronzeTableName']+'('
            sql1_2='CREATE TABLE IF NOT EXISTS '+row['bronzeTableSchema']+'.'+row['bronzePreparedTableName']+'('
            sql1_3='CREATE TABLE IF NOT EXISTS '+row['silverTableSchema']+'.'+row['silverTableName']+'('
            ipdf_dd_columns=pdf_columns[pdf_columns['tableName']==row['tableName']]
            sql2=''
            for ii,roww in ipdf_dd_columns.iterrows():
                sep = "," if ii < ipdf_dd_columns.index[-1] else ")"
                sql2+=(roww['dwColumnName']+' '+roww['dataTypeDW'])+sep

            sql3=   ' USING DELTA'

            sql4_1= ' TBLPROPERTIES ("quality" = "bronze",delta.enableChangeDataFeed=true)'
            sql4_2= ' TBLPROPERTIES ("quality" = "silver",delta.enableChangeDataFeed=true)'

            sql5_1= 'LOCATION "'+row['bronzeTableLocation']+'"'
            sql5_2= 'LOCATION "'+row['bronzePreparedTableLocation']+'"'
            sql5_3= 'LOCATION "'+row['silverTableLocation']+'"'

            row['ddlBronzeRawCreateTable']     =sql1_1 +sql2 +sql3 +sql4_1 +sql5_1
            row['ddlBronzePreparedCreateTable']=sql1_2 +sql2 +sql3 +sql4_1 +sql5_2
            row['ddlSilverCreateTable']        =sql1_3 +sql2 +sql3 +sql4_2 +sql5_3
            row['bronzePreCopyScript']='CREATE SCHEMA IF NOT EXISTS '+row['bronzeTableSchema']
            row['silverPreCopyScript']='CREATE SCHEMA IF NOT EXISTS '+row['silverTableSchema']
            row['isActive']='Y'
        sdf_metadatatables=spark.createDataFrame(pdf_metadatatables)
        sdf_metadatatables.createOrReplaceTempView("tempdf_metadatatables")
            
        def execute_update():
            
            spark.sql("MERGE INTO metadata.tables t \
            USING tempdf_metadatatables s \
            ON s.tableName = t.tableName \
            WHEN MATCHED THEN \
            UPDATE \
            SET t.ddlBronzeRawCreateTable = s.ddlBronzeRawCreateTable, \
            t.ddlBronzePreparedCreateTable = s.ddlBronzePreparedCreateTable, \
            t.ddlSilverCreateTable = s.ddlSilverCreateTable, \
            t.SourceQuery=s.SourceQuery, \
            t.SourceSchema=s.SourceSchema, \
            t.MergeConstraints=s.MergeConstraints, \
            t.bronzePreCopyScript=s.bronzePreCopyScript, \
            t.silverPreCopyScript=s.silverPreCopyScript, \
            t.isActive=s.isActive \
            WHEN NOT MATCHED THEN \
            INSERT * ")
            
        start = int(time.time())
        print(f"Updating the Metadata.tables table", end="...")
        ### create the table first
        try: execute_update()
        except AnalysisException: execute_update()
        print(f"({int(time.time())-start} seconds)")

        total = spark.read.table("Metadata.tables").count()
        print(f"...Metadata.tables: {total} records)")
        self.pdf_metadatatables = pdf_metadatatables
    
    # run the autoloader
    def execute_load_data(self, create_all=False):
        if create_all:
            df_dd_metadatatables=spark.sql(f"select * from metadata.tables")
            df_dd_PreCopyScript=df_dd_metadatatables[['bronzePreCopyScript','silverPreCopyScript']].distinct()
            self.pdf_metadatatables=df_dd_metadatatables.toPandas()
            self.pdf_dd_PreCopyScript=df_dd_PreCopyScript.toPandas()
        else :
            df_dd_metadatatables=spark.sql(f"select * from metadata.tables where pipelineName='{self.pipelinename}' and isActive='Y' ")
            df_dd_PreCopyScript=df_dd_metadatatables[['bronzePreCopyScript','silverPreCopyScript']].distinct()
            
            self.pdf_metadatatables=df_dd_metadatatables.toPandas()
            self.pdf_dd_PreCopyScript=df_dd_PreCopyScript.toPandas()
        
        #pocess bronze
            self._load_bronze_data()
    
    def _load_bronze_data(self):
        for _, row in self.pdf_dd_PreCopyScript.iterrows():
            spark.sql(row['bronzePreCopyScript'])

        for i,row in  self.pdf_metadatatables.iterrows():
            # create the tables
            spark.sql(row['ddlBronzeRawCreateTable'])
            #spark.sql(row['ddlBronzePreparedCreateTable'])

            
            targettable=row['bronzeTableSchema']+"."+row['bronzeTableName']
            MergeConstraints=row['mergeConstraints']
            checkpoint_path = row['bronzeTableLocation']+'/_checkpoints'
            write_path = row['bronzeTableLocation']
            upload_path ="/mnt/"+row['bsFolderName']
            SourceSchema =row['sourceSchema']
            
            #create a upsert class object
            streaming_merge = Upsert(targettable=targettable,MergeConstraints=MergeConstraints,checkpoint_path=checkpoint_path,write_path=write_path,upload_path=upload_path,SourceSchema=SourceSchema)

            df=streaming_merge._sourcedata()
            print(i,targettable)
            streamQuery=(df.writeStream
                    .format('delta') \
                    .outputMode('append')\
                    .foreachBatch(streaming_merge._upsert_to_delta)\
                    .queryName(targettable)\
                    .option('checkpointLocation', checkpoint_path)\
                    .trigger(once=True)
                    .start()
                    )
    
    
    #def _load_silver_data(self):
        
