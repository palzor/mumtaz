from ._utility_functions import *

class Path():
    def __init__(self, working_dir, pipelinename):
        self.working_dir=working_dir
        
class DBFrameworkHelper(utiltyfunctions):
    # initialize variables
    def __init__(self,pipelinename):
        import re
        assert pipelinename is not None, f"The pipelinename  must be specified"
        self.pipelinename=pipelinename
        self.initialized = False
        self.metadata_db_name = "metadata"
        
    def mountFolder(self, containername, storageaccountname, scopename, keyname):
        mount_point_name = f"/mnt/{containername}"
 
        if not any(mount.mountPoint == mount_point_name for mount in dbutils.fs.mounts()):
            try:
                dbutils.fs.mount(
                source = f"wasbs://{containername}@{storageaccountname}.blob.core.windows.net",
                mount_point = mount_point_name,
                extra_configs = {f"fs.azure.account.key.{storageaccountname}.blob.core.windows.net":dbutils.secrets.get(scope = scopename, key =keyname)}

                )
                print(f'{mount_point_name} is mounted')
            except Exception as e:
                print(e)
        else: 
            print(f'{mount_point_name} is already mounted')
    
    def init(self, create_db=True):
        spark.catalog.clearCache()
        self.create_db = create_db
        
        if create_db:
            print(f"\nCreating the database \"{self.metadata_db_name}\"")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.metadata_db_name} ")
            spark.sql(f"USE {self.metadata_db_name}")
            
        self.initialized = True
    
    def cleanup(self):
        
        if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.metadata_db_name}'").count() == 1:
            print(f"Dropping the database \"{self.metadata_db_name}\"")
            spark.sql(f"DROP DATABASE {self.metadata_db_name} CASCADE")
      
    def conclude_setup(self, validate=True):
        import time
        
        spark.conf.set("da.metadata_db_name", self.metadata_db_name)
#         for key in self.paths.__dict__:
#             spark.conf.set(f"da.paths.{key.lower()}", self.paths.__dict__[key])
        
#         print("\nPredefined Paths:")
#         DA.paths.print()

        if validate:
            print(f"\nPredefined tables in {self.metadata_db_name}:")
            tables = spark.sql(f"SHOW TABLES IN {self.metadata_db_name}").filter("isTemporary == false").select("tableName").collect()
            if len(tables) == 0: print("  -none-")
            for row in tables: print(f"  {row[0]}")
                
        print()
#         if validate: validate_datasets()
#         print(f"\nSetup completed in {int(time.time())-self.start} seconds")
