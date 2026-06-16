import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node airport_dimensions
airport_dimensions_node1781549434536 = glueContext.create_dynamic_frame.from_catalog(database="airport_dim_glue_db", table_name="airports_dim", transformation_ctx="airport_dimensions_node1781549434536")

# Script generated for node daily_flights
daily_flights_node1781549465312 = glueContext.create_dynamic_frame.from_catalog(database="daily_flights_glue_db", table_name="daily_flights", transformation_ctx="daily_flights_node1781549465312")

# Script generated for node Filter dep_delay>=60
Filterdep_delay60_node1781549489673 = Filter.apply(frame=daily_flights_node1781549465312, f=lambda row: (row["depdelay"] >= 60), transformation_ctx="Filterdep_delay60_node1781549489673")

# Script generated for node Left join for origin data
Filterdep_delay60_node1781549489673DF = Filterdep_delay60_node1781549489673.toDF()
airport_dimensions_node1781549434536DF = airport_dimensions_node1781549434536.toDF()
Leftjoinfororigindata_node1781549564801 = DynamicFrame.fromDF(Filterdep_delay60_node1781549489673DF.join(airport_dimensions_node1781549434536DF, (Filterdep_delay60_node1781549489673DF['originairportid'] == airport_dimensions_node1781549434536DF['airport_id']), "left"), glueContext, "Leftjoinfororigindata_node1781549564801")

# Script generated for node Change Schema
ChangeSchema_node1781549690454 = ApplyMapping.apply(frame=Leftjoinfororigindata_node1781549564801, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "long"), ("arrdelay", "long", "arr_delay", "long"), ("city", "string", "dep_city", "string"), ("state", "string", "dep_state", "string"), ("name", "string", "dep_airport", "string")], transformation_ctx="ChangeSchema_node1781549690454")

# Script generated for node Left Join for departure data
ChangeSchema_node1781549690454DF = ChangeSchema_node1781549690454.toDF()
airport_dimensions_node1781549434536DF = airport_dimensions_node1781549434536.toDF()
LeftJoinfordeparturedata_node1781550072493 = DynamicFrame.fromDF(ChangeSchema_node1781549690454DF.join(airport_dimensions_node1781549434536DF, (ChangeSchema_node1781549690454DF['destairportid'] == airport_dimensions_node1781549434536DF['airport_id']), "left"), glueContext, "LeftJoinfordeparturedata_node1781550072493")

# Script generated for node Change Schema
ChangeSchema_node1781550156763 = ApplyMapping.apply(frame=LeftJoinfordeparturedata_node1781550072493, mappings=[("carrier", "string", "carrier", "string"), ("dep_delay", "long", "dep_delay", "long"), ("arr_delay", "long", "arr_delay", "long"), ("dep_city", "string", "dep_city", "string"), ("dep_state", "string", "dep_state", "string"), ("dep_airport", "string", "dep_airport", "string"), ("city", "string", "arr_city", "string"), ("state", "string", "arr_state", "string"), ("name", "string", "arr_airport", "string")], transformation_ctx="ChangeSchema_node1781550156763")

# Script generated for node Amazon Redshift
AmazonRedshift_node1781607482490 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1781550156763, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://airlines-landing-data-proj/temp_files/", "useConnectionProperties": "true", "dbtable": "\"airlines\".\"daily_flight_facts\"", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS \"airlines\".\"daily_flight_facts\" (\"carrier\" VARCHAR, \"dep_delay\" BIGINT, \"arr_delay\" BIGINT, \"dep_city\" VARCHAR, \"dep_state\" VARCHAR, \"dep_airport\" VARCHAR, \"arr_city\" VARCHAR, \"arr_state\" VARCHAR, \"arr_airport\" VARCHAR);"}, transformation_ctx="AmazonRedshift_node1781607482490")

job.commit()
