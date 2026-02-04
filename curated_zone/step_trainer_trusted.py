import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1770013264742 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="AmazonS3_node1770013264742")

# Script generated for node Amazon S3
AmazonS3_node1770013262761 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landinglanding", transformation_ctx="AmazonS3_node1770013262761")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1770178945174 = ApplyMapping.apply(frame=AmazonS3_node1770013262761, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1770178945174")

# Script generated for node Join
Join_node1770013329753 = Join.apply(frame1=AmazonS3_node1770013264742, frame2=RenamedkeysforJoin_node1770178945174, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1770013329753")

# Script generated for node Drop Fields
DropFields_node1770013372645 = DropFields.apply(frame=Join_node1770013329753, paths=["timestamp", "registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "sharewithresearchasofdate", "phone"], transformation_ctx="DropFields_node1770013372645")

# Script generated for node Change Schema
ChangeSchema_node1770179318905 = ApplyMapping.apply(frame=DropFields_node1770013372645, mappings=[("right_distancefromobject", "int", "distancefromobject", "int"), ("right_sensorreadingtime", "long", "sensorreadingtime", "long"), ("right_serialnumber", "string", "serialnumber", "string")], transformation_ctx="ChangeSchema_node1770179318905")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1770179318905, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770013203492", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1770013471136 = glueContext.getSink(path="s3://udit-test-bucket-1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1770013471136")
AmazonS3_node1770013471136.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1770013471136.setFormat("json")
AmazonS3_node1770013471136.writeFrame(ChangeSchema_node1770179318905)
job.commit()