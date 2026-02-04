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
AmazonS3_node1770013264742 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="AmazonS3_node1770013264742")

# Script generated for node Amazon S3
AmazonS3_node1770013262761 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1770013262761")

# Script generated for node Join
Join_node1770013329753 = Join.apply(frame1=AmazonS3_node1770013264742, frame2=AmazonS3_node1770013262761, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1770013329753")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1770013329753, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770013203492", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1770013471136 = glueContext.getSink(path="s3://udit-test-bucket-1/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1770013471136")
AmazonS3_node1770013471136.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1770013471136.setFormat("json")
AmazonS3_node1770013471136.writeFrame(Join_node1770013329753)
job.commit()