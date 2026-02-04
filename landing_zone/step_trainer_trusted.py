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
AmazonS3_node1769581584030 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://cd0030bucket/step_trainer/"], "recurse": True}, transformation_ctx="AmazonS3_node1769581584030")

# Script generated for node Customer Landing to Trusted
EvaluateDataQuality().process_rows(frame=AmazonS3_node1769581584030, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769581549008", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerLandingtoTrusted_node1769581748906 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1769581584030, connection_type="s3", format="glueparquet", connection_options={"path": "s3://udit-test-bucket-1/step_trainer/landing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="CustomerLandingtoTrusted_node1769581748906")
job.commit()