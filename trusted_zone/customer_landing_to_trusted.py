import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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
AmazonS3_node1769581584030 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udit-test-bucket-1/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1769581584030")

# Script generated for node PrivacyFilter
PrivacyFilter_node1769581702829 = Filter.apply(frame=AmazonS3_node1769581584030, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1769581702829")

# Script generated for node Customer Landing to Trusted
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1769581702829, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769581549008", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerLandingtoTrusted_node1769581748906 = glueContext.write_dynamic_frame.from_options(frame=PrivacyFilter_node1769581702829, connection_type="s3", format="glueparquet", connection_options={"path": "s3://udit-test-bucket-1/customer/trusted/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="CustomerLandingtoTrusted_node1769581748906")

job.commit()