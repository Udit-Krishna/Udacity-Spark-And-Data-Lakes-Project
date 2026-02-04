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
AmazonS3_node1770013264742 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://udit-test-bucket-1/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1770013264742")

# Script generated for node Amazon S3
AmazonS3_node1770013262761 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udit-test-bucket-1/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1770013262761")

# Script generated for node Join
Join_node1770013329753 = Join.apply(frame1=AmazonS3_node1770013264742, frame2=AmazonS3_node1770013262761, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1770013329753")

# Script generated for node Drop Fields
DropFields_node1770013372645 = DropFields.apply(frame=Join_node1770013329753, paths=["serialnumber", "sharewithpublicasofdate", "birthday", "registrationdate", "sharewithresearchasofdate", "customername", "sharewithfriendsasofdate", "lastupdatedate", "phone", "lastUpdateDate", "email", "shareWithFriendsAsOfDate", "customerName", "shareWithResearchAsOfDate", "registrationDate", "birthDay", "shareWithPublicAsOfDate", "serialNumber"], transformation_ctx="DropFields_node1770013372645")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1770013372645, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770013203492", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1770013471136 = glueContext.getSink(path="s3://udit-test-bucket-1/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1770013471136")
AmazonS3_node1770013471136.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1770013471136.setFormat("json")
AmazonS3_node1770013471136.writeFrame(DropFields_node1770013372645)
job.commit()