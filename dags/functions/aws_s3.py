from airflow.models import Variable

AWS_KEY = Variable.get("aws_access_key_id")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key")

def create_bucket(bucket_name, region=None):
    try:
        s3 = boto3.resource('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

        if region is None:
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
    except ClientError as e:
        logging.error(e)
        return False
    return True

def store_rates_file(bucket, file_complete_path, file_name, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)
    
    s3 = boto3.resource('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    s3.Bucket(bucket).upload_file(file_complete_path, file_name)