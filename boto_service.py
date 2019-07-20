# -*- coding:utf-8 -*-

import boto3
AWS_ACCESS_KEY_ID = 'xxxxxxxxxxxxxx'
AWS_SECRET_ACCESS_KEY = 'xxxxxxxxxxxxxxxxxxxx'
AWS_REGION_NAME ='xxxxxxx'
AWS_S3_BUCKET_NAME = 'xxxxxx'

#import logger

from botocore.exceptions import ClientError
import botocore



class S3Connect:

    def __init__(self):
        self.s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        self.s3_client = boto3.client('s3', region_name=AWS_REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    def bucket_exists(self, bucket_name):
        """Determine whether bucket_name exists and the user has permission to access it
        :param bucket_name: string
        :return: True if the referenced bucket_name exists, otherwise False
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            ##logger.info('{} exists and you have permission to access it.'.format(bucket_name))
        except ClientError as e:
            ##logger.debug(e)
            return False
        return True

    def create_bucket(self, bucket_name, region=AWS_REGION_NAME):
        """Create an S3 bucket in a specified region
        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).
        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        except ClientError as e:
            ##logger.error(e)
            return False
        return True

    def delete_bucket(self, bucket_name):
        """Delete an empty S3 bucket
        If the bucket is not empty, the operation fails.
        :param bucket_name: string
        :return: True if the referenced bucket was deleted, otherwise False
        """
        try:
            self.s3_client.delete_bucket(Bucket=bucket_name)
        except ClientError as e:
            ##logger.error(e)
            return False
        return True

    def put_object(self, dest_bucket_name, dest_object_name, object_data,content_type=None):
        """Add an object to an Amazon S3 bucket
        The src_data argument must be of type bytes or a string that references
        a file specification.
        :param dest_bucket_name: string
        :param dest_object_name: string
        :param src_data: bytes of data or string reference to file spec
        :return: True if src_data was added to dest_bucket/dest_object, otherwise
        False
        """
        try:
            self.s3_client.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data,ContentType=content_type)
            ##logger.info('{} upload success'.format(dest_object_name))
        except ClientError as e:
            ##logger.error('{} upload failed , error is'.format(dest_object_name, str(e)))
            return False
        return True

    def upload_file(self, file_name, bucket, object_name):
        """Upload a file to an S3 bucket
            :param file_name: File to upload
            :param bucket: Bucket to upload to
            :param object_name: S3 object name. If not specified then same as file_name
            :return: True if file was uploaded, else False
            """
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name
        try:
            self.s3_client.upload_file(file_name, bucket, object_name)
            ##logger.info('{} 上传成功'.format(object_name))
        except ClientError as e:
            #logger.error(e)
            return False
        return True

    def download_file(self, bucket_name, object_name, file_name):
        try:
            self.s3_resource.Bucket(bucket_name).download_file(object_name, file_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                #logger.error("The object does not exist.")
                pass
            else:
                raise
            return False
        return True

    def delete_object(self, bucket_name, object_name):
        """Delete an object from an S3 bucket
        :param bucket_name: string
        :param object_name: string
        :return: True if the referenced object was deleted, otherwise False
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_name)
        except ClientError as e:
            #logger.error(e)
            return False
        return True

    def delete_objects(self, bucket_name, object_names):
        """Delete multiple objects from an Amazon S3 bucket
        :param bucket_name: string
        :param object_names: list of strings
        :return: True if the referenced objects were deleted, otherwise False
        """
        # Convert list of object names to appropriate data format
        objlist = [{'Key': obj} for obj in object_names]
        try:
            self.s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objlist})
        except ClientError as e:
            #logger.error(e)
            return False
        return True

    def list_bucket_objects(self, bucket_name):
        """List the objects in an Amazon S3 bucket
        :param bucket_name: string
        :return: List of bucket objects. If error, return None.
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
        except ClientError as e:
            # AllAccessDisabled error == bucket not found
            #logger.error(e)
            return None
        return response['Contents']

#AWS_S3_BUCKET_NAME = 'udaan-listing'
#object_data=open('/home/star01/2.png', 'rb')
if __name__ == '__main__':
    src='/home/star01/1.png'
    object_data = open(src, 'rb').read()
    s=S3Connect()
    s.put_object(AWS_S3_BUCKET_NAME,'tt.png',object_data,content_type='image/png')
    #s3官方文档有解释Content Type

