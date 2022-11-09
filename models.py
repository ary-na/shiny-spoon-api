import datetime
import json
import logging

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# Shiny-Spoon Logins table
class SSLogins:

    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    # Returns true if table exists
    def exists(self, table_name):
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    # Create Logins table
    def create_table(self, table_name):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'email', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'username', 'KeyType': 'RANGE'},  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'username', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    # Add Login item to database
    def add_login(self, email, username, password, img_key):

        try:
            self.table.put_item(
                Item={
                    'email': email,
                    'username': username,
                    'password': password,
                    'img_key': img_key})
        except ClientError as err:
            logger.error(
                "Couldn't add login %s to table %s. Here's why: %s: %s",
                email, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Get Login item from database
    def get_login(self, email, username):
        try:
            response = self.table.get_item(Key={'email': email, 'username': username})
        except ClientError as err:
            logger.error(
                "Couldn't get login %s from table %s. Here's why: %s: %s",
                email, username, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    # Update Login password in database
    def update_login_password(self, email, username, password):
        try:
            response = self.table.update_item(
                Key={'email': email, 'username': username},
                UpdateExpression="set password=:p",
                ExpressionAttributeValues={
                    ':p': password},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update login password %s in table %s. Here's why: %s: %s",
                email, username, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

    # Update Login image key in database
    def update_login_profile_image(self, email, username, img_key):
        try:
            response = self.table.update_item(
                Key={'email': email, 'username': username},
                UpdateExpression="set img_key=:i",
                ExpressionAttributeValues={
                    ':i': img_key},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update login image key %s in table %s. Here's why: %s: %s",
                email, username, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

    # Delete Login item from database
    def delete_login(self, email, username):
        try:
            self.table.delete_item(Key={'email': email, 'username': username})
        except ClientError as err:
            logger.error(
                "Couldn't delete login %s. Here's why: %s: %s", email, username,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Query Login item by username from database
    def query_login(self, email):
        try:
            response = self.table.query(KeyConditionExpression=Key('email').eq(email))
        except ClientError as err:
            logger.error(
                "Couldn't query for login with username %s. Here's why: %s: %s", email,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']


# -----------------------------------------------------------------------------------------------

# Shiny Spoon Posts table
class SSPosts:

    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    # Returns true if table exists
    def exists(self, table_name):
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    # Create Posts table
    def create_table(self, table_name):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'email', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'date_time_utc', 'KeyType': 'RANGE'},  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'date_time_utc', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    # Add Post item to database
    def add_post(self, email, username, user_profile_img_key, description, post_img_key):
        try:
            self.table.put_item(
                Item={
                    'email': email,
                    'date_time_utc': str(datetime.datetime.utcnow()),
                    'username': username,
                    'user_profile_img_key': user_profile_img_key,
                    'description': description,
                    'post_img_key': post_img_key,
                    'active_state': 1})
        except ClientError as err:
            logger.error(
                "Couldn't add post %s to table %s. Here's why: %s: %s",
                email, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Get Post item from database
    def get_post(self, email, date_time_utc):
        try:
            response = self.table.get_item(Key={'email': email, 'date_time_utc': date_time_utc})
        except ClientError as err:
            logger.error(
                "Couldn't get post %s from table %s. Here's why: %s: %s",
                email, date_time_utc, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    # Update Post active state in database
    def update_post_active_state(self, email, date_time_utc):
        try:
            response = self.table.update_item(
                Key={'email': email, 'date_time_utc': date_time_utc},
                UpdateExpression="set active_state=:a",
                ExpressionAttributeValues={
                    ':a': 0},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update post %s in table %s. Here's why: %s: %s",
                email, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

    # Delete Post item from database
    def delete_post(self, email, date_time_utc):
        try:
            self.table.delete_item(Key={'email': email, 'date_time_utc': date_time_utc})
        except ClientError as err:
            logger.error(
                "Couldn't delete post %s. Here's why: %s: %s", email, date_time_utc,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Query Post items by email and sort by date-time-utc from database
    def query_user_post(self, email):
        try:
            response = self.table.query(KeyConditionExpression=Key('email').eq(email),
                                        ScanIndexForward=False,
                                        Limit=15
                                        )
        except ClientError as err:
            logger.error(
                "Couldn't query for posts %s. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

    # Query Post items by account state and sort by date-time-utc from database
    def query_post(self):
        try:
            response = self.table.query(IndexName='active_state_date_time_utc-index',
                                        KeyConditionExpression=Key('active_state').eq(1),
                                        ScanIndexForward=False,
                                        Limit=15
                                        )
        except ClientError as err:
            logger.error(
                "Couldn't query for posts %s. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

    # Scan Post items from database
    def scan_posts(self):
        try:
            response = self.table.scan()
        except ClientError as err:
            logger.error(
                "Couldn't scan for posts %s. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']


# -----------------------------------------------------------------------------------------------

# Initialize Shiny Spoon Logins table
def init_ss_logins(table_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    ss_logins = SSLogins(dyn_resource)
    ss_logins_exists = ss_logins.exists(table_name)
    if not ss_logins_exists:
        print(f"\nCreating table {table_name}...")
        ss_logins.create_table(table_name)
        print(f"\nCreated table {ss_logins.table.name}.")

    return ss_logins


# Initialize Shiny Spoon Posts table
def init_ss_posts(table_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    ss_posts = SSPosts(dyn_resource)
    ss_posts_exists = ss_posts.exists(table_name)
    if not ss_posts_exists:
        print(f"\nCreating table {table_name}...")
        ss_posts.create_table(table_name)
        print(f"\nCreated table {ss_posts.table.name}.")

    return ss_posts


# -----------------------------------------------------------------------------------------------

# Call aws lambda function
def email_notification(lambda_client, email, username):
    lambda_payload = {"user_email": email, "username": username}
    lambda_client.invoke(FunctionName='emailNotification',
                         InvocationType='RequestResponse',
                         Payload=json.dumps(lambda_payload))


# -----------------------------------------------------------------------------------------------

# Upload image
def upload_img(s3_client, bucket_name, img_file, folder_name, object_key):
    s3_client.upload_fileobj(img_file.file, bucket_name, folder_name + object_key,
                             ExtraArgs={'ContentType': img_file.content_type})


# Get pre-signed url
def generate_pre_signed_url(s3_client, bucket_name, folder_name, object_key):
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={'Bucket': bucket_name, 'Key': folder_name + object_key},
            ExpiresIn=3600
        )
        logger.info("Got pre-signed URL: %s", url)
    except ClientError:
        logger.exception(
            "Couldn't get a pre-signed URL for client method '%s'.")
        raise
    return url
