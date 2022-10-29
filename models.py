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
    def add_login(self, email, username, password):

        try:
            self.table.put_item(
                Item={
                    'email': email,
                    'username': username,
                    'password': password})
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

    # Update Login item in database
    def update_login(self, email, username, password):
        try:
            response = self.table.update_item(
                Key={'email': email, 'username': username},
                UpdateExpression="set password=:p",
                ExpressionAttributeValues={
                    ':p': password},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update login %s in table %s. Here's why: %s: %s",
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
                    {'AttributeName': 'post_id', 'KeyType': 'RANGE'},  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'post_id', 'AttributeType': 'N'},
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
    def add_post(self, email, post_id, post_content):
        try:
            self.table.put_item(
                Item={
                    'email': email,
                    'post_id': int(post_id),
                    'post_content': post_content})
        except ClientError as err:
            logger.error(
                "Couldn't add post %s to table %s. Here's why: %s: %s",
                email, post_id, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Get Post item from database
    def get_post(self, email, post_id):
        try:
            response = self.table.get_item(Key={'email': email, 'post_id': int(post_id)})
        except ClientError as err:
            logger.error(
                "Couldn't get post %s from table %s. Here's why: %s: %s",
                email, post_id, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    # Update Post item in database
    def update_post(self, email, post_id, post_content):
        try:
            response = self.table.update_item(
                Key={'email': email, 'post_id': int(post_id)},
                UpdateExpression="set post_content=:p",
                ExpressionAttributeValues={
                    ':p': post_content},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update post %s in table %s. Here's why: %s: %s",
                email, post_id, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

    # Delete Post item from database
    def delete_post(self, email, post_id):
        try:
            self.table.delete_item(Key={'email': email, 'post_id': post_id})
        except ClientError as err:
            logger.error(
                "Couldn't delete post %s. Here's why: %s: %s", email, post_id,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Query Post item by username from database
    def query_post(self, email):
        try:
            response = self.table.query(KeyConditionExpression=Key('email').eq(email))
        except ClientError as err:
            logger.error(
                "Couldn't query for posts with email %s. Here's why: %s: %s", email,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

    # Scan Posts by submission time from database
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
