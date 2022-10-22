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
                    {'AttributeName': 'user_name', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'email', 'KeyType': 'RANGE'},  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'user_name', 'AttributeType': 'S'},
                    {'AttributeName': 'email', 'AttributeType': 'S'},
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

    # Add login item to database
    def add_login(self, user_name, email, password):
        try:
            self.table.put_item(
                Item={
                    'user_name': user_name,
                    'email': email,
                    'password': password})
        except ClientError as err:
            logger.error(
                "Couldn't add login %s to table %s. Here's why: %s: %s",
                email, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Get login item from database
    def get_login(self, user_name, email):
        try:
            response = self.table.get_item(Key={'user_name': user_name, 'email': email})
        except ClientError as err:
            logger.error(
                "Couldn't get login %s from table %s. Here's why: %s: %s",
                user_name, email, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    # Update login item in database
    def update_login(self, user_name, email, password):
        try:
            response = self.table.update_item(
                Key={'user_name': user_name, 'email': email},
                UpdateExpression="set password=:r",
                ExpressionAttributeValues={
                    ':r': password},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update login %s in table %s. Here's why: %s: %s",
                user_name, email, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

    # Delete login item from database
    def delete_login(self, user_name, email):
        try:
            self.table.delete_item(Key={'user_name': user_name, 'email': email})
        except ClientError as err:
            logger.error(
                "Couldn't delete login %s. Here's why: %s: %s", user_name, email,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    # Query login item by username from database
    def query_login(self, user_name):
        try:
            response = self.table.query(KeyConditionExpression=Key('user_name').eq(user_name))
        except ClientError as err:
            logger.error(
                "Couldn't query for login with username %s. Here's why: %s: %s", user_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']


# -----------------------------------------------------------------------------------------------

# Initialize Shiny Spoon table
def init_ss_logins(table_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    ss_logins = SSLogins(dyn_resource)
    ss_logins_exists = ss_logins.exists(table_name)
    if not ss_logins_exists:
        print(f"\nCreating table {table_name}...")
        ss_logins.create_table(table_name)
        print(f"\nCreated table {ss_logins.table.name}.")

    return ss_logins
