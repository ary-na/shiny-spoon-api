import boto3
from fastapi import FastAPI

from models import init_ss_logins

app = FastAPI()

ss_logins = init_ss_logins('ss-logins', boto3.resource('dynamodb', 'ap-southeast-2'))


# Create new login
@app.post('/api/add-login')
async def add_ss_login(user_name: str, email: str, password: str):
    ss_logins.add_login(user_name, email, password)


# Get login using username and email
@app.get("/api/{user_name}/{email}")
async def get_ss_login(user_name: str, email: str):
    return ss_logins.get_login(user_name, email)


# Update login
@app.put("/api/update-login")
async def update_ss_login(user_name: str, email: str, password: str):
    return ss_logins.update_login(user_name, email, password)


# Delete login
@app.delete('api/delete-login')
async def delete_ss_login(user_name: str, email: str):
    ss_logins.delete_login(user_name, email)


# Get login using user_name
@app.get("/api/{user_name}")
async def get_ss_login(user_name: str):
    return ss_logins.query_login(user_name)
