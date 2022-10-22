import boto3
from fastapi import FastAPI

from models import init_ss_logins

app = FastAPI()

ss_logins = init_ss_logins('ss-logins', boto3.resource('dynamodb', 'ap-southeast-2'))


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
