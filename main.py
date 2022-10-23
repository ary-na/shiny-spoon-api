import boto3
from fastapi import FastAPI

from models import init_ss_logins, init_ss_posts

app = FastAPI()

ss_logins = init_ss_logins('ss-logins', boto3.resource('dynamodb', 'ap-southeast-2'))
ss_posts = init_ss_posts('ss-posts', boto3.resource('dynamodb', 'ap-southeast-2'))


# Create new login
@app.post('/logins/add-login')
async def add_ss_login(user_name: str, email: str, password: str):
    ss_logins.add_login(user_name, email, password)


# Get login using username and email
@app.get("/logins/{user_name}/{email}")
async def get_ss_login(user_name: str, email: str):
    return ss_logins.get_login(user_name, email)


# Update login
@app.put("/logins/update-login")
async def update_ss_login(user_name: str, email: str, password: str):
    return ss_logins.update_login(user_name, email, password)


# Delete login
@app.delete('/logins/delete-login')
async def delete_ss_login(user_name: str, email: str):
    ss_logins.delete_login(user_name, email)


# Get login using user_name
@app.get("/logins/{user_name}")
async def get_ss_login(user_name: str):
    return ss_logins.query_login(user_name)


# -----------------------------------------------------------------------------------------------

# Create new post
@app.post('/posts/add-post')
async def add_ss_post(user_name: str, post_id: int, post_content: str):
    ss_posts.add_post(user_name, post_id, post_content)


# Get post using username and post id
@app.get("/posts/{user_name}/{post_id}")
async def get_ss_post(user_name: str, post_id: int):
    return ss_posts.get_post(user_name, post_id)


# Update post
@app.put("/posts/update-post")
async def update_ss_post(user_name: str, post_id: int, post_content: str):
    return ss_posts.update_post(user_name, post_id, post_content)


# Delete post
@app.delete('/posts/delete-post')
async def delete_ss_post(user_name: str, post_id: int):
    ss_posts.delete_post(user_name, post_id)


# Get post using user_name
@app.get("/posts/{user_name}")
async def get_ss_post(user_name: str):
    return ss_posts.query_post(user_name)


# Get posts
@app.get("/posts")
async def get_ss_posts():
    return ss_posts.scan_posts()
