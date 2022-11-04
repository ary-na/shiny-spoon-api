import boto3
from fastapi import FastAPI, UploadFile, File

from models import init_ss_logins, init_ss_posts, upload_img, generate_pre_signed_url

app = FastAPI()

ss_logins = init_ss_logins('ss-logins', boto3.resource('dynamodb', 'ap-southeast-2'))
ss_posts = init_ss_posts('ss-posts', boto3.resource('dynamodb', 'ap-southeast-2'))
s3_client = boto3.client('s3')
bucket_name = 'shiny-spoon'


# Create new login
@app.post('/logins/add-login')
async def add_ss_login(email: str, username: str, password: str):
    ss_logins.add_login(email, username, password)


# Get login using username and email
@app.get("/logins/{email}/{username}")
async def get_ss_login(email: str, username: str):
    return ss_logins.get_login(email, username)


# Update login
@app.put("/logins/update-login")
async def update_ss_login(email: str, username: str, password: str):
    return ss_logins.update_login(email, username, password)


# Delete login
@app.delete('/logins/delete-login')
async def delete_ss_login(email: str, username: str):
    ss_logins.delete_login(email, username)


# Get login using user_name
@app.get("/logins/{email}")
async def get_ss_login(email: str):
    return ss_logins.query_login(email)


# -----------------------------------------------------------------------------------------------

# Create new post
@app.post('/posts/add-post')
async def add_ss_post(email: str, description: str, post_img_key: str):
    ss_posts.add_post(email, description, post_img_key)


# Get post using date time utc and email
@app.get('/posts/{date_time_utc}/{email}')
async def get_ss_post(date_time_utc: str, email: int):
    return ss_posts.get_post(date_time_utc, email)


# Update post
@app.put('/posts/update-post')
async def update_ss_post(date_time_utc: str, email: str, post_content: str):
    return ss_posts.update_post(date_time_utc, email, post_content)


# Delete post
@app.delete('/posts/delete-post')
async def delete_ss_post(date_time_utc: str, email: str):
    ss_posts.delete_post(date_time_utc, email)


# Get posts (Query)
@app.get('/posts')
async def get_ss_posts():
    return ss_posts.query_post()


# Get posts (scan)
@app.get('/scan-posts')
async def get_ss_post():
    return ss_posts.scan_posts()


# -----------------------------------------------------------------------------------------------

# Upload Image
@app.post('/utilities/upload-img')
async def upload_ss_img(img_file: UploadFile, folder_name: str, object_key: str):
    upload_img(s3_client, bucket_name, img_file, folder_name, object_key)


# Get pre-signed url
@app.get('utilities/{object-key}')
async def get_pre_signed_url(object_key: str):
    return generate_pre_signed_url(s3_client, bucket_name, object_key)
