import boto3
from fastapi import FastAPI, UploadFile, File

from models import init_ss_logins, init_ss_posts, upload_img, generate_pre_signed_url, email_notification

app = FastAPI()

ss_logins = init_ss_logins('ss-logins', boto3.resource('dynamodb', 'ap-southeast-2'))
ss_posts = init_ss_posts('ss-posts', boto3.resource('dynamodb', 'ap-southeast-2'))
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda', region_name='ap-southeast-2')
bucket_name = 'shiny-spoon'

profile_images_folder = 'profile-images/'
post_images_folder = 'post-images/'


# Create new login
@app.post('/logins/add-login')
async def add_ss_login(email: str, username: str, password: str, img_key: str):
    ss_logins.add_login(email, username, password, img_key)
    email_notification(lambda_client, email, username)


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
@app.get('/posts/{email}/{date_time_utc}')
async def get_ss_post(email: str, date_time_utc: str, ):
    return ss_posts.get_post(email, date_time_utc)


# Update post
@app.put('/posts/update-post')
async def update_ss_post(email: str, date_time_utc: str, post_content: str):
    return ss_posts.update_post(email, date_time_utc, post_content)


# Delete post
@app.delete('/posts/delete-post')
async def delete_ss_post(email: str, date_time_utc: str, ):
    ss_posts.delete_post(email, date_time_utc)


# Get posts (Query)
@app.get('/posts')
async def get_ss_posts():
    return ss_posts.query_post()


# Get posts (scan)
@app.get('/scan-posts')
async def get_ss_post():
    return ss_posts.scan_posts()


# -----------------------------------------------------------------------------------------------

# Upload profile image
@app.post('/utilities/upload-profile-img')
async def upload_ss_profile_img(img_file: UploadFile, object_key: str):
    upload_img(s3_client, bucket_name, img_file, profile_images_folder, object_key)


# Upload post image
@app.post('/utilities/upload-post-img')
async def upload_ss_post_img(img_file: UploadFile, object_key: str):
    upload_img(s3_client, bucket_name, img_file, post_images_folder, object_key)


# Get pre-signed url profile image
@app.get("/utilities/profile-img/{object_key}")
async def get_pre_signed_url_profile_img(object_key: str):
    return generate_pre_signed_url(s3_client, bucket_name, profile_images_folder, object_key)


# Get pre-signed url post image
@app.get("/utilities/post-img/{object_key}")
async def get_pre_signed_url_post_img(object_key: str):
    return generate_pre_signed_url(s3_client, bucket_name, post_images_folder, object_key)
