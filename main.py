from fastapi import FastAPI, HTTPException, Depends, File, UploadFile
from pydantic import BaseModel
import jwt
import datetime
import boto3
from sqlalchemy import create_engine
import pandas as pd
from fastapi.security import OAuth2PasswordBearer

app = FastAPI()

SECRET_KEY = "your_secret_key"
BUCKET_NAME = 'your_bucket_name'
SQLALCHEMY_DATABASE_URL = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
s3_client = boto3.client('s3')

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class LoginRequest(BaseModel):
    username: str
    password: str

@app.get("/")
def read_root():
    return {"message": "Hello World"}

@app.post("/login")
def login(request: LoginRequest):
    if request.username == "user" and request.password == "pass":
        expiration = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
        token = jwt.encode({"id_usuario": 1, "rol": "user", "exp": expiration}, SECRET_KEY, algorithm="HS256")
        return {"token": token}
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

def validate_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        if payload["rol"] != "admin":
            raise HTTPException(status_code=403, detail="Not enough permissions")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/upload", dependencies=[Depends(validate_token)])
async def upload_file(file: UploadFile = File(...)):
    contents = await file.read()
    df = pd.read_csv(contents)
    validations = []
    if df.isnull().values.any():
        validations.append("Valores vacíos encontrados")
    s3_client.put_object(Bucket=BUCKET_NAME, Key=file.filename, Body=contents)
    df.to_sql('table_name', engine, if_exists='append', index=False)
    return {"validations": validations}

@app.post("/renew")
def renew_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        expiration = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
        new_token = jwt.encode({"id_usuario": payload["id_usuario"], "rol": payload["rol"], "exp": expiration}, SECRET_KEY, algorithm="HS256")
        return {"token": new_token}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")