from fastapi import FastAPI

from utils import *
from pydantic import BaseModel

#простейшая модель для группы ссылок
class Links(BaseModel):
    links: str

app = FastAPI()

#ручка для получения 10 последних твитов
@app.get("/api/tweets/{twitter_id}")
async def last_10_tweets(twitter_id : int):
    return get_last_10_tweets(twitter_id)

#ручка для получения инфы о юзере по юзернейму
@app.get("/api/user/{username}")
async def get_user_info_via_username(username : str):
    return find_user_via_username(username=username)

@app.get("/api/userid/{userid}")
async def get_user_info_via_userid(userid : str):
    return find_user_via_userid(userid=userid)

#ручка для отправки списка пользователей
@app.post("/api/add_users")
async def add_users_to_parse(links: Links):
    return insert_new_users(links.links)