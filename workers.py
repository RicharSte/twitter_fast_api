import argparse
import time

from pymongo import MongoClient

from local_configs import mongo_url
from utils import *

mongo = MongoClient(f"{mongo_url}")
db = mongo["twitterdb"]
users = db["users"]

def update_record():

    """Функция берет из базы аккаунты, которые не обновлялись 15 и более минут и отправляет их на обновление
    """

    links_to_update = []
    data = users.find({'update_datetime': {"$lt": int(time.time()) - 900}})
    for user in data:
        links_to_update.append(user['twitter_link'])
    update_string = '\n'.join(links_to_update)
    insert_new_users(update_string)

if __name__ == '__main__':

    """Данный скрипт нужен для выполнение каких-то либо рутинных и повторяющихся задач. Как например, актуализация профилей пользователей
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--update_users", help="update all user DB every 15 minutes", default=False)
    args = parser.parse_args()

    if args.update_users:
        while True:
            update_record()
            time.sleep(900)
