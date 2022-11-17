from datetime import datetime
from multiprocessing import Manager, Pool
from time import time

import pdb
from pprint import pprint
import requests
from pymongo import MongoClient, UpdateOne

from local_configs import BARE_TOKEN, mongo_url

#подключаемся к базе
mongo = MongoClient(f"{mongo_url}")
db = mongo["twitterdb"]
users = db["users"]

#создаем сессию, чтобы при каждом хапросе не передавать токен явно
session = requests.session()
session.headers = {"Authorization": f"Bearer {BARE_TOKEN}"}  # type: ignore

def get_user_unfo(usernames, list, updated_list):

    """Функция забирает основую информациб с аккаунтов (такую как 'id','username','description','name','followers_count', 'following_count','update_datetime' -- дата последнего обнавления,"twitter_link".

    Твиттер может отдать только данные о 100 аккаунтах разом, поэтому мы делим список по 100 юзернеймов

    Args:
        usernames (str): строка из юзернеймов через запятую на поис
        list (manager): список, шарющий память между потоками, чтобы пультипроцессоринг нрмально отработал
    """
    
    print(f"Sending Req {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}")
    raw_data = session.get(f'https://api.twitter.com/2/users/by?usernames={usernames}&user.fields=description,id,name,username,public_metrics').json()
    if 'data' in raw_data.keys():
        for user in raw_data['data']:
            list.append(UpdateOne({'_id': user['id']},{ "$set":{
                '_id': user['id'],
                'username': user['username'],
                'description': user['description'],
                'name': user['name'],
                'followers_count': user['public_metrics']['followers_count'],
                "following_count" : user['public_metrics']['following_count'],
                "update_datetime" : int(time()),
                "twitter_link": f"https://twitter.com/{user['username']}"
            }}))
            updated_list.append(user['username'])

def clear_username(links: str, manager_list, updated_list):

    """_summary_

    Args:
        links (str): тут строка линков на твиттер ввиде строчки с разделителем через новую строку
        manager_list (manager): список, шарющий память между потоками, чтобы пультипроцессоринг нрмально отработал

    Returns:
        list: возращает список со 100 юзернеймами и списком для мультипроцессеринга [[100_usernames_str,list], [100_usernames_str,list], ...]
    """

    usernames = links.replace('https://twitter.com/', '').strip().split('\n')
    n = 100 #how much in one list
    return  usernames, [[','.join(usernames[i * n:(i + 1) * n]), manager_list, updated_list] for i in range((len(usernames) + n - 1) // n )]


def find_user_via_username(username: str):

    """нужно для поиска юзера по юзернейму в базе и если его нет, то искать в твиттере, добавлять в базу и возвращать пользователю

    Returns:
        list: возвращает или ошибку
    """

    user = users.find_one({"username": username})
    if user:
        return user
    print('Getting data')
    raw_data = session.get(f'https://api.twitter.com/2/users/by/username/{username}?user.fields=description,id,public_metrics,username').json()
    if 'data' in raw_data.keys():
        result_dict = {
            '_id': raw_data['data']['id'],
            'username': raw_data['data']['username'],
            'description': raw_data['data']['description'],
            'name': raw_data['data']['name'],
            'followers_count': raw_data['data']['public_metrics']['followers_count'],
            "following_count" : raw_data['data']['public_metrics']['following_count'],
            "update_datetime" : int(time()),
            "twitter_link": f"https://twitter.com/{raw_data['data']['username']}"
        }
        users.insert_one(result_dict)
        return result_dict

    return [{"Error": "Wrong Username"}]


def find_user_via_userid(userid: str):

    """нужно для поиска юзера по айди в базе и если его нет, то искать в твиттере, добавлять в базу и возвращать пользователю

    Returns:
        list: возвращает или ошибку
    """

    user = users.find_one({"_id": userid})
    if user:
        return user
    print('Getting data')
    raw_data = session.get(f'https://api.twitter.com/2/users/{userid}?user.fields=description,id,public_metrics,username').json()

    if 'data' in raw_data.keys():
        result_dict = {
            '_id': raw_data['data']['id'],
            'username': raw_data['data']['username'],
            'description': raw_data['data']['description'],
            'name': raw_data['data']['name'],
            'followers_count': raw_data['data']['public_metrics']['followers_count'],
            "following_count" : raw_data['data']['public_metrics']['following_count'],
            "update_datetime" : int(time()),
            "twitter_link": f"https://twitter.com/{raw_data['data']['username']}"
        }
        users.insert_one(result_dict)
        return result_dict

    return [{"Error": "Wrong UserID"}]

def insert_new_users(links):

    """Функция нужна для добавления и обновления юзеров в нашей базе. Процесс работает яерез простой мультипроцессеринг разделяя каждые 100 юзернеймов на 1 поток (по дефолту 5 потоков, т.к. в тестовом было 500 юзернеймов, можно сделать динамически настраемым чтобы секономить немного оперативки)

    Returns:
        list: Простая заглушка
    """
    if links:
        manager = Manager()
        update_list = manager.list()
        updated_list = manager.list()

        new_user_names, one_list = clear_username(links, update_list, updated_list)

        pool = Pool(processes=5)
        pool.starmap(get_user_unfo, one_list)
        pool.close()

        update_list = list(update_list)
        updated_list = list(updated_list)
        if update_list:
            users.bulk_write(update_list,ordered=False)

            return [{"Added": {"usernames":updated_list, "count": len(updated_list)}}, {"Error Users": {"usernames": list(set(new_user_names) - set(updated_list)), "count": len(list(set(new_user_names) - set(updated_list)))}}]
        return {"Error": "All usernames, which were provided are not exist or suspended"}
    return {"Error":"Bad Input"}

def get_last_10_tweets(twitter_id):

    """Функция возвращает последние 10 твитов (закрепленный твит игнорируется), если id пользователя указан правильно, если нет, то выкидывает ошибку

    Returns:
        list: или список твитов+tweet_id или ошибку если user_id не верный
    """

    result = list()
    raw_data = session.get(f'https://api.twitter.com/2/users/{twitter_id}/tweets?max_results=10&tweet.fields=text').json()
    if 'data' in raw_data.keys():
        for tweet in raw_data['data']:
            result.append({
            "_id": tweet['id'],
            'text': tweet['text']
            })
        return result
    return [{"Error": "Wrong ID"}]