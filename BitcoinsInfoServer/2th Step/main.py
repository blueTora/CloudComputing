from flask import Flask
import redis
import json
import requests

app = Flask(__name__)
my_redis = redis.Redis(host='my-redis', port=6379)
# my_redis.flushdb()

file = open('config.json')
data = json.load(file)
print(my_redis.mset(data))

dic = dict()
dic['vs_currency'] = 'usd'


@app.route('/', methods=['get'])
def get_info():
    result = {}

    if my_redis.exists(my_redis.get('COIN')) == 0:
        dic['ids'] = my_redis.get('COIN').decode()
        resp = requests.get('https://api.coingecko.com/api/v3/coins/markets', params=dic).json()

        result['name'] = my_redis.get('COIN').decode()
        result['price'] = resp[0]['current_price']

        my_redis.set(my_redis.get('COIN'), resp[0]['current_price'], ex=int(my_redis.get('TIMER')))
        return json.dumps(result)

    else:
        result['name'] = my_redis.get('COIN').decode()
        print(my_redis.get('bitcoin'))
        result['price'] = my_redis.get(my_redis.get('COIN')).decode()

        return json.dumps(result)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=my_redis.get("PORT"))
