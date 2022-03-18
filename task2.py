import json
import pandas as pd
import time
from threading import Timer
from multiprocessing import Pool


pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 500)


def get_data(filename: str):
    with open(filename, 'r') as data_file:
        data = json.load(data_file)
        return data


cars = pd.read_json('data-2/cars.json')
sales = pd.read_json('data-2/sales.json')
sellers = pd.read_json('data-2/sellers.json')
spares = pd.read_json('data-2/spares.json')


def top_car_rating():
    max_car_sell = sales.groupby('car_id').count()['seller_id'].sort_values(ascending=False)
    max_car_sell.name = 'count'
    return pd.concat([max_car_sell.head(1), cars], axis='columns', join='inner')


def top5_spares():
    return spares.sort_values('price', ascending=False).head(5)


def top_seller():
    revenue = pd.merge(left=sales, right=spares, left_on='spare_id', right_index=True, how='left')
    revenue = pd.merge(left=revenue, right=cars, left_on='car_id', right_index=True, how='left', suffixes=('_spare', '_car'))
    revenue['price'] = revenue['price_spare'].fillna(0) + revenue['price_car'].fillna(0)
    revenue.drop(columns=['price_spare', 'price_car', 'name_spare', 'name_car'], inplace=True)
    revenue_seller = pd.concat([revenue.groupby('seller_id').sum()['price'], sellers], axis='columns')
    return revenue_seller.sort_values(by='price', ascending=False).head(1)


def top_common(mode: int):
    if mode == 1:
        result = top_car_rating()
    elif mode == 2:
        result = top5_spares()
    elif mode == 3:
        result = top_seller()
    print(f'{result}\n')


if __name__ == "__main__":
    while True:
        command = int(input("1 - События таймера\n2 - Процессное решение\n3 - Выход\nВведите команду: "))
        if command == 1:
            for i in range(1, 4):
                thread = Timer(interval=5, function=top_common, args=[i])
                thread.start()
            time.sleep(10)
        elif command == 2:
            with Pool(processes=3) as pool:
                pool.map(top_common, [1, 2, 3])
        elif command == 3:
            break
        else:
            print('Неверная команда')
