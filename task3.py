import pandas as pd
import pickle
import time
from threading import Thread

with open('data-3.pickle', 'rb') as file:
    data = pickle.load(file)
    sales = pd.DataFrame.from_records(data, columns=["Type", "Model", "Price", "Seller"])


def top_laptop():
    laptop_sales = sales.where(sales['Type'] == "Laptop").dropna()
    return laptop_sales.groupby('Model').count()['Type'].sort_values(ascending=False).head(1)


def top5_video_cards():
    video_cards_sales = sales.where(sales['Type'] == "Video Card").dropna()
    return video_cards_sales.sort_values('Price', ascending=False)[['Model', 'Price']].head(5)


def top_seller():
    return sales.groupby('Seller').sum()['Price'].sort_values(ascending=False).head(1)


def top_common(mode: int):
    if mode == 1:
        result = top_laptop()
    elif mode == 2:
        result = top5_video_cards()
    elif mode == 3:
        result = top_seller()
    print(f'{result}\n')


if __name__ == "__main__":
    while True:
        command = int(input("1 - Решение с потоками\n2 - Решение без потоков\n3 - Выход\nВведите команду: "))
        if command == 1:
            for i in range(1, 4):
                thread = Thread(target=top_common, args=[i])
                thread.start()
            time.sleep(5)
        elif command == 2:
            for i in range(1, 4):
                top_common(i)
        elif command == 3:
            break
        else:
            print('Неверная команда')
