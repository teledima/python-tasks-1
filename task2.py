import json
import pandas as pd
from multiprocessing import Pool

def get_data(filename: str):
    with open(filename, 'r') as data_file:
        data = json.load(data_file)
        return data

cars = pd.read_json('data-2/cars.json')
sales = pd.read_json('data-2/sales.json')
sellers = pd.read_json('data-2/sellers.json')
spares = pd.read_json('data-2/spares.json')


def top_car_rating():
    max_car_sell = sales.groupby('car_id').count()['seller_id'].sort_values()
    return mean_rating['seller_id']


def top5_students_truancy():
    sum_truancy = ratings.groupby('student_id').sum()["truancy"]
    return pd.concat([students, sum_truancy], axis='columns', join='inner').sort_values('truancy', ascending=False)[:10]
    

def top5_subjects_rating():
    mean_rating = ratings.groupby('subject_id').mean()["rating"]
    return pd.concat([subjects, mean_rating], axis='columns', join='inner').sort_values('rating', ascending=True)[:10]

if __name__ == "__main__":
    while True:
        mode = int(input("1 - События таймера\n2 - Процессное решение\n3 - Выход\nВведите команду: "))
        if mode == 1:
            print(top_car_rating())
            # top10_common(2)
            # top10_common(3)
        elif mode == 2:
            pass
        elif mode == 3:
            break
        
