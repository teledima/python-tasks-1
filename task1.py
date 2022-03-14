import json
import pandas as pd
from multiprocessing import Pool

def get_data(filename: str):
    with open(filename, 'r') as data_file:
        data = json.load(data_file)
        return data

students = pd.read_json('data/students.json')
subjects = pd.read_json('data/subjects.json')
ratings = pd.read_json('data/ratings.json')


def top10_students_rating():
    mean_rating = ratings.groupby('student_id').mean()["rating"]
    return pd.concat([students, mean_rating], axis='columns', join='inner').sort_values('rating', ascending=False)[:10]


def top10_students_truancy():
    sum_truancy = ratings.groupby('student_id').sum()["truancy"]
    return pd.concat([students, sum_truancy], axis='columns', join='inner').sort_values('truancy', ascending=False)[:10]
    

def top10_subjects_rating():
    mean_rating = ratings.groupby('subject_id').mean()["rating"]
    return pd.concat([subjects, mean_rating], axis='columns', join='inner').sort_values('rating', ascending=True)[:10]
    

def top10_common(mode: int):
    if mode == 1:
        result = top10_students_rating()
    elif mode == 2:
        result = top10_students_truancy()
    elif mode == 3: 
        result = top10_subjects_rating()
    print(f'{result}\n')


while True:
    mode = int(input("1 - Однопоточное решение\n2 - Процессное решение\n3 - Выход\nВведите команду: "))
    if mode == 1:
        top10_common(1)
        top10_common(2)
        top10_common(3)
    elif mode == 2:
        with Pool(processes=3) as pool:
            pool.map(top10_common, (1, 2, 3))
    elif mode == 3:
        break
        
