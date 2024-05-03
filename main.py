import string

import pandas as pd
import concurrent.futures
import time
import random


def generate_data(num_rows, row_length):
    # Генерируем случайные данные в виде DataFrame
    data = {'text': [''.join(random.choices(string.ascii_letters + string.digits, k=row_length))
                     for _ in range(num_rows)]}
    df = pd.DataFrame(data)
    return df


def count_chars(data):
    # Функция для подсчета количества каждого символа в строке данных
    counts = [0] * 65536  # Максимальное значение Unicode
    for char in data:
        counts[ord(char)] += 1
    return counts


def process_data_in_threads(df, num_threads):
    bufsize = len(df) // num_threads  # Размер части данных для каждого потока
    tasks = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(num_threads):
            start_idx = i * bufsize
            end_idx = start_idx + bufsize if i < num_threads - 1 else len(df)
            task_data = df['text'].iloc[start_idx:end_idx].to_string(index=False)  # Преобразуем часть данных в строку
            tasks.append(executor.submit(count_chars, task_data))  # Запускаем задачу в потоке

    # Собираем результаты из потоков
    global_counts = [0] * 65536
    for task in tasks:
        local_counts = task.result()
        global_counts = [x + y for x, y in zip(global_counts, local_counts)]

    return global_counts


if __name__ == "__main__":
    num_rows = 10  # Количество строк данных
    row_length = 10  # Длина каждой строки данных
    num_threads = 4  # Количество потоков для обработки данных

    # Генерируем данные с помощью pandas
    input_df = generate_data(num_rows, row_length)

    # Обрабатываем данные в нескольких потоках
    start_time = time.perf_counter()
    result_counts = process_data_in_threads(input_df, num_threads)
    end_time = time.perf_counter()

    # Выводим результаты обработки
    print("Результаты подсчета количества символов:")
    for i, count in enumerate(result_counts):
        if count > 0:
            print(f"\"{chr(i)}\" - {count}")

    # Выводим время выполнения
    print(f"Время выполнения: {end_time - start_time:.5f} секунд")
