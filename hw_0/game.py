"""Игра угадай число
Компьютер сам загадывает и сам угадывает число
"""

import numpy as np



def random_predict(number: int = 1) -> int:
    """Просто угадываем на random, никак не используя информацию о больше или меньше.
       Функция принимает загаданное число и возвращает число попыток

    Args:
        number (int, optional): Загаданное число. Defaults to 1.

    Returns:
        int: Число попыток
    """
    count = 0

    while True:
        count += 1
        predict_number = np.random.randint(1, 101)  # предполагаемое число
        if number == predict_number:
            break  # выход из цикла если угадали
    
    return count



def game_core_v2(number: int = 1) -> int:
    """В этой функции мы используем подход "угадай и проверь".
    Сначала устанавливаем любое random число, а потом уменьшаем
    или увеличиваем его в зависимости от того, больше оно или меньше нужного.
    Функция принимает загаданное число и возвращает число попыток.
       
    Args:
        number (int, optional): Загаданное число. Defaults to 1.

    Returns:
        int: Число попыток
    """
    count = 0  # Инициализируем счетчик попыток
    predict = np.random.randint(1, 101)  # Предполагаемое число
    
    # Пока число не угадано, продолжаем цикл
    while number != predict:
        count += 1  # Увеличиваем счетчик попыток
        # Если загаданное число больше предполагаемого, то увеличиваем предполагаемое
        if number > predict:
            predict += 1
        # Если загаданное число меньше предполагаемого, то уменьшаем предполагаемое
        elif number < predict:
            predict -= 1

    return count  # Возвращаем количество попыток



def game_core_v3(number: int = 1) -> int:
    """
    Функция определяет, сколько попыток понадобится для нахождения заданного числа 
    в списке от 1 до 100 с использованием метода бинарного поиска. 
    
    Алгоритм работы:
    1. Формируется список от 1 до 100.
    2. Определяется центральный элемент списка.
    3. Если центральный элемент меньше заданного числа, то искомое число 
       находится в правой половине списка, иначе в левой.
    4. Определенная половина становится новым списком для поиска, 
       и процесс повторяется, пока число не будет найдено.
    
    Args:
        number (int, optional): Загаданное число в диапазоне от 1 до 100. 
                                По умолчанию равно 1.

    Returns:
        int: Число попыток, которое потребовалось для нахождения заданного числа.
    """
    # Формирование списка чисел от 1 до 100
    lst = list(range(1, 101))
    count = 0  # Счетчик попыток
    
    while True:
        count += 1  # Увеличиваем счетчик попыток
        
        # Определение центрального элемента списка
        y = lst[int(len(lst)/2)]
        
        # Если загаданное число меньше центрального, выбираем левую половину списка
        if number < y:
            lst = lst[:int(len(lst)/2)]
        # Если загаданное число больше центрального, выбираем правую половину списка
        elif number > y:
            lst = lst[int(len(lst)/2):]
        # Если число найдено, завершаем цикл
        else:
            break
            
    return count  # Возвращаем количество попыток



def score_game(random_predict) -> int:
    """Оценка эффективности алгоритма угадывания.
    
    Функция проводит 10000 испытаний алгоритма угадывания и вычисляет среднее количество попыток, 
    необходимых для успешного угадывания числа.

    Args:
        random_predict (function): Функция угадывания.

    Returns:
        int: Среднее количество попыток за 10000 испытаний.
    """
    count_ls = []  # Список для хранения количества попыток угадывания каждого числа
    
    # Генерация списка из 10000 случайных чисел в диапазоне от 1 до 100
    random_array = np.random.randint(1, 101, size=(10000))
    
    # Для каждого числа из списка определяем количество попыток, которое потребовалось для его угадывания
    for number in random_array:
        count_ls.append(random_predict(number))
    
    # Вычисляем и возвращаем среднее значение попыток
    score = int(np.mean(count_ls))
    return score


if __name__ == "__main__":
    # RUN
    score_game(random_predict)
