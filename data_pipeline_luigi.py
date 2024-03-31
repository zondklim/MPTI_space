import luigi 
from luigi.util import requires
import os
import requests
import gzip
import io
import tarfile
import pandas as pd

""" 
Данный файл исполняется командой: python -m luigi_test_full FinalReportTask --url 'https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file' --local-scheduler
где https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file - ссылка на исходный файл из задания
"""

class DataDownloader(luigi.Task):
    # Параметр URL для скачивания данных
    url = luigi.Parameter()
    # Текущий рабочий каталог
    cwd = os.getcwd()
    
    # Генерация имени файла из URL
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())
    
    # Определение выходного файла
    def output(self):
        return luigi.LocalTarget(f'{self.cwd}/data/{self.name()}.tar')
    
    # Загрузка файла по URL
    def run(self):
        content = requests.get(self.url)
        # Проверка статуса запроса и запись содержимого
        if content.status_code == 200:
            os.makedirs(f'{self.cwd}/data', exist_ok=True)
            with open(f'{self.cwd}/data/{self.name()}.tar', 'wb') as f:
                f.write(content.content)
        else:
            raise FileExistsError('URL download is wrong')
        
class TarExtracter(luigi.Task):
    url = luigi.Parameter()
    
    def requires(self):
        return DataDownloader(self.url)
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'TarExtracter_result_{self.name()}.txt'))
        
    def members(self):
        tar_path = self.input().path
        member_list = []
        with tarfile.open(f'{tar_path}', 'r:*') as tar:
            for member in tar.getmembers():
                member_list.append(member.name)
        return member_list
    
    def dir_name(self, name):
        return os.path.join(os.getcwd(), 'data', name)
    
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())
    
    def run(self):
        with tarfile.open(f'{self.input().path}', 'r:*') as tar:
            for member in tar.getmembers():
                file_gz = tar.extractfile(member)
                name = member.name
            
                with open(f'{self.dir_name(name)}', 'wb') as f:
                    f.write(file_gz.read())
                with open(self.output().path, 'a') as exit_file:
                    exit_file.write(f'\n{self.dir_name(name)}')             
       
class TarExtracter(luigi.Task):
    # Параметр URL
    url = luigi.Parameter()
    
    # Зависимость от предыдущей задачи
    def requires(self):
        return DataDownloader(self.url)
    
    # Определение выходного файла
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'TarExtracter_result_{self.name()}.txt'))
        
    # Список файлов в архиве
    def members(self):
        tar_path = self.input().path
        member_list = []
        with tarfile.open(tar_path, 'r:*') as tar:
            for member in tar.getmembers():
                member_list.append(member.name)
        return member_list
    
    # Создание директории для файлов
    def dir_name(self, name):
        return os.path.join(os.getcwd(), 'data', name)
    
    # Генерация имени файла из URL
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())
    
    # Распаковка архива и запись файлов
    def run(self):
        with tarfile.open(self.input().path, 'r:*') as tar:
            for member in tar.getmembers():
                file_gz = tar.extractfile(member)
                name = member.name
            
                with open(self.dir_name(name), 'wb') as f:
                    f.write(file_gz.read())
                with open(self.output().path, 'a') as exit_file:
                    exit_file.write(f'\n{self.dir_name(name)}')
                      
class DataUnpacking(luigi.Task):
    # Параметр URL
    url = luigi.Parameter()
    
    # Зависимость от предыдущей задачи
    def requires(self):
        return TarExtracter(self.url)
    
    # опрелелим функцию для проверки выхода
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'DataUnpacking_result_{self.name()}.txt'))
        
    # функция для получения имени файла
    def f_name(self, file_path):
        return file_path.split('/')[-1].split('.')[0]
    
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())
    
     # функция для получения директории файла
    def dir_name(self, file_path):
        cwd = os.getcwd()
        return os.path.join(cwd, 'data', self.f_name(file_path)) 
    
    # Разархивация и создание текстовых файлов с данными
    def run(self):
        path_list = []
        with open(self.input().path, 'r') as f:
            for l in f.readlines():
                if l != '\n':
                    path_list.append(l.replace('\n', ''))
        
        # Обработка каждого файла
        for file_path in path_list:
            os.makedirs(self.dir_name(file_path), exist_ok=True)
            with gzip.open(file_path, 'rb') as gzip_file: 
                decompressed_data = gzip_file.read()
                
                with open(os.path.join(self.dir_name(file_path), f'{self.f_name(file_path)}.txt'), 'wb') as output_file:
                    output_file.write(decompressed_data)
                
                with open(self.output().path, 'a') as exit_file:
                    added_file_path = os.path.join(self.dir_name(file_path), f'{self.f_name(file_path)}.txt')
                    exit_file.write(f'\n{added_file_path}')
            
class DataFrameProcessor(luigi.Task):
    # Параметр URL для обработки данных
    url = luigi.Parameter()
    
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())
    
    # Зависит от предыдущего шага распаковки данных
    def requires(self):
        return DataUnpacking(self.url)
    
    # Определяет выходной файл для результатов обработки
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'DataFrameProcessor_result_{self.name()}.txt'))
       
    # Генерирует путь к файлу CSV из контента
    def csv_name(self, path, csv_content):
       cwd = os.getcwd()
       folder = path.split(os.sep)[-2]
       return os.path.join(cwd, 'data', folder, f'{csv_content}.csv')
    
    # Преобразует текстовый файл в набор DataFrame'ов
    def file_to_df(self, file_path):
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith('['):
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=None if write_key == 'Heading' else 'infer')
                    fio = io.StringIO()
                    write_key = l.strip('[]\n')
                    continue
                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')
        return dfs
     
    # Выполняет чтение файлов, их обработку и сохранение в формате CSV
    def run(self):
        path_list = []
        with open(self.input().path, 'r') as f:
            for l in f.readlines():
                if l != '\n':
                    path_list.append(l.replace('\n', ''))
        
        for file_path in path_list:
            dfs = self.file_to_df(file_path)
            for data_frame in dfs.keys():
                dfs[data_frame].to_csv(self.csv_name(file_path, data_frame))
                with open(self.output().path, 'a') as exit_file:
                    exit_file.write(f'\n{self.csv_name(file_path, data_frame)}')

class ColumnsDropper(luigi.Task):
    # Параметр URL
    url = luigi.Parameter()
    
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())
    
    # Зависит от обработки DataFrame
    def requires(self):
        return DataFrameProcessor(self.url)

    # Определяет выходной файл для результата удаления колонок
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'ColumnsDropper_result_{self.name()}.txt'))
    
    # Удаляет указанные колонки из CSV файлов
    def run(self):
        path_list = []
        with open(self.input().path, 'r') as f:
            for l in f.readlines():
                if l.endswith('Probes.csv\n'):
                    path_list.append(l.strip())
        
        for file_path in path_list:
            df = pd.read_csv(file_path)
            df.drop(['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'], axis=1, inplace=True)
            df.to_csv(file_path)
            with open(self.output().path, 'a') as exit_file:
                exit_file.write(f'\n{file_path}')

class CleanUpTask(luigi.Task):
    # Параметр URL для задачи
    url = luigi.Parameter()

    # Задает зависимость от предыдущей задачи ColumnsDropper
    def requires(self):
        return ColumnsDropper(self.url)

    # Определяет файл результатов для задачи очистки
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'CleanUpTask_result_{self.name()}.txt'))

    # Генерация имени для выходного файла, основанного на URL
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())

    # Удаляет файлы, не относящиеся к CSV или не содержащие 'result' в имени
    def run(self):
        files_to_drop = []
        # Перебор файлов в каталоге 'data'
        for root, dirs, files in os.walk(os.path.join('data')):
            for name in files:
                if not name.endswith('.csv') and 'result' not in name:
                    files_to_drop.append(os.path.join(root, name))
                    os.remove(os.path.join(root, name))
        
        # Запись выполненных действий в выходной файл
        with self.output().open('w') as f:
            if files_to_drop:
                f.writelines(["Не относящиеся к CSV файлы или файлы без 'result' в названии были удалены.\n",
                              "Очищены следующие файлы:\n"] + [f"* {file}\n" for file in files_to_drop])
            else:
                f.write("Файлы для удаления не найдены.\n")


class FinalReportTask(luigi.Task):
    # Параметр URL для генерации финального отчета
    url = luigi.Parameter()

    # Зависимость от задачи очистки CleanUpTask
    def requires(self):
        return CleanUpTask(self.url)

    # Определяет файл для финального отчета
    def output(self):
        return luigi.LocalTarget(os.path.join('data', f'FinalReportTask_report_{self.name()}.txt'))

    # Генерация имени файла на основе URL
    def name(self):
        return ''.join(char for char in self.url.split('/')[-1] if char.isalnum())

    # Сборка финального отчета об выполнении всех задач
    def run(self):
        report_lines = ["Отчет о выполнении задач:\n"]
        # Описание задач и их действий
        tasks_info = [
            (DataDownloader, "скачал файл и поместил его в директорию 'data'"),
            (TarExtracter, "распаковал файлы из архива в директорию 'data'"),
            (DataUnpacking, "извлек данные из сжатых файлов и создал текстовые файлы с результатами"),
            (DataFrameProcessor, "обработал датасеты, произвёл анализ данных"),
            (ColumnsDropper, "удалил ненужные колонки из датасетов и обновил файлы"),
            (CleanUpTask, "удалил нерелевантные файлы и файлы не содержащие 'result'")
        ]

        # Генерация отчета
        for i, (task_cls, action) in enumerate(tasks_info, start=1):
            task_name = task_cls.__name__
            report_lines.append(f"{i} шаг: {task_name} ({action}).\n")
            # Попытка чтения результатов каждой задачи для включения в отчет
            try:
                with open(os.path.join('data', f"{task_name}_result_{self.name()}.txt"), 'r') as f:
                    paths = [line.strip() for line in f.readlines() if line.strip()]
                    if paths:
                        report_lines.append(f"Задача '{task_name}' создала файлы:\n* " + "\n* ".join(paths) + "\n")
                    else:
                        report_lines.append(f"Для задачи '{task_name}' файлы не найдены.\n")
            except FileNotFoundError:
                report_lines.append(f"Файл результатов для '{task_name}' не найден.\n")

        # Запись отчета в файл
        with self.output().open('w') as f:
            f.writelines(report_lines)


if __name__ == '__main__':
    luigi.run()

