
import os
import random
from datetime import datetime, timedelta
from multiprocessing import Pool

import pandas as pd
from faker import Faker

class DataSetGenerator:

    def __init__(self, num_records: int = 1000, num_process: int = None, duplicate_fraction: float = 0.1, ready_file_name: str = 'big_data_set.csv'):

        if num_process is None:
            self.num_process = os.cpu_count()

        self.num_records = num_records
        self.duplicate_fraction = duplicate_fraction
        self.file_name = ready_file_name
        
        # salary limit
        self.min_salary = 30_000
        self.max_salary = 200_000

        # age limit
        current_date = datetime.now()
        self.start_date = current_date - timedelta(days=(65 * 365)) # 65 years ago
        self.end_date = current_date - timedelta(days=(18*365))     # 18 years ago

    def generate_chunk(self, chunk_size: int, chunk_id: int = None):

        fake = Faker()
        fake.seed_instance(chunk_id)

        unique_size = int(chunk_size * (1 - self.duplicate_fraction))   # create general part without duplicates
        duplicate_size = chunk_size - unique_size

        names =  [f"Worker_{chunk_id}_{i}" for i in range(unique_size)]
        salaries = [random.randint(self.min_salary, self.max_salary) if random.random() > 0.5 else '' for _ in range(unique_size)]
        birth_dates = [fake.date_between_dates(date_start=self.start_date, date_end=self.end_date) if random.random() > 0.5 else '' for _ in range(unique_size)]
        times = [fake.time() if random.random() > 0.5 else '' for _ in range(unique_size)]

        df = pd.DataFrame(
            {
                'Name': names ,
                'Salary': salaries,
                'BirthDate': birth_dates,
                'Time': times
            }
        )

        duplicates = df.sample(n=duplicate_size, replace=True)
        full_data = pd.concat([df, duplicates]).reset_index(drop=True)

        return full_data
    
    def create_set(self):

        chunk_size = self.num_records // 10
        num_chunks = self.num_records // chunk_size

        with Pool(self.num_process) as pool:
            chunks = pool.starmap(self.generate_chunk, [(chunk_size, i) for i in range(num_chunks)])

        data_set = pd.concat(chunks).reset_index(drop=True)
        return data_set


if __name__ == "__main__":
    gen = DataSetGenerator(num_records=100)
    data = gen.create_set()

    data.to_csv('big_data.csv', index=False)