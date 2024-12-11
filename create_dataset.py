
import os
import random
from datetime import datetime, timedelta
from multiprocessing import Pool

import pandas as pd
import numpy as np
from faker import Faker

class DataSetGenerator:

    def __init__(self, num_records: int = 1000, num_process: int = None, duplicate_fraction: float = 0.1, ready_file_name: str = 'big_data_set.csv'):

        if num_process is None:
            self.num_process = os.cpu_count()

        self.num_records = num_records
        self.duplicate_fraction = duplicate_fraction
        self.file_name = ready_file_name
        
        # height limit
        self.mean_height = 160.00
        self.std_dev = 20.00      # standart deviation


        # age limit
        current_date = datetime.now()
        self.start_date = current_date - timedelta(days=(65 * 365)) # 65 years ago
        self.end_date = current_date - timedelta(days=(18*365))     # 18 years ago

    def generate_chunk(self, chunk_size: int, chunk_id: int = None):

        fake = Faker()
        fake.seed_instance(chunk_id)

        unique_size = int(chunk_size * (1 - self.duplicate_fraction))   # create general part without duplicates
        duplicate_size = chunk_size - unique_size

        names =  [f"{fake.name()}" for i in range(unique_size)]
        height = [round(float(np.random.normal(self.mean_height, self.std_dev)), 2) if np.random.random() > 0.3 else '' for _ in range(unique_size)]
        birth_dates = [str(fake.date_between_dates(date_start=self.start_date, date_end=self.end_date)) + ' ' + fake.time() if random.random() > 0.3 else '' for _ in range(unique_size)]

        df = pd.DataFrame(
            {
                'Name': names ,
                'Height': height,
                'BirthDate': birth_dates
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
    file_name = '.test_data_set.csv'
    gen = DataSetGenerator(num_records=1_000, ready_file_name=file_name)
    data = gen.create_set()

    data.to_csv(file_name, index=False)