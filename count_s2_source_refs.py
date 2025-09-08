import json
import gzip
from collections import Counter
from multiprocessing import Pool, cpu_count
from operator import add
from functools import reduce
import glob
import pickle
import uuid
import os
import shutil
from pathlib import Path
import logging
import sys


log_level = os.environ.get('LOG_LEVEL', 'INFO')

logging.basicConfig(
    level=log_level,
    format='%(asctime)s : %(levelname)s : %(message)s',
    stream=sys.stdout
)


def create_counter(input_file_path: str, path_to_counter: str) -> None:

    with open(f'{path_to_counter}/counter_{uuid.uuid4()}.pkl', 'wb') as outp:
        try:
            with gzip.open(input_file_path, mode='r') as file:
                new_data = []

                for line in file:
                    new_item = json.loads(line)
                    if new_item.get('citedcorpusid'):
                        new_data.append(new_item.get('citingcorpusid'))

                if len(new_data) > 0:
                    pickle.dump(Counter(new_data), outp, pickle.HIGHEST_PROTOCOL)
        except:
            logging.info(input_file_path)
            pickle.dump(Counter(), outp, pickle.HIGHEST_PROTOCOL)

def read_pickle_counter(input_file_path: str):
    with open(input_file_path, 'rb') as file:
        return pickle.load(file)

def create_counter_in_parallel(path_to_references: str,
                               path_to_counter: str,
                               max_workers: int = cpu_count()):

    list_with_files = glob.glob(f'{path_to_references}/*.jsonl.gz')

    with Pool(processes=max_workers) as pool:
        pool.starmap(create_counter, zip(list_with_files, [path_to_counter] * len(list_with_files)))

def reduce_counter(path_to_counters: str, output_file_path: str) -> None:

    list_with_counters = glob.glob(f'{path_to_counters}/*.pkl')

    c_reduced = reduce(add, (Counter(dict(read_pickle_counter(i))) for i in list_with_counters))

    with open(file=f'{output_file_path}/counter.json', mode='w') as output_file:
        result = json.dumps(c_reduced)
        output_file.write(result)


if __name__ == '__main__':

    path_to_references = '/scratch/users/haupka/semantic-scholar-snapshot/citations'
    path_to_counters = '/scratch/users/haupka/semantic-scholar-snapshot/counters'
    output_file_path = '/scratch/users/haupka/semantic-scholar-snapshot'

    if Path(path_to_counters).exists() and Path(path_to_counters).is_dir():
        shutil.rmtree(path_to_counters)

    os.makedirs(path_to_counters, exist_ok=False)

    create_counter_in_parallel(path_to_references=path_to_references,
                               path_to_counter=path_to_counters)
    reduce_counter(path_to_counters=path_to_counters,
                   output_file_path=output_file_path)

    
