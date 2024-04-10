import multiprocessing
import os
import pathlib
import time
import uuid
from glob import glob
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
from typing import Dict, List, NoReturn, Optional, Union

from .logging import logger
from .utils.file import load_csv, load_log

ALLOWED_SUFFIXES = ['.log', '.csv']
ALLOWED_DATASETS = ['raw', 'meta', 'processed']
FILE_LOADER_MAPING = {'.csv': load_csv, '.log': load_log}


def get_data_paths(data_path: Union[str, pathlib.Path], suffix: str) -> Dict[int, pathlib.Path]:
    if paths := [
        path_ for path in glob(os.path.join(data_path, '**'), recursive=True)
        if (path_ := pathlib.Path(path)) and path_.is_file() and path_.suffix.lower() == suffix
    ]:
        return dict(zip(
            [uuid.uuid4().int for _ in range(len(paths))],
            paths
        ))


class DataLoader:
    def __init__(
        self,
        data_path: Union[str, pathlib.Path],
        data_names: Union[str, List[str], Dict[str, Union[str, List[str]]]] = None,
        suffix: Union[str, List[str]] = None,
        n_jobs: int = None
    ):
        self.data_path = data_path
        self.data_names = data_names
        self.suffix = suffix
        self.n_jobs = n_jobs if n_jobs is not None else multiprocessing.cpu_count()
        self._check_suffix()
        self._check_datasets()

    def _check_datasets(self):
        if self.data_names is None:
            logger.warning("No datasets provided, using default for 'raw'")
            self.raw = {'parent': 'raw_dataset', 'suffix': ['.log']}
            self.data_names = ['raw']

        elif isinstance(self.data_names, str):
            if self.data_names not in ALLOWED_DATASETS:
                raise ValueError(f"Dataset '{self.data_names}' not allowed")
            setattr(
                self,
                self.data_names,
                {
                    'parent': f'{self.data_names}_dataset',
                    'suffix': self._check_suffix(suffix=(
                        self.suffix if self.suffix is not None else ['.log', '.csv']
                    ))
                }
            )
            self.data_names = [self.data_names]

        elif isinstance(self.data_names, list):
            for dataset in self.data_names:
                if dataset not in ALLOWED_DATASETS:
                    raise ValueError(f"Dataset '{dataset}' not allowed")
                setattr(
                    self,
                    dataset,
                    {
                        'parent': f'{dataset}_dataset',
                        'suffix': self._check_suffix(suffix=(
                            self.suffix if self.suffix is not None else ['.log', '.csv']
                        ))
                    }
                )

        elif isinstance(self.data_names, dict):
            for dataset, suffix in self.data_names.items():
                setattr(
                    self,
                    dataset,
                    {
                        'parent': f'{dataset}_dataset',
                        'suffix': self._check_suffix(suffix=suffix)
                    }
                )
        else:
            raise ValueError("Datasets must be a string or a list of strings")

    def _check_suffix(
        self, suffix: Optional[Union[str, List[str]]] = None
    ) -> Union[List[str], NoReturn]:
        if suffix is None:
            if self.suffix is None:
                if isinstance(self.data_names, dict):
                    self.suffix = list(set([
                        item for sublist in [
                            [item] if isinstance(item, str) else item
                            for item in self.data_names.values()
                        ] for item in sublist
                    ]))
                else:
                    logger.warning("No suffix provided, using default for '.log'")
                    self.suffix = ['.log']
            if isinstance(self.suffix, str):
                if self.suffix not in ALLOWED_SUFFIXES:
                    raise ValueError(f"Suffix '{self.suffix}' not allowed")
                self.suffix = [self.suffix]
            elif isinstance(self.suffix, list):
                for suffix in self.suffix:
                    if suffix not in ALLOWED_SUFFIXES:
                        raise ValueError(f"Suffix '{suffix}' not allowed")
            elif not isinstance(self.suffix, list):
                raise ValueError("Suffix must be a string or a list of strings")
        else:
            if isinstance(suffix, str):
                if suffix not in ALLOWED_SUFFIXES:
                    raise ValueError(f"Suffix '{suffix}' not allowed")
                return [suffix]
            elif isinstance(suffix, list):
                for suf in suffix:
                    if suf not in ALLOWED_SUFFIXES:
                        raise ValueError(f"Suffix '{suf}' not allowed")
                return suffix
            elif not isinstance(suffix, list):
                raise ValueError("Suffix must be a string or a list of strings")

    def _get_data_paths(self):
        for name in self.data_names:
            info = getattr(self, name)
            update = {
                'data_paths': {
                    suffix: get_data_paths(
                        os.path.join(self.data_path, info['parent']), suffix=suffix
                    )
                    for suffix in info['suffix']
                }
            }
            info.update(update)
            setattr(self, name, info)

    def _load_files(self):
        pool = multiprocessing.Pool(self.n_jobs)
        for name in self.data_names:
            info = getattr(self, name)
            data = {}
            for suffix, paths in info['data_paths'].items():
                files, n_completed, n_empty = {}, 0, 0
                if paths is None:
                    continue
                for complete in process_map(
                    pool.map(FILE_LOADER_MAPING.get(suffix), list(paths.items())),
                    total=len(paths)
                ):
                    if complete:
                        n_completed += 1
                        complete = list(complete.items())
                        files[complete[0][0]] = complete[0][1]
                    else:
                        n_empty += 1
                        logger.error(f"Empty '{suffix}' file!")
                logger.info(
                    f"Dataset -> {name}. "
                    f"Suffix -> {suffix}. "
                    f"Number_of_complete_files -> {n_completed}. "
                    f"Number of empty files -> {n_empty}"
                )
                data[suffix] = files
            info.update({'data': data})
            setattr(self, name, info)

    def load(self):
        self._get_data_paths()
        self._load_files()
        print('Done')


    # def load(self):
    #     for dataset, parent in self.datasets.items():

    #         self._get_data_paths(os.path.join(self.data_path, parent), suffix=dataset)

    #     pool = multiprocessing.Pool(self.n_jobs)
    #     logs, number_of_complete_logs, empty_logs = {}, 0, 0
    #     for complete in tqdm(
    #         pool.map(process_log, list(datapaths['raw'].items())),
    #         total=len(datapaths)
    #     ):
    #         if complete and len(complete) > 0:
    #             number_of_complete_logs += 1
    #             complete = list(complete.items())
    #             logs[complete[0][0]] = complete[0][1]
    #         else:
    #             empty_logs += 1
    #             logger.error(f"Empty log file")

    # print(f"number_of_complete_logs -> {number_of_complete_logs}")
    # print(f"empty_logs -> {empty_logs}")

    #     for suffix, paths in self.data_paths.items():
    #         if suffix == '.log':
    #             self.logs = self._load_log(paths)
    #         elif suffix == '.csv':
    #             self.datasets = self._load_csv(paths)
