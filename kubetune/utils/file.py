import gc
import io
import pathlib
import uuid
from typing import Dict, List, Tuple

import pandas as pd
from pandas.errors import ParserError, EmptyDataError

from ..logging import logger


def load_csv(file_info: Tuple[uuid.uuid4, pathlib.Path]) -> Dict[int, pd.DataFrame]:
    file_id, file_path = file_info
    try:
        return {'file_id': pd.read_csv(
            file_path, header=None, low_memory=False, delimiter=',', decimal='.'
        )}
    except ParserError as e:
        iob = io.StringIO()
        with open(file_path, 'r') as file:
            use_cols = len(file.readline().split(','))
            for line in file:
                if (commas := line.count(',')) >= use_cols:
                    line = line[::-1].replace(',', '',  1)[::-1]
                iob.write(line.replace('"', ''))
        file.close()
        del file
        gc.collect()

        iob.seek(0)
        data = pd.read_csv(
            iob, header=None, low_memory=False, delimiter=',', decimal='.'
        )
        logger.info(f"CSV file fixed by extra comma: {file_path}")
        return {'file_id': data}

    except EmptyDataError as e:
        # logger.error(f"Error reading CSV file because is empty: {file_path} -> {e}")
        return {'file_id': pd.DataFrame()}

    except Exception as e:
        logger.error(f"Error reading CSV file: {file_path} -> {e}")
        return {'file_id': pd.DataFrame()}


def load_log(file_info: Tuple[uuid.uuid4, pathlib.Path]) -> Dict[uuid.uuid4, List[str]]:
    file_id, file_path = file_info
    try:
        with open(file_path, 'r') as file:
            data = {file_id: file.readlines()}
        file.close()
        del file
        gc.collect()
        return data
    except Exception as e:
        try:
            with io.open(file_path, 'r', encoding="latin1") as file:
                tmp = [line for line in file]
            file.close()
            data = {file_id: tmp}
            del file, tmp
            gc.collect()
            logger.info(f"LOG file fixed successfully by \"latin1\" encoding: {file_path}")
            return data
        except Exception as e:
            logger.error(f"Error reading LOG file: {file_path} -> {e}")
            return {file_id: []}
