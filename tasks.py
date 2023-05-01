import multiprocessing
import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Callable

from constants import DIR, RENAMED_FILE, FILE, RENAMED_DIR, CITIES
from task_api.tasks import (DataFetchingTask, DataCalculationTask,
                            DataAggregationTask, DataAnalyzingTask)
from utils import logger


def task_1():
    """Create and write the file"""

    try:
        with open(FILE, mode='w') as f:
            f.write('Hello world!')
        logger.info(f'Done {task_1.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_1.__doc__}')


def task_2():
    """Rename the file"""

    try:
        os.rename(FILE, RENAMED_FILE)
        logger.info(f'Done {task_2.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_2.__doc__}')


def task_3():
    """Make new dir"""

    try:
        os.mkdir(DIR)
        logger.info(f'Done {task_3.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_3.__doc__}')


def task_4():
    """Rename dir"""

    try:
        os.rename(DIR, RENAMED_DIR)
        logger.info(f'Done {task_4.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_4.__doc__}')


def task_5():
    """Move renamed file to renamed dir"""

    try:
        shutil.move(os.path.join(os.getcwd(), RENAMED_FILE), RENAMED_DIR)
        logger.info(f'Done {task_5.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_5.__doc__}')


def task_6():
    """Delete renamed file"""

    try:
        os.remove(os.path.join(RENAMED_DIR, RENAMED_FILE))
        logger.info(f'Done {task_6.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_6.__doc__}')


def task_7():
    """Delete renamed dir"""

    try:
        os.rmdir(RENAMED_DIR)
        logger.info(f'Done {task_7.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_7.__doc__}')


def task_8():
    """Make request, get API data, analysing and return answer"""

    logger.debug('Running ThreadPoolExecutor() for make_request')
    with ThreadPoolExecutor() as pool:
        forecasts = pool.map(DataFetchingTask().make_request, CITIES.keys())
        logger.info(f'Done {task_8.__doc__}')

    logger.debug('Running ProcessPoolExecutor() for cities models')
    cores_count = multiprocessing.cpu_count()
    with ProcessPoolExecutor(max_workers=cores_count - 1) as executor:
        data = executor.map(
            DataCalculationTask().general_calculation, forecasts
        )

    logger.debug('Add rating for cities')
    result_data = DataCalculationTask().adding_rating(data)

    logger.debug('Write results to json file')
    DataAggregationTask(threading.RLock()).save_results_to_json(result_data)

    logger.debug('Return the best city by weather conditions')
    logger.info(f'Done {task_8.__doc__}')
    return DataAnalyzingTask().get_result(result_data)


def task_9():
    """Delete report.json"""

    try:
        os.remove('report.json')
        logger.info(f'Done {task_9.__doc__}')
    except Exception as error:
        logger.info(f'{error} {task_9.__doc__}')


def get_task(task_name: str) -> Callable:
    return TASKS[task_name]


TASKS = {
    'task_1': task_1,
    'task_2': task_2,
    'task_3': task_3,
    'task_4': task_4,
    'task_5': task_5,
    'task_6': task_6,
    'task_7': task_7,
    'task_8': task_8,
    'task_9': task_9
}
