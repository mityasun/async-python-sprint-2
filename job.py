from datetime import datetime
from threading import Thread, Timer
from typing import Generator
from uuid import uuid4

from constants import TIME_PATTERN
from tasks import get_task
from utils import logger, coroutine


class Job:
    def __init__(self,
                 task: str,
                 uid: str = "",
                 start_time: str = "",
                 duration: int = -1,
                 restarts: int = 0,
                 dependencies: list = None):
        self.task = get_task(task)
        if start_time:
            self.start_time = datetime.strptime(start_time, TIME_PATTERN)
        else:
            self.start_time = None
        self.duration = duration
        self.restarts = restarts
        self.dependencies = dependencies or []
        self.uid = uid if uid else uuid4().hex
        self.worker = None

    def perform_job(self) -> None:
        """
        1. Retrieves the task name from the docstring of the task object.
        2. If a start time is specified (self.start_at) and that time is in
        the future, it schedules a timer to run the task at the specified
        time.
        3. If no start time is specified or the start time has already passed,
        it starts the task immediately.
        4. If a maximum working time is specified, wait for the worker thread
        to finish or until the maximum working time is reached,
        whichever comes first.
        5. If the worker thread is still alive after the maximum working time
        has been reached, terminate the thread. Store the worker thread or
        timer in the worker attribute of the Job instance.
        """

        task_name = self.task.__doc__
        if self.start_time and self.start_time > datetime.now():
            seconds = (self.start_time - datetime.now()).total_seconds()
            logger.info(
                'Task "%s" will starts at %s.', task_name, self.start_time
            )
            worker = Timer(seconds, self.task)
            worker.start()
        else:
            logger.info('Task "%s" started.', task_name)
            worker = Thread(target=self.task)
            worker.start()
            if self.duration >= 0:
                worker.join(self.duration)
                if worker.is_alive():
                    worker.join()
                    logger.warning('Task "%s" was terminated.', task_name)
            else:
                worker.join()
        self.worker = worker

    def __getstate__(self):
        """Get dict with all Job attributes without worker"""

        state = self.__dict__.copy()
        state['worker'] = None
        return state

    def __setstate__(self, state):
        """
        Take the dict returned by __getstate__ and use it to set
        the instance attributes.
        """

        self.__dict__.update(state)

    @staticmethod
    @coroutine
    def run() -> Generator[None, 'Job', None]:
        """
        This is a static method that creates a coroutine generator.
        It runs in an infinite loop and yields control to the calling code
        each time it receives a new job. When a job is received, it is executed
        by calling the perform_job method of the Job object. If the
        execution of the job raises an exception, the method retries the
        job up to job tries times before giving up.
        """
        while True:
            job = yield
            try:
                job.perform_job()
            except GeneratorExit:
                logger.info('Finished schedule jobs.')
                raise
            except Exception as error:
                logger.error(error)
                while job.restarts > 0:
                    job.restarts -= 1
                    task_name = job.task.__doc__
                    logger.warning('Task "%s" restarted.', task_name)
                    try:
                        job.perform_job()
                        logger.info(
                            'Task "%s" successful finished.', task_name
                        )
                    except Exception as error:
                        logger.error(error)
            finally:
                del job
