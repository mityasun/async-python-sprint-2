import pickle
from datetime import datetime
from typing import Optional

from constants import SAVED_JOBS
from job import Job
from utils import logger


class Scheduler:

    def __init__(self, pool_size: int = 10):
        self.pool_size = pool_size
        self.job_manager = Job.run()
        self.queue = []

    @staticmethod
    def save_to_file(queue: list[Job]) -> None:
        """
        Saves a list of Job objects to a binary file using pickle.
        queue: A list of Job objects that needs to be saved in a file.
        """

        with open(SAVED_JOBS, 'wb') as f:
            pickle.dump(queue, f)
        logger.debug('Tasks saved')

    @staticmethod
    def load_from_file() -> list[Job]:
        """
        Loads list of Job objects from a binary file using pickle.
        Returns the list of Job objects if the file exists, else returns an
        empty list.
        """

        try:
            with open(SAVED_JOBS, 'rb') as f:
                logger.debug('Tasks loaded')
                return pickle.load(f)
        except FileNotFoundError:
            logger.debug(f'Couldnt open {SAVED_JOBS}, check file')
            return []

    def schedule(self, job_list: list[Job]) -> None:
        """
        Schedules a list of Job objects.
        job_list: A list of Job objects that needs to be scheduled.
        If there are any saved tasks in the binary file, it loads them.
        For each job in the job_list, it adds the job to the queue.
        If the queue is full, it logs an error message and skips the job.
        If the job has a start time in the future, it logs a warning message.
        Otherwise, it logs an info message that the task has been added to the
        schedule.
        """

        self.queue = self.load_from_file()
        if not self.queue:
            logger.info('No saved tasks found in %s', SAVED_JOBS)
            self.queue = []
        for job in job_list:
            self.queue.append(job)
            task_name = job.task.__doc__
            if self.pool_size < len(self.queue):
                logger.error(
                    'Tried schedule "%s", but the queue is full',
                    task_name
                )
                continue
            if job.start_time and job.start_time > datetime.now():
                logger.warning(
                    'Task "%s" added to scheduling at %s',
                    task_name,
                    job.start_time
                )
            else:
                logger.info('Task "%s" is added to the schedule', task_name)

    def get_job(self) -> Optional[Job]:
        """
        Returns the next job to run.
        Checks the first job in the queue and removes it.
        If the job's start time has already passed, and it has no dependencies,
        it's returned.
        If the job's start time has not yet arrived, it logs a warning message
        and returns None.
        If the job has dependencies that are either in the queue or running,
        it puts the job at the end of the queue and returns None.
        """

        job = self.queue.pop(0)
        task_name = job.task.__doc__
        if job.start_time and job.start_time < datetime.now():
            logger.warning(
                'Tried to add task "%s" to the schedule, but time is expired',
                task_name
            )
            return None
        if job.dependencies:
            for dependency in job.dependencies:
                if (dependency in self.queue
                        or dependency.worker
                        and dependency.worker.is_alive()):
                    self.queue.append(job)
                    return None
        return job

    def run(self) -> None:
        """
        Runs the scheduled jobs.
        If there are any jobs in the queue, it logs a message that
        the scheduled jobs are starting.
        It continues to get jobs from the queue until either the queue is empty
        or the pool size is reached.
        If a job is obtained, it sends it to the job manager to run it.
        Finally, it saves the remaining jobs in the queue to the binary file.
        """

        count = 0
        if self.queue:
            logger.info('Starting schedule jobs.')
        while self.queue and count < self.pool_size:
            job = self.get_job()
            if job:
                count += 1
                self.job_manager.send(job)
        self.save_to_file(self.queue)
