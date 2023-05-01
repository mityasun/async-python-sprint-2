import os
from datetime import datetime, timedelta
from threading import Thread

from constants import RENAMED_FILE, FILE
from job import Job
from scheduler import Scheduler


def test_scheduler():
    """
    Test queue should add tasks to queue and should be empty after
    running tasks
    """

    task_1 = Job('task_1')
    task_2 = Job('task_2',
                 start_time=(datetime.now() + timedelta(seconds=5)).strftime(
                     '%d.%m.%Y %H:%M:%S'))
    scheduler = Scheduler(pool_size=2)
    scheduler.schedule([task_1, task_2])
    assert len(scheduler.queue) == 2
    scheduler.run()
    assert not scheduler.queue


def test_scheduler_get_job_no_dependencies():
    """Test getting a job without dependencies."""

    task = Job('task_1')
    scheduler = Scheduler(pool_size=1)
    scheduler.schedule([task])

    job = scheduler.get_job()

    assert job is not None
    assert job == task


def test_scheduler_with_dependencies():
    """Test scheduling tasks with dependencies."""

    task_1 = Job('task_1')
    task_2 = Job('task_2', dependencies=[task_1])
    scheduler = Scheduler(pool_size=2)
    scheduler.schedule([task_1, task_2])
    assert len(scheduler.queue) == 2
    scheduler.run()
    assert not scheduler.queue


def test_scheduler_with_max_working_time():
    """Test scheduling tasks with maximum working time."""

    task_1 = Job('task_1', duration=1)
    scheduler = Scheduler(pool_size=1)
    scheduler.schedule([task_1])
    scheduler.run()
    assert not scheduler.queue


def test_job_perform_with_thread():
    """Test perform a job with a thread."""

    job = Job('task_1')
    job.perform_job()
    assert job.worker is not None
    assert isinstance(job.worker, type(Thread(target=lambda: None)))
    job.worker.join()
    assert not job.worker.is_alive()


def test_delete_files_after_test():
    """Delete tests files after all tests"""

    os.remove(FILE)
    os.remove(RENAMED_FILE)
