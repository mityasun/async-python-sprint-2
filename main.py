from datetime import datetime, timedelta

from job import Job
from scheduler import Scheduler

if __name__ == '__main__':

    task_1 = Job('task_1')
    task_2 = Job('task_2', restarts=3, dependencies=[task_1])
    task_3 = Job('task_3')
    task_4 = Job('task_4', dependencies=[task_3])
    task_5 = Job('task_5', dependencies=[task_1, task_2, task_3, task_4])
    task_6 = Job('task_6', dependencies=[task_5])
    task_7 = Job('task_7', dependencies=[task_5])
    task_8 = Job('task_8', duration=10)
    task_9 = Job(
        'task_9', restarts=3, dependencies=[task_8],
        start_time=(datetime.now() + timedelta(seconds=5)).strftime(
            '%d.%m.%Y %H:%M:%S'
        )
    )

    scheduler = Scheduler(pool_size=10)

    scheduler.schedule([
        task_1, task_2, task_3, task_4, task_5, task_6, task_7, task_8, task_9
    ])

    scheduler.run()
