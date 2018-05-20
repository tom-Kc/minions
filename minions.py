import psycopg2
from table import BossTable, MinionWorker
import multiprocessing
import traceback
import time
import logging
import json

# LOGGING SETUP
FORMAT = '%(asctime)-15s] %(message)s'
logging.basicConfig(format=FORMAT, level="INFO")
logger = logging.getLogger(__file__)

# DATABASE SETUP
CONNECTION_STRING = "host=localhost port=5432 dbname=project user=project password=project"
BOSS_ID = "test"
MT = BossTable(BOSS_ID)
MT.conn = psycopg2.connect(CONNECTION_STRING)

def make_example_tasks(boss_table, n):
    """Clears & populates the tasks table"""
    try:
        boss_table.drop_table()
    except psycopg2.ProgrammingError as e:
        logger.info(e)

    try:
        tasks = []
        for i in range(1, n+1):
            task_spec = {'p' : 179424691, 'a' : i}
            tasks.append((f'task{i}', json.dumps(task_spec)))
        boss_table.create_table(*zip(*tasks))
    except psycopg2.ProgrammingError as e:
        logger.info(e)


make_example_tasks(MT, 13)

print("===========")
MT.show_table()

# Number of workers
k = 3
processes = []

def f(x):
    """An example worker function
    x will be the task_spec column in the row we are working on"""
    spec = json.loads(x)
    a, p = int(spec['a']), int(spec['p'])
    r = 1
    for k in range(1, p):
        r = (r * a) % p
        if r == 0:
            return 0
        elif r == 1:
            return k
    raise ValueError('Reached end of loop, nothing found!')

# Launch workers
for i in range(k):
    worker = MinionWorker(CONNECTION_STRING, BOSS_ID, "worker{}".format(i))
    p = multiprocessing.Process(target=lambda : worker.run(f))
    p.start()
    processes.append(p)

# Show their progress
from itertools import count
for i in count():
    print("=========== ", i)
    MT.show_table()
    remaining = list(MT.remaining_tasks())
    print("\t {} remain = {}".format(len(remaining), {row['id'] for row in remaining}))

    if not remaining:
        logger.info("None remaining - stopping.")
        break
    time.sleep(1)

# Wait for worker processes to end
for p in processes:
    p.join()

MT.conn.close()

