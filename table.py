from datetime import datetime
import psycopg2
import psycopg2.extras
import logging
import traceback

FORMAT = '%(asctime)-15s] %(message)s'
logging.basicConfig(format=FORMAT, level="INFO")
logger = logging.getLogger(__file__)
logger.warning("HI")


class BossTable(object):
    def __init__(self, boss_id):
        self.boss_id = boss_id
        self.conn = None

    @property
    def _sql_table_name(self):
        return 'tasks_{}'.format(self.boss_id)

    @property
    def _sql_tasks_table(self):
        """
        id : the id for the task
        task_spec : this will be input to the tasks'
        """
        return """
        CREATE TABLE {} (
          id VARCHAR(64) NOT NULL,
          task_spec TEXT NOT NULL,
          status VARCHAR(32) NOT NULL,
          worker TEXT,
          updated TIMESTAMP WITHOUT TIME ZONE,
          data TEXT,
          PRIMARY KEY(id)
        )
        """.format(self._sql_table_name)

    def drop_table(self):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("DROP TABLE {}".format(self._sql_table_name))

    def create_table(self, task_ids, task_specs):
        now = datetime.utcnow()
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(self._sql_tasks_table)
                cur.executemany("INSERT INTO {} (id, task_spec, status, updated) VALUES (%s,%s,%s,%s)".format(self._sql_table_name),
                                [(t_id, t_spec, "UNASSIGNED", now) for t_id, t_spec in zip(task_ids, task_specs)])

    def show_table(self):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM {}".format(self._sql_table_name))
                for x in cur.fetchall():
                    print(x)

    def remaining_tasks(self):
        with self.conn:
            with self.conn.cursor(cursor_factory = psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM {}".format(self._sql_table_name))
                for x in cur.fetchall():
                    if x['status'] not in ['SUCCESS', 'FAILURE']:
                        yield x

    def work_one(self, worker_id):
        """Checkout next available task"""
        with self.conn:
            with self.conn.cursor(cursor_factory = psycopg2.extras.DictCursor) as cur:
                cur.execute("""UPDATE {table_name} SET (status,worker) = (%s,%s) WHERE id=(
                SELECT id
                FROM {table_name} 
                WHERE status='UNASSIGNED'
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                )
                RETURNING *""".format(table_name=self._sql_table_name), ('WORKING', worker_id))
                r = cur.fetchone()
                return r

    def _update_task(self, task_id, status, data):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""UPDATE {table_name} SET (status,data,updated)=(%s,%s,%s) 
                WHERE id=%s""".format(table_name=self._sql_table_name), (status, data, datetime.now(), task_id))

    def success(self, task_id, data):
        self._update_task(task_id, "SUCCESS", data)

    def failure(self, task_id, data):
        self._update_task(task_id, "FAILURE", data)

    def iter_free_tasks(self, worker_id):
        """Note that this will checkout/assign the tasks!"""
        while True:
            r = self.work_one(worker_id)
            if r is not None:
                yield r
            else:
                break


class MinionWorker(object):
    def __init__(self, conn_str, boss_id, worker_id):
        self.boss_id = boss_id
        self.worker_id = worker_id
        self.conn_str = conn_str

    def run(self, f):
        mt = BossTable(self.boss_id)
        try:
            mt.conn = psycopg2.connect(self.conn_str)

            for row in mt.iter_free_tasks(worker_id=self.worker_id):
                try:
                    result = f(row['task_spec'])
                except Exception as e:
                    mt.failure(row['id'], traceback.format_exc())
                    continue
                mt.success(row['id'], result)
        finally:
            mt.conn.close()

