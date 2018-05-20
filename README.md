# minions

Implements a concurrent, persistent work queue with a postgresql table

```sql
CREATE TABLE {} (
  id VARCHAR(64) NOT NULL,
  task_spec TEXT NOT NULL,
  status VARCHAR(32) NOT NULL,
  worker TEXT,
  updated TIMESTAMP WITHOUT TIME ZONE,
  data TEXT,
  PRIMARY KEY(id)
)
```

`id` : id of the task

`task_spec` : this will be passed as argument to the worker function

`status` : one of
```
UNASSIGNED
WORKING
SUCCESS
FAILURE
```

`worker` : id for the worker

`updated` : last time this row was updated (can be used to timeout lost workers)

`data` : outcome of the task (error message / result of worker function)


### Usage
```bash
# Start postgresql docker container
sudo docker-compose up -d

# Install requirements
pip install -r requirements.txt

# Start example script
python minions.py
```


