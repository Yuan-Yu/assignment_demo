import numpy as np
from celery import Celery
import sqlite3
import config
import json


job_app = Celery('job',broker=config.broker, backend=config.backend) 
job_app.conf.task_routes  = {'job.*': {'queue': 'job'}}

def submitJob(img_path):
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    c = c.execute("INSERT INTO inference_job_profile (IMGPATH,STATUS,PROGRESS) \
        VALUES (?, 0, 0 );",[img_path])
    task = job_app.send_task("job.inference",(c.lastrowid,))
    c.execute("UPDATE inference_job_profile \
                    SET TASKID=? \
                    WHERE ID=?",(task.id,c.lastrowid))
    conn.commit()
    conn.close()
    return c.lastrowid
    
def cancel_job(jobid):
    _update_job_status(jobid,3)

def _update_job_status(jobid, status_code):
    # 0 queuing
    # 1 running
    # 2 finish
    # 3 cancel
    # -1 error
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    c.execute("UPDATE inference_job_profile SET STATUS=? WHERE ID=?;",(status_code,jobid))
    conn.commit()
    conn.close()  

def _update_job_progress(jobid, progress):
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    c.execute("UPDATE inference_job_profile SET PROGRESS=? WHERE ID=?;",(progress,jobid))
    conn.commit()
    conn.close()


def _update_job_result(jobid, results):
    conn = sqlite3.connect(config.db_name)
    result_json = json.dumps(np.packbits(results).tolist())
    c = conn.cursor()
    c.execute("UPDATE inference_job_profile SET RESULTS=?,PROGRESS=100,STATUS=2 WHERE ID=?;",(result_json,jobid))
    conn.commit()
    conn.close()

def get_job_info(jobid):
    conn = sqlite3.connect(config.db_name)
    c = conn.cursor()
    c.execute("SELECT * FROM inference_job_profile WHERE ID=?;",(jobid,))
    row = c.fetchone()
    conn.close()
    return row

def get_job_result(jobid):
    job_info = get_job_info(jobid)
    if job_info[2] != 2: #status != finished
        return None
    tmp = np.array(json.loads(job_info[4]),dtype=np.uint8)
    return np.unpackbits(tmp)

def get_job_progress(jobid):
    job_info = get_job_info(jobid)
    s = job_info[2]
    if s == 0:
        return ("Queuing",0)
    elif s == 1:
        return ("Running",job_info[3])
    elif s == 2:
        return ("Finished",100)
    elif s == 3:
        return ("Canceled",job_info[3])
    else:
        return ("Error",0)