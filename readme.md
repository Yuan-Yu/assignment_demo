# start system
celery --app=job  worker  -c 1 -Q job -D

# this is gpu worker can use different CUDA_VISIBLE_DEVICES for different GPU worker
CUDA_VISIBLE_DEVICES=1 celery -A sub_job worker  -c 1 -Q sub_job -D 
CUDA_VISIBLE_DEVICES=2 celery -A sub_job worker  -c 1 -Q sub_job -D

# shutdown
celery -A sub_job control shutdown
celery -A job control shutdown


# User API
submitJob(img_path: str) -> int
get_job_progress(jobid: int)
cancel_job(jobid: int)
get_job_info(jobid: int)


# Data schema
````sql
CREATE TABLE inference_job_profile
       (ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        IMGPATH       TEXT    NOT NULL,
        STATUS        INT     NOT NULL,
        PROGRESS      INT  NOT NULL,
        RESULTS       TEXT ,
        INFO          TEXT,
        TASKID VARCHAR(36)); 
````
