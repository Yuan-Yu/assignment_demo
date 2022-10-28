# Start job worker
````bash
celery --app=job  worker  -c 1 -Q job -D
````

# Start GPU worker with different CUDA_VISIBLE_DEVICES
````bash
CUDA_VISIBLE_DEVICES=0 celery -A sub_job worker -n GPU0 -c 1 -Q sub_job -D   
CUDA_VISIBLE_DEVICES=1 celery -A sub_job worker -n GPU1 -c 1 -Q sub_job -D
````

# Shutdown
````bash
celery -A sub_job control shutdown
celery -A job control shutdown
````

# User API (import job_control)
````python
submitJob(img_path: str) -> int  
get_job_progress(jobid: int)  
cancel_job(jobid: int)  
get_job_info(jobid: int)  
````


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
