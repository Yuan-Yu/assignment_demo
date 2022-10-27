from celery import Celery
from celery.signals import celeryd_after_setup, worker_shutdown
from multiprocessing import shared_memory
import numpy as np
import logging 
import config
import cv2
import sqlite3
import time
from job_control import _update_job_status,get_job_info,_update_job_progress,_update_job_result

app = Celery('job',broker=config.broker, backend=config.backend) 
app.conf.task_routes  = {'job.*': {'queue': 'job'}}
task = Celery('sub_job',broker=config.broker, backend=config.backend) 
task.conf.task_routes  = {'sub_job.*': {'queue': 'sub_job'}}
shared_raw_img = None
img_shared = None
result_raw = None
result_shared = None
result_raw_error = None
result_error_shared = None

@app.task
def inference(jobid,timeout=7*60):
    startTime = time.time()
    try:
        job_info = get_job_info(jobid)
        if job_info[2]!=0: #status != queuing
            return
        _update_job_status(jobid,1)
        img = cv2.imread(job_info[1])
        np.copyto(img_shared, img)
        result_error_shared[:] = False
        result_shared[:] = False
        runnings = []
        num_tiles = config.num_x_tile * config.num_y_tile
        lastTaskn = 0
        for toTaskn in range(config.batch_size,num_tiles+config.batch_size,config.batch_size):
            toTaskn = num_tiles if toTaskn > num_tiles else toTaskn
            runnings.append(task.send_task('sub_job.predict_tiles',(lastTaskn,
                                            toTaskn,
                                            shared_raw_img.name,
                                            result_raw.name,
                                            result_raw_error.name)))
            lastTaskn = toTaskn
        lastTime = time.time()
        while runnings:
            if time.time()-lastTime > 3 :
                job_info = get_job_info(jobid)
                status = job_info[2]
                lastTime =time.time()
                if time.time() - startTime > timeout:
                    for running in runnings:
                        running.revoke()
                    _update_job_status(jobid,-1)
                    return
                    
                if status == 3: #status == cancel
                    for running in runnings:
                        running.revoke()
                    return
            for i in range(len(runnings)-1,-1,-1):
                if runnings[i].ready():
                    runnings.pop(i)
            progress = int((num_tiles - len(runnings)*config.batch_size) * 100 /num_tiles)
            _update_job_progress(jobid,progress)
        if np.all(result_error_shared == False): 
            _update_job_result(jobid,result_shared)    
        else:
            _update_job_status(jobid,-1)
    except Exception as e:
        _update_job_status(jobid,-1)
        raise e

@celeryd_after_setup.connect()    
def init(sender, instance, **kwargs):
    global shared_raw_img,img_shared,result_raw,\
        result_shared,result_raw_error,result_error_shared
    shared_raw_img = shared_memory.SharedMemory(create=True,
                        size=config.allow_image_size[0]*config.allow_image_size[1]*3)

    img_shared = np.ndarray((config.allow_image_size[0],config.allow_image_size[1],3), 
                                dtype=np.uint8, buffer=shared_raw_img.buf)

    result_raw = shared_memory.SharedMemory(create=True,
                        size=config.num_x_tile*config.num_x_tile)
    result_shared = np.ndarray(config.num_x_tile*config.num_x_tile, dtype=bool, buffer=result_raw.buf)

    result_raw_error = shared_memory.SharedMemory(create=True,
                        size=config.num_x_tile*config.num_x_tile)
    result_error_shared = np.ndarray(config.num_x_tile*config.num_x_tile, dtype=bool, buffer=result_raw_error.buf)
