from celery import Celery
from celery.signals import celeryd_after_setup
from multiprocessing import shared_memory,resource_tracker
import numpy as np
import tf as tf
import config
import os


app = Celery('sub_job',broker=config.broker, backend=config.backend) 
app.conf.task_routes  = {'sub_job.*': {'queue': 'sub_job'}}
model = None #the cache of model 

@celeryd_after_setup.connect()
def load_model(sender, instance, **kwargs):
    global model
    model = tf.keras.models.load_model('saved_model/my_model')

@app.task
def test():
    print(os.environ["CUDA_VISIBLE_DEVICES"])

@app.task
def predict_tiles(start_tile,end_tile,img_mem_name,result_mem_name,error_mem_name):
    global model
    shared_raw_img = shared_memory.SharedMemory(name=img_mem_name,
                        size=config.allow_image_size[0]*config.allow_image_size[1]*3)
    img_shared = np.ndarray((config.allow_image_size[0],config.allow_image_size[1],3), 
                                dtype=np.uint8, buffer=shared_raw_img.buf)

    result_raw = shared_memory.SharedMemory(name=result_mem_name,
                        size=config.num_x_tile*config.num_x_tile)
    result_shared = np.ndarray(config.num_x_tile*config.num_x_tile, dtype=bool, buffer=result_raw.buf)

    result_raw_error = shared_memory.SharedMemory(name=error_mem_name,
                        size=config.num_x_tile*config.num_x_tile)
    result_error_shared = np.ndarray(config.num_x_tile*config.num_x_tile, dtype=np.uint8, buffer=result_raw_error.buf)
    for i in range(start_tile,end_tile):
        tile_y = int(i / config.num_x_tile)
        tile_x = i % config.num_x_tile
        x_s = tile_x * config.tile_size[1]
        x_e = x_s + config.tile_size[1]
        y_s= tile_y * config.tile_size[0]
        y_e=  y_s + config.tile_size[0]
        img_shared[y_s:y_e,x_s:x_e]
        try:
            result_shared[i] = model.predict(img_shared[y_s:y_e,x_s:x_e])
        except Exception as e:
            print(e)
            result_error_shared[i] = 1
    resource_tracker.unregister(shared_raw_img._name, 'shared_memory')
    resource_tracker.unregister(result_raw._name, 'shared_memory')
    resource_tracker.unregister(result_raw_error._name, 'shared_memory')
    # logging.info(start_tile,end_tile)
    


