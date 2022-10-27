import time
import random
import logging

class FeakeModel():
    def fit(self,x,y,epochs=10):
        time.sleep(20)
    def predict(self,x,batch_size=None):
        time.sleep(0.02) # mimic inference
        return random.randint(0,1) == 1

def load_model(path):
    logging.info(f"load model from {path}")
    time.sleep(5)
    return FeakeModel()

class Object(object):
    pass

keras = Object()
keras.models = Object()
keras.models.load_model = load_model