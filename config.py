allow_image_size = (10240,10240)
tile_size = (512,512)
batch_size = 20
db_name = "test.db"
num_x_tile = int(allow_image_size[1] / tile_size[1])
num_y_tile = int(allow_image_size[0] / tile_size[0])

broker = "redis://default:redispw@localhost:49153"
backend = "redis://default:redispw@localhost:49153/0"