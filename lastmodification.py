from time import ctime
import os.path

file = '/home/pi/data.txt'
modi_time = os.path.gettime(file)
print (ctime(modi_time))