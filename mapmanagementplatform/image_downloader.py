import sys
import sqlite3
import urllib.request
import datetime
import threading

import io
import random
import shutil
import sys
from multiprocessing.pool import ThreadPool
import pathlib

import requests
from PIL import Image
import time

# connection to db
conA = sqlite3.connect('db.sqlite3')
# cursor
curA = conA.cursor()
# query
curA.execute("SELECT image_key FROM backend_coordinate_property")
# load image keys into variable "[keys]"
image_keysA = [item[0] for item in curA.fetchall()]
print(type(image_keysA))
# set resolution
resolutionA = '/thumb-320.jpg'
resolutionB = '/thumb-640.jpg'
resolutionC = '/thumb-1024.jpg'
resolutionD = '/thumb-2048.jpg'

start = time.time()

def image_downloader(img_url: str):
  print(f'Downloading: {img_url}')
  res = requests.get(img_url, stream=True)
  count = 1
  while res.status_code != 200 and count <= 5:
      res = requests.get(img_url, stream=True)
      print(f'Retry: {count} {img_url}')
      count += 1
  # checking the type for image
  if 'image' not in res.headers.get("content-type", ''):
      print('ERROR: URL doesnot appear to be an image')
      return False
  # Trying to red image name from response headers
  try:
    image_name = img_url.split("/")
    name = "img_" + str(image_name[3]) + ".jpg"
  except:
      image_name = str(random.randint(11111, 99999))+'.jpg'

  i = Image.open(io.BytesIO(res.content))
  download_location = "./backend/static/Multi-images"
  i.save(download_location + '/'+ name)
  return f'Download complete: {img_url}'

def run_downloader(process:int, images_url:list):
  print(f'MESSAGE: Running {process} process')
  results = ThreadPool(process).imap_unordered(image_downloader, images_url)
  for r in results:
      print(r)


filename =[]
url=[]
# Generate url and file names for all keys
for i in range (0,100000):
  urlA = 'https://images.mapillary.com/'
  # formatting image name
  image_nameA = str(image_keysA[i]) 
  # build image url for resolutionB '640' 
  urlA = urlA + image_nameA + resolutionB
  url.append(urlA)

n_process = 30
run_downloader(n_process, url)

end = time.time()
print('Time taken to download {}'.format(len(url)))
print(end - start)