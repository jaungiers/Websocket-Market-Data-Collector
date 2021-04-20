import os
import numpy as np
import pandas as pd
import h5py
import hashlib
from time import sleep
from datetime import datetime

chunkFilepath = 'datachunk.csv'
datapackFilepath = 'datapack.h5'
checksumFilepath = 'datachunk.m5sum'
md5sum = None
depth = 5
init_chunk_size = 10000

# (shape, csvIdxStart, csvIdxEnd, dtype)
dataKeysMeta = {
	'tsLocal': (1, 0, 1, 'int64'),
	'tsXchng': (1, 1, 2, 'int64'),
	'bids': (depth, 2, 7, 'float'),
	'vbids': (depth, 7, 12, 'float'),
	'asks': (depth, 12, 17, 'float'),
	'vasks': (depth, 17, 22, 'float'),
	'tickDir': (4, 22, 26, 'int'),
	'last': (1, 26, 27, 'float'),
	'vlast': (1, 27, 28, 'float'),
	'isSell': (1, 28, 29, 'int'),
	'homeNotional': (1, 29, 30, 'float'),
	'foreignNotional': (1, 30, 31, 'float'),
	'grossValue': (1, 31, 32, 'float')
}

def init_checks():
	global md5sum

	with open(checksumFilepath, 'r') as md5File:
		md5sum = md5File.read()

with h5py.File(datapackFilepath, 'a') as h5file:
	for key, dataMeta in dataKeysMeta.items():
		if key not in h5file:
			dset = h5file.create_dataset(
				key,
				(dataMeta[0], 0),
				maxshape=(dataMeta[0], None),
				chunks=(dataMeta[0], init_chunk_size),
				dtype=dataMeta[3]
			)

def appendChunk():
	try:
		chunkData = pd.read_csv(chunkFilepath, header=None)
		chunk_size = chunkData.shape[0]
	except pandas.errors.EmptyDataError:
		# DataCapture process is in the process of writing the chunk
		sleep(5)
		return False

	with h5py.File(datapackFilepath, 'a') as h5file:
		for key, dataMeta in dataKeysMeta.items():
			data = chunkData.iloc[:, dataMeta[1]:dataMeta[2]].values.T
			chunk_insert_idx = h5file[key].shape[1]
			h5file[key].resize(h5file[key].shape[1]+chunk_size, axis=1)
			try:
				h5file[key][:, chunk_insert_idx:] = data
			except TypeError as e:
				print(e)
				print(f'TypeError!\nchunkData.shape: {chunkData.shape}\ndata.shape: {data.shape}\nkey: {key}\ndataMeta: {dataMeta}')
				exit()
	print(f'[{datetime.now()}] Datapack HDF file updated')
	return True

def run(freq):
	global md5sum

	try:
		while True:
			try:
				_md5sum = hashlib.md5(open(chunkFilepath, 'rb').read()).hexdigest()
				if md5sum != _md5sum:
					print(f'[{datetime.now()}] ChunkFile change detected')
					md5sum = _md5sum
					with open(checksumFilepath, 'w') as md5File:
						md5File.write(_md5sum)
					appendSuccess = False
					while not appendSuccess:
						appendSuccess = appendChunk()
			except FileNotFoundError:
				print(f'[{datetime.now()}] ChunkFile does not exist')
			sleep(freq)
	except KeyboardInterrupt:
		print("Press Ctrl-C to terminate")
		return

if __name__ == '__main__':
	print('Running Chunk2HDF process')
	init_checks()
	run(freq=5)
