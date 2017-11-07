# Standard library
import time

# Third-party
import h5py
import numpy as np

results_file = 'results2.h5'

def worker(task):
    i, obj = task

    # do some analysis
    if obj < 0.5:
        thing = obj ** 2

    else:
        thing = obj / 4.

    time.sleep(0.01)

    return i, thing

def callback(result):
    i, thing = result

    with h5py.File(results_file, 'a') as f:
        f['data'][i] = thing

data = np.random.uniform(size=100)

# create an HDF5 file to write results to
with h5py.File(results_file, 'w') as f:
    f.create_dataset(name='data', shape=data.shape,
                     dtype=np.float64)

tasks = [(i,obj) for i,obj in enumerate(data)]
for result in map(worker, tasks):
    callback(result)

with h5py.File(results_file, 'r') as f:
    print(f['data'][:])

# But: weird scope issues
