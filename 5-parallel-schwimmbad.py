# Standard library
import os
import time

# Third-party
import h5py
import numpy as np

from schwimmbad import MultiPool

class Worker:

    def __init__(self, power, factor, results_file):
        self.power = power
        self.factor = factor
        self.results_file = results_file

    def work(self, obj):
        # do some analysis
        if obj < 0.5:
            thing = obj ** self.power

        else:
            thing = obj / self.factor

        time.sleep(0.01)

        return thing

    def __call__(self, task):
        i, obj = task
        return (i, self.work(obj))

    def callback(self, result):
        i, thing = result

        with h5py.File(self.results_file, 'a') as f:
            f['data'][i] = thing

# ------------------------------------------------------------------------------

data = np.random.uniform(size=100)

# create an HDF5 file to write results to
results_file = 'results5.h5'
with h5py.File(results_file, 'w') as f:
    f.create_dataset(name='data', shape=data.shape,
                     dtype=np.float64)

worker = Worker(power=2., factor=4.,
                results_file=results_file)
tasks = [(i,obj) for i,obj in enumerate(data)]

pool = MultiPool()
print("Using {0} CPUs".format(os.cpu_count()))
pool.map(worker, tasks, callback=worker.callback)

with h5py.File(results_file, 'r') as f:
    print(f['data'][:])
