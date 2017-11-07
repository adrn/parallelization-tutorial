# Standard library
import time

# Third-party
import h5py
import numpy as np
import tqdm

data = np.random.uniform(size=100)

# create an HDF5 file to write results to
results_file = 'results1.h5'
with h5py.File(results_file, 'w') as f:
    f.create_dataset(name='data', shape=data.shape,
                     dtype=np.float64)

for i in tqdm.tqdm(range(len(data))):
    obj = data[i]

    # do some analysis
    if obj < 0.5:
        thing = obj ** 2

    else:
        thing = obj / 4.

    # write output / results?
    with h5py.File(results_file, 'a') as f:
        f['data'][i] = thing

    time.sleep(0.01)

with h5py.File(results_file, 'r') as f:
    print(f['data'][:])
