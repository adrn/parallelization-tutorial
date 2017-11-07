"""
To use serial processing, execute as:

    python 6-schwimmbad.py


To use multiprocessing on a single node, execute as:

    python 6-schwimmbad.py --ncores=4


To use MPI to distribute across many CPUs, execute as:

    mpiexec python 6-schwimmbad.py --mpi
"""

# Standard library
import sys
import time

# Third-party
import h5py
import numpy as np

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

def main(pool):
    data = np.random.uniform(size=100)

    # create an HDF5 file to write results to
    results_file = 'results6.h5'
    with h5py.File(results_file, 'w') as f:
        f.create_dataset(name='data', shape=data.shape,
                         dtype=np.float64)

    worker = Worker(power=2., factor=4.,
                    results_file=results_file)
    tasks = [(i,obj) for i,obj in enumerate(data)]

    for r in pool.map(worker, tasks, callback=worker.callback):
        pass

    with h5py.File(results_file, 'r') as f:
        print(f['data'][:])

if __name__ == "__main__":
    import schwimmbad

    from argparse import ArgumentParser

    parser = ArgumentParser(description="")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--ncores", dest="n_cores", default=1,
                       type=int, help="Number of processes (uses "
                                      "Python's multiprocessing).")
    group.add_argument("--mpi", dest="mpi", default=False,
                       action="store_true", help="Run with MPI.")
    args = parser.parse_args()

    pool = schwimmbad.choose_pool(mpi=args.mpi,
                                  processes=args.n_cores)

    print("Using pool: {0}".format(pool.__class__.__name__))

    main(pool)
    sys.exit(0)
