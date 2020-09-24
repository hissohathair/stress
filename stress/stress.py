"""
A trivial utility for consuming system resources.
"""
from __future__ import print_function

import argparse
import multiprocessing
import psutil
import signal
import sys
import time


def _spin(ram_per_core_mb):
    """
    Spin in an infinite loop, consuming up to ram_per_core_mb.

    :param ram_per_core_mb: How many MB of RAM to consume in loop.
    :return: None
    """
    me = psutil.Process()
    print(
        (
            f"Start time: {time.asctime()}. "
            f"Current RAM usage: {me.memory_info().rss / 1024 / 1024:,.1f} MB. "
            f"RAM usage target : {ram_per_core_mb:,} MB"
        )
    )

    steps = 10
    ram_per_step_mb = int(ram_per_core_mb / steps)

    dummy = list([1.0])
    while True:
        # Allocate RAM until resident set size (rss) reaches 90% of target
        if me.memory_info().rss < ram_per_core_mb * 1024 * 1024 * 0.90:
            try:
                num = 1.0
                dummy += [num] * int(ram_per_step_mb / sys.getsizeof(num) * 1024 * 1024)

            except MemoryError:
                # We didn't have enough RAM for our attempt, so we will recursively try
                # smaller amounts 10% smaller at a time
                ram_per_step_mb = int(ram_per_step_mb * 0.8)

        print((
            f"Memory allocated: {me.memory_info().rss / 1024 / 1024:,.1f} MB "
            f"of {ram_per_core_mb:,} MB in {len(dummy):,} elements "
            f"as at {time.asctime()}."
        ))

        # Do something with dummy to try and keep it in active RAM
        for i, n in enumerate(dummy):
            dummy[i] += 1.0


def _parse_args():
    """
    Parse the command line arguments

    :return: the parsed arguments
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cores",
                        help="The number of cores to stress")
    parser.add_argument("-m", "--memory",
                        help="The amount of memory in MB to allocate")

    return parser.parse_args()


def stress_processes(num_processes, ram_per_core_mb):
    """
    Starts a given number infinite processing loops. Pauses briefly between
    starting each process.

    :param num_processes: the number of processes to spin
    :param ram_per_core_mb: amount of RAM each process should try and use
    :return: None
    """

    for _ in range(num_processes):
        multiprocessing.Process(target=_spin, args=(ram_per_core_mb,)).start()
        time.sleep(3)


def _cmd_line():
    """
    The command line entry point for this script.

    :return: None
    """

    arguments = _parse_args()
    cores_to_stress = int(arguments.cores) if arguments.cores else None
    memory_to_allocate = int(arguments.memory) if arguments.memory else None

    # This eats the interrupted exception when the process is terminated
    signal.signal(signal.SIGINT, lambda x, y: sys.exit(1))

    if not cores_to_stress:
        cores_to_stress = multiprocessing.cpu_count()
    if not memory_to_allocate:
        memory_to_allocate = psutil.virtual_memory().total / 1024 / 1024

    memory_per_core = int(memory_to_allocate / cores_to_stress)

    print(f"Memory: {psutil.virtual_memory()}")
    print(f"Starting {cores_to_stress} processes which will consume up to {memory_per_core:,} MB each")

    if cores_to_stress:
        # The CPU stress has to happen in another process since it is infinitely
        # CPU hungry. Each process will consume RAM.
        multiprocessing.Process(target=stress_processes,
                                args=(cores_to_stress, memory_per_core)).start()
    else:
        # Changed from 1.0 - memory is allocated in stress_process
        raise ValueError("Must have some cores to stress")


if __name__ == '__main__':
    _cmd_line()
