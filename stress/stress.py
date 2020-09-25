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
import tempfile


def _spin(ram_per_core_mb):
    """
    Spin in an infinite loop, consuming up to ram_per_core_mb.

    :param ram_per_core_mb: How many MB of RAM to consume in loop.
    :return: None
    """
    me = psutil.Process()

    steps = 8
    ram_per_step_mb = int(ram_per_core_mb / steps)

    dummy = list([1.0])
    while True:
        # Allocate RAM until resident set size (rss) reaches target
        if me.memory_info().rss < ram_per_core_mb * 1024 * 1024:
            try:
                num = 1.0
                dummy += [num] * int(ram_per_step_mb / sys.getsizeof(num) * 1024 * 1024)

            except MemoryError:
                # We didn't have enough RAM for our attempt, so we will recursively try
                # smaller amounts 10% smaller at a time
                ram_per_step_mb = int(ram_per_step_mb * 0.8)

        # Do something with dummy to try and keep it in active RAM
        for i, n in enumerate(dummy):
            dummy[i] += 1.010101


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
    parser.add_argument("-w", "--write",
                        help="MB of data to write to disk per second")
    parser.add_argument("--max",
                        help="Max file size to write with -w (MB)")

    return parser.parse_args()


def stress_processes(num_processes, ram_per_core_mb):
    """
    Starts a given number infinite processing loops. Pauses briefly between
    starting each process, and will restart processes that are killed by the
    OS. Does not return.

    :param num_processes: the number of processes to spin
    :param ram_per_core_mb: amount of RAM each process should try and use
    :return: None
    """

    for i in range(num_processes):
        p = multiprocessing.Process(target=_spin, args=(ram_per_core_mb,))
        p.start()
        print(f"Started process #{i} ({p.pid}) at {time.asctime()}")
        time.sleep(1)

    # Now wait, and check children. If it appears one has been killed,
    # respawn
    while True:
        children = multiprocessing.active_children()
        if len(children) < num_processes:
            p = multiprocessing.Process(target=_spin, args=(ram_per_core_mb,))
            p.start()
            print(f"Only {len(children)}/{num_processes} running - respawned {p.pid}")

        # This is parent process of CPU stressors -- don't thrash CPU
        time.sleep(2)


def stress_io(write_mb_per_second, max_file_size_mb=None):
    """
    Writes a given number of MB per second to a temporary file, until
    max_file_size is reached or disk space exhausted.

    :param write_mb_per_second: MB of data to write per second (or try to)
    :param max_file_size: Written file won't exceed this. None is "unlimited"
    :return: None
    """
    tf = tempfile.NamedTemporaryFile(prefix="stress_io_", suffix=".tmp")
    print(f"Writing {write_mb_per_second:,} MB/s to file: {tf.name} (max={max_file_size_mb:,} MB)")

    write_bytes = write_mb_per_second * 1024 * 1024
    max_bytes = None
    if max_file_size_mb:
        max_bytes = max_file_size_mb * 1024 * 1024

    try:
        bytes_written = 0
        while True:
            if max_bytes and bytes_written >= max_bytes:
                tf.seek(0)

            tf.write(b'0' * write_bytes)
            bytes_written += write_bytes
            time.sleep(1)

    finally:
        tf.close()


def _cmd_line():
    """
    The command line entry point for this script.

    :return: None
    """

    arguments = _parse_args()
    cores_to_stress = int(arguments.cores) if arguments.cores else None
    memory_to_allocate = int(arguments.memory) if arguments.memory else None
    io_to_stress = int(arguments.write) if arguments.write else None
    max_file_size = int(arguments.max) if arguments.max else None

    # This eats the interrupted exception when the process is terminated
    signal.signal(signal.SIGINT, lambda x, y: sys.exit(1))

    if not cores_to_stress:
        cores_to_stress = multiprocessing.cpu_count()
    if not memory_to_allocate:
        memory_to_allocate = psutil.virtual_memory().total / 1024 / 1024

    memory_per_core = int(memory_to_allocate / cores_to_stress)

    if cores_to_stress:
        # The CPU stress has to happen in another process since it is infinitely
        # CPU hungry. Each process will consume RAM.
        print(f"Starting {cores_to_stress} CPU/RAM stressors which will consume up to {memory_per_core:,} MB each")
        multiprocessing.Process(target=stress_processes, args=(cores_to_stress, memory_per_core,)).start()

    if io_to_stress:
        # IO stress also in another process, constantly writing to disk
        print(f"Starting IO stressor which will write {io_to_stress:,} MB/s")
        multiprocessing.Process(target=stress_io, args=(io_to_stress, max_file_size)).start()


if __name__ == '__main__':
    _cmd_line()
