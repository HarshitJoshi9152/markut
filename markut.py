#!/usr/bin/env python

import csv
import multiprocessing
import sys
import subprocess
import argparse
import time
from multiprocessing import Process

def ts_to_secs(ts):
    comps = ts.split(':');
    assert len(comps) == 3;
    return 60 * 60 * int(comps[0]) + 60 * int(comps[1]) + int(comps[2]);

def secs_to_ts(secs):
    return f'{secs//60//60:02}:{secs//60%60:02}:{secs%60:02}';

def ffmpeg_cut_chunk(input_path, start_secs, duration_secs, output_path):
    cli = ['./ffmpeg',
           '-ss', str(start_secs),
           '-i', input_path,
           '-c', 'copy',
           '-t', str(duration_secs),
           output_path];
    subprocess.run(cli)

def ffmpeg_concat_chunks(list_path, output_path):
    cli = ['./ffmpeg',
           '-f', 'concat',
           '-safe', '0',
           '-i', list_path,
           '-c', 'copy',
           output_path]
    subprocess.run(cli)

def ffmpeg_generate_concat_list(chunk_names, output_path):
    with open(output_path, 'w') as f:
        for name in chunk_names:
            f.write(f"file '{name}'\n")

def load_ts_from_file(path, delay):
    with open(path, newline='') as csvfile:
        return [ts_to_secs(row[0]) + delay
                for row in csv.reader(csvfile)];


if __name__ == '__main__':
    #  ./markut.py -c download.csv -i filename.mp4 -d 4
    parser = argparse.ArgumentParser();
    parser.add_argument('-c', dest='csv', required=True, metavar='CSV');
    parser.add_argument('-i', dest='input', required=True, metavar='INPUT');
    parser.add_argument('-d', dest='delay', required=True, metavar='DELAY_SECS');
    args = parser.parse_args();
    ts = load_ts_from_file(args.csv, int(args.delay));
    n = len(ts)
    assert n % 2 == 0

    processes = []
    start_time = time.perf_counter();

    secs = 0;
    cuts_ts = [];
    logs = [];

    for i in range(0, n // 2):
        start    = ts[i * 2 + 0]
        end      = ts[i * 2 + 1]
        duration = end - start
        secs    += duration
        cuts_ts.append(secs_to_ts(secs));
        start_time = time.perf_counter()
        ffmpeg_cut_chunk(args.input, start, duration, f'chunk-{i:02}.mp4')
        logs.append(f"time takeen {time.perf_counter() - start_time} seconds")
        # spawn process.
        # p = Process(target=ffmpeg_cut_chunk, args = [args.input, start, duration, f'chunk-{i:02}.mp4'])
        # p.start()
        # start_time = time.perf_counter()
        # processes.append({
        #     "process":p,
        #     "start_time":start_time
        # })

    ourlist_path = 'ourlist.txt'
    chunk_names = [f'chunk-{i:02}.mp4' for i in range(0, n // 2)]

    ffmpeg_generate_concat_list(chunk_names, ourlist_path);
    # wait until chunks have been generated.
    # for process in processes:
    #     s = process["start_time"]
    #     process["process"].join()
    #     logs.append(f"time takeen {time.perf_counter() - s} seconds")

    ffmpeg_concat_chunks(ourlist_path, "output.mp4")
    
    print(f'\x1b[93m\nTIME TAKEN: {time.perf_counter() - start_time} seconds\x1b[0m');

    print("\n".join(logs));

    print("Timestamps of cuts:");
    for ts in cuts_ts:
        print(ts)

# perf
"""
TIME TAKEN: 4.124831271999938 seconds

rm chunk-*.mp4 output.mp4

./markut.py -c test2/ts.csv -i '[pseudo] Rick and Morty S01E10 Close Rick-counters of the Rick Kind [BDRip] [1080p] [h.265].mkv' -d 0

time_taken: 0.666997985 seconds
TIME TAKEN: 0.6350990590000001 seconds : multiprocessing.

new 

TIME TAKEN: 4.978266584999999 seconds : without multiprocessing
TIME TAKEN: 6.456984531 seconds : multiprocessing

brutal level (72 timestamps, 36 cuts)

TIME TAKEN: 49.467330585999996 seconds : without multiprocessing
TIME TAKEN: 34.310738132 seconds : with mutiprocessing
TIME TAKEN: 26.091401449 seconds : with multiproccessing
TIME TAKEN: 23.105684441 seconds : without multiprocessing ! WWWwwhhhhhhhaaaaaatttttt !!!!
c137


chunk timings | with multiprocessing (TO BE FAIR I THINK THESE ARE INCORRECT FOR MULTIPROCESSING)

time takeen 1.674626257 seconds
time takeen 1.6737905190000002 seconds
time takeen 1.672702251 seconds
time takeen 1.6712394210000001 seconds
time takeen 2.834075463 seconds
time takeen 10.161229239 seconds
time takeen 10.1564454 seconds
time takeen 10.142239400000001 seconds
time takeen 10.140821225 seconds
time takeen 10.139422297 seconds
time takeen 10.137902879 seconds
time takeen 10.135844909 seconds
time takeen 10.125925769 seconds
time takeen 10.12003634 seconds
time takeen 10.109604824 seconds
time takeen 10.104341332 seconds
time takeen 10.103009773 seconds
time takeen 10.101797933 seconds
time takeen 10.100456749 seconds
time takeen 10.099162145000001 seconds
time takeen 10.097769376999999 seconds
time takeen 10.096334650000001 seconds
time takeen 10.094889527 seconds
time takeen 10.093480591 seconds
time takeen 10.091982731000002 seconds
time takeen 10.090667724000001 seconds
time takeen 10.078687919 seconds
time takeen 10.07627194 seconds
time takeen 10.072754218999998 seconds
time takeen 10.070825594 seconds
time takeen 10.069321189 seconds
time takeen 10.067861439000001 seconds
time takeen 10.066540658 seconds
time takeen 10.027902321 seconds
time takeen 10.026210444 seconds
time takeen 10.012618446 seconds

chunk timings | without multiprocessing | sum of all time is 9.059824348 (sum([float(i) for i in c]));
TIME TAKEN: 23.105684441 seconds

time takeen 0.132094291 seconds
time takeen 0.12325956999999998 seconds
time takeen 0.11375348699999999 seconds
time takeen 0.111800609 seconds
time takeen 0.20108980799999998 seconds
time takeen 0.2342275199999999 seconds
time takeen 0.10029006399999996 seconds
time takeen 0.140359664 seconds
time takeen 0.1284664740000001 seconds
time takeen 0.10642008999999986 seconds
time takeen 0.11733241499999991 seconds
time takeen 0.11696285799999995 seconds
time takeen 0.13679189899999988 seconds
time takeen 0.2022589170000002 seconds
time takeen 0.2587006609999998 seconds
time takeen 0.10327731600000023 seconds
time takeen 0.14272174900000012 seconds
time takeen 0.1315118540000002 seconds
time takeen 0.12825166799999987 seconds
time takeen 0.12931260499999997 seconds
time takeen 0.1334527969999999 seconds
time takeen 0.13788016700000005 seconds
time takeen 0.21271391500000014 seconds
time takeen 0.2369664760000001 seconds
time takeen 0.11651707899999986 seconds
time takeen 0.4235246110000004 seconds
time takeen 0.27952827600000063 seconds
time takeen 0.24039334799999956 seconds
time takeen 0.42247599299999994 seconds
time takeen 0.273660789 seconds
time takeen 0.3072712260000001 seconds
time takeen 0.8015367409999996 seconds
time takeen 1.3813192800000005 seconds
time takeen 0.11091559699999909 seconds
time takeen 0.4548549580000003 seconds
time takeen 0.6679295759999988 seconds

"""