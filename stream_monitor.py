import requests
import time
import threading
import queue
import csv
import datetime
import argparse

def file_writer(q, filename):
    with open(filename, 'a', newline='') as f:
        writer = csv.writer(f)
        while True:
            data = q.get()
            if data is None:
                break
            writer.writerow(data)
            f.flush()
            q.task_done()

def console_writer(q):
    while True:
        measurement = q.get()
        if measurement is None:
            break
        print(measurement)
        q.task_done()

def monitor_stream(url, interval, chunk_size=8192, convert_to_kbps=False, silent=False, csv=False, filename='stream_monitor.csv'):
    file_queue = queue.Queue()
    console_queue = queue.Queue()
    writer_thread = threading.Thread(target=file_writer, args=(file_queue, filename), daemon=True)
    console_thread = threading.Thread(target=console_writer, args=(console_queue,), daemon=True)
    writer_thread.start()
    console_thread.start()

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            start_time = time.time()
            bytes_received = 0
            for chunk in r.iter_content(chunk_size=chunk_size):
                bytes_received += len(chunk)
                elapsed_time = time.time() - start_time
                if elapsed_time > interval:
                    if not silent:
                        measurement = f'Bytes received in last {interval} seconds: {bytes_received if convert_to_kbps else bytes_received * 8 / 1000:.2f} kB/s'
                        console_queue.put(measurement)
                    if csv:
                        date = datetime.datetime.now().isoformat()
                        file_queue.put((date, bytes_received if convert_to_kbps else bytes_received * 8 / 1000))
                    start_time = time.time()
                    bytes_received = 0
    except requests.exceptions.RequestException as e:
        print(f'Connection error: {e}')

    file_queue.put(None)
    console_queue.put(None)
    writer_thread.join()
    console_thread.join()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Monitor a stream and write measurements to a CSV file.')
    parser.add_argument('url', help='The URL of the stream to monitor.')
    parser.add_argument('-i', '--interval', type=int, default=1, help='The interval in seconds between measurements.')
    parser.add_argument('-c', '--chunk_size', type=int, default=8192,
                        help='The size of the chunks to read from the stream.')
    parser.add_argument('-k', '--convert_to_kbps', action='store_true',
                        help='Convert the measurements to kilobits per second.')
    parser.add_argument('-s', '--silent', action='store_true', help='Do not print the measurements to the console.')
    parser.add_argument('-v', '--csv', action='store_true', help='Write the measurements to a CSV file.')
    parser.add_argument('-f', '--filename', default='stream_monitor.csv', help='The name of the CSV file to write to.')
    args = parser.parse_args()

    monitor_stream(args.url, args.interval, args.chunk_size, args.convert_to_kbps, args.silent, args.csv, args.filename)