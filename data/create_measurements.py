#!/usr/bin/env python
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Based on the script at https://github.com/gunnarmorling/1brc/

import os
import sys
import random
import time


def exit_with_help():
    """
    Print out usage and exit
    """
    print("Usage:  create_measurements.py <number_of_records> [output_file]")
    print(
        "        You can use underscore notation for large number of records.")
    print("        For example:  1_000_000_000 for one billion")
    exit()


def parse_args():
    args = sys.argv[1:]
    if len(args) < 1:
        exit_with_help()
    if len(args) == 1:
        return int(args[0]), None
    if len(args) == 2:
        return int(args[0]), args[1]

    exit_with_help()


def read_weather_station_data():
    """
    Grabs the weather station names from example data provided in repo and dedups
    """
    stations = {}
    with open('./weather_stations.csv', 'r') as file:
        for station in file:
            if "#" in station:
                continue

            name, temp = station.split(';')
            stations[name] = float(temp)
    return stations


def format_bytes(num):
    """
    Format bytes to a human-readable format (e.g., KiB, MiB, GiB)
    """
    for x in ['bytes', 'KiB', 'MiB', 'GiB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds):
    """
    Format elapsed time in a human-readable format
    """
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


def estimate_file_size(weather_station_names, num_rows_to_create):
    """
    Tries to estimate how large a file the test data will be
    """
    longest_record_station_name = max(len(x) for x in weather_station_names) or 0
    max_record_size = longest_record_station_name + len(";-99.9")

    total_file_size = num_rows_to_create * max_record_size
    human_file_size = format_bytes(total_file_size)

    return (f"Estimated max file size is:  {human_file_size}.\n"
            f"True size is probably smaller.")


def write_progress(percentage):
    filled = '=' * (int(percentage * 50) // 100)
    sys.stdout.write("\r[%-50s] %d%%" % (filled, percentage))
    sys.stdout.flush()


def build_test_data(weather_stations, num_rows_to_create, output_file=None):
    """
    Generates and writes to file the requested length of test data.
    The maximum number of stations used is 10,000.
    """
    if output_file is None:
        output_file = "measurements.txt"

    start_time = time.time()
    batch_size = min(num_rows_to_create, 10_000)
    weather_station_names = list(weather_stations.keys())
    station_names_10k_max = random.choices(weather_station_names, k=batch_size)
    progress_step = max(1, (num_rows_to_create // batch_size) // 100)
    print('Building test data...')

    try:
        with open(output_file, 'w') as file:
            for s in range(0, num_rows_to_create // batch_size):

                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = '\n'.join(
                    [
                        f"{station};{random.gauss(weather_stations[station], 10):.1f}"
                        for station in batch])
                file.write(prepped_deviated_batch + '\n')

                # Update progress bar every 1%
                if s % progress_step == 0:
                    write_progress((s * batch_size + 1) / num_rows_to_create * 100)

            write_progress(100)
        sys.stdout.write('\n')
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        exit()

    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize(output_file)
    human_file_size = format_bytes(file_size)

    print(f"Test data successfully written to {output_file}.")
    print(f"Actual file size: {human_file_size}")
    print(f"Elapsed time: {format_elapsed_time(elapsed_time)}")


def main():
    """
    main program function
    """
    num_rows_to_create, output_file = parse_args()
    weather_station_names = read_weather_station_data()
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(weather_station_names, num_rows_to_create, output_file)
    print("Test data build complete.")


if __name__ == "__main__":
    main()
exit()
