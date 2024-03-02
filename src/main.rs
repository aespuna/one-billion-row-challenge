use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{stdout, BufRead, BufReader, BufWriter, Read, Write};
use std::str::FromStr;
use std::time::{Duration, Instant};

const MEASUREMENTS_FILE_PATH: &str = "data/measurements.txt";

/// TODO: Add tests some edge cases for 5, 100, 10K rows (modify the script that generates it)
/// TODO: The expectation is to have Golden Master tests (e2e)
/// TODO: Make them work on github CI
/// Strategies (depending on the flamegraph):
///   - Avoid String allocation for the keys of the map, (just once on the structure)
///   - Try to add some parallelism

struct AggregatedMeasurement {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl AggregatedMeasurement {
    fn start_with(measurement: f64) -> AggregatedMeasurement {
        AggregatedMeasurement {
            min: measurement,
            max: measurement,
            sum: measurement,
            count: 1,
        }
    }

    fn add(&mut self, another_measurement: f64) {
        self.count += 1;
        self.sum += another_measurement;

        self.min = self.min.min(another_measurement);
        self.max = self.max.max(another_measurement);
    }

    fn mean(&self) -> f64 {
        self.sum / self.count as f64
    }
}

struct StationMeasurements {
    name: String,
    measurement: AggregatedMeasurement,
}

fn main() {
    let start = Instant::now();

    let filename = env::args()
        .nth(1)
        .unwrap_or(MEASUREMENTS_FILE_PATH.to_string());

    let file = File::open(filename).expect("file should be readable");
    let (lines_processed, stations) = challenge(BufReader::new(file), BufWriter::new(stdout()));

    let duration = start.elapsed();
    eprintln!("Total lines processed: {}", lines_processed);
    eprintln!("Total stations with measurements: {}", stations);
    print_time(duration);
}

fn challenge(mut input: BufReader<impl Read>, output: BufWriter<impl Write>) -> (usize, usize) {
    let mut buffer = String::with_capacity(1024);
    let mut measurements_by_station = HashMap::<String, StationMeasurements>::with_capacity(10_000);

    let mut counter = 0u32;
    while input
        .read_line(&mut buffer)
        .expect("line reading won't fail")
        > 0
    {
        process_line(&buffer).map(|(name, measurement)| {
            measurements_by_station
                .entry(name.into())
                .and_modify(|measurements| measurements.measurement.add(measurement))
                .or_insert(StationMeasurements {
                    name: name.into(),
                    measurement: AggregatedMeasurement::start_with(measurement),
                })
        });

        counter += 1;
        buffer.clear();
    }

    let sorted_results = sort_results(&measurements_by_station);
    write_results(&sorted_results, output);
    (counter as usize, sorted_results.len())
}

fn sort_results(
    measurements_by_station: &HashMap<String, StationMeasurements>,
) -> Vec<&StationMeasurements> {
    let mut ordered_by_name = measurements_by_station.values().collect::<Vec<_>>();

    ordered_by_name.sort_by(|&a, &b| a.name.cmp(&b.name));

    ordered_by_name
}

fn write_results(sorted_results: &[&StationMeasurements], mut output: BufWriter<impl Write>) {
    for &result in sorted_results.iter() {
        let measurement = &result.measurement;
        writeln!(
            output,
            "{}={:.1}/{:.1}/{:.1}",
            result.name,
            measurement.min,
            measurement.mean() + 0.001, // To make .X5 to round up
            measurement.max
        )
        .expect("write to stdout worked")
    }
}

fn process_line(line: &str) -> Option<(&str, f64)> {
    line.trim_end()
        .split_once(';')
        .map(|(name, measurement)| (name, f64::from_str(measurement).expect("it is a float")))
}

fn print_time(duration: Duration) {
    let seconds = duration.as_secs();
    let (minutes, seconds) = (seconds / 60, seconds % 60);
    eprintln!(
        "It took {}m{}.{}s",
        minutes,
        seconds,
        duration.subsec_millis()
    )
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_process_line() {
        let line = "station1;1.0";
        let (name, measurement) = process_line(line).unwrap();
        assert_eq!(name, "station1");
        assert_eq!(measurement, 1.0);
    }

    #[test]
    fn test_aggregated_measurement() {
        let mut measurement = AggregatedMeasurement::start_with(5.0);
        measurement.add(20.0);
        measurement.add(-5.0);
        measurement.add(-15.0);
        assert_eq!(measurement.min, -15.0);
        assert_eq!(measurement.max, 20.0);
        assert_eq!(measurement.sum, 5.0);
        assert_eq!(measurement.count, 4);
        assert_eq!(measurement.mean(), 1.25);
        assert_eq!(format!("{:.1}", 1.251), "1.3");
    }

    #[test]
    fn test_challenge() {
        for &test_name in &[
            "data/test/measurements-1",
            "data/test/measurements-2",
            "data/test/measurements-3",
            "data/test/measurements-10",
            "data/test/measurements-20",
            "data/test/measurements-10000-unique-keys",
            "data/test/measurements-boundaries",
            "data/test/measurements-complex-utf8",
            "data/test/measurements-dot",
            "data/test/measurements-rounding",
            "data/test/measurements-short",
            "data/test/measurements-shortest",
        ] {
            test_single_challenge(test_name);
        }
    }

    fn test_single_challenge(test_name: &str) {
        let input = File::open(format!("{}.in", test_name)).unwrap();
        let expected_output = read_expected_file(test_name);

        let mut actual_output: Vec<u8> = Vec::new();
        challenge(BufReader::new(input), BufWriter::new(&mut actual_output));
        assert_eq!(actual_output, expected_output, "{}", test_name);
    }

    fn read_expected_file(test_name: &str) -> Vec<u8> {
        let mut file = File::open(format!("{}.out", test_name)).unwrap();
        let mut expectation = Vec::new();
        file.read_to_end(&mut expectation)
            .expect("reading the file should work");
        expectation
    }
}
