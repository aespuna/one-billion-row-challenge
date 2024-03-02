use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};


fn main() {
    let file = File::open("data/measurements.txt").expect("file should be readable");
    let reader = BufReader::new(file);

    let start = Instant::now();
    let mut counter = 0u32;
    for _line in reader.lines() {
        // let line = line.expect("line should be read");
        counter += 1;
    }

    let duration = start.elapsed();
    eprintln!("Total lines processed: {}", counter);
    print_time(duration);
}

fn print_time(duration: Duration) {
    let seconds = duration.as_secs();
    let (minutes, seconds) = (seconds / 60, seconds % 60);
    eprintln!("It took {}m{}.{}s", minutes, seconds, duration.subsec_millis())
}
