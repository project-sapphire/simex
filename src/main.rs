#[macro_use]
extern crate log;
extern crate simplelog;
extern crate csv;

use std::collections::HashMap;
use std::vec::Vec;


#[derive(Clone, Debug)]
struct Order {
    from: String,
    to: String,
    amount: f64,
}

#[derive(Clone, Debug)]
struct Transaction {
    order: Order,
    complete: bool,
}


struct Exchange {
    time: u64,
    pending_transactions: HashMap<String, Transaction>,
    data: HashMap<String, Vec<f64>>,
}

impl Exchange {
    fn new() -> Exchange {
        Exchange {
            time: 0,
            pending_transactions: HashMap::new(),
            data: HashMap::new(),
        }
    }

    fn load_history(&mut self, currency: &str, file: &str) {
        let mut reader = csv::Reader::from_file(file)
            .expect("failed to open history file")
            .has_headers(false);

        self.data.insert(currency.to_string(), reader.decode().map(|row| {
            let (_, value): (String, f64) = row.unwrap(); value
        }).collect::<Vec<f64>>());

        info!("Loaded history for {}", currency.to_uppercase());
    }
}


fn main() {
    simplelog::TermLogger::init(log::LogLevelFilter::Trace, simplelog::Config::default());
    info!("Starting simulation exchange...");

    let mut exchange = Exchange::new();
    exchange.load_history("btc", "data/btc.csv");
    exchange.load_history("eth", "data/eth.csv");

    info!("Welcome to the SimEx simulation exchange!");
}
