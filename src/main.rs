#[macro_use]
extern crate log;
extern crate simplelog;
extern crate csv;
extern crate prism;

pub mod com;

use std::collections::HashMap;
use std::vec::Vec;

use prism::Message;


#[derive(Clone, Debug)]
pub struct Order {
    from: String,
    to: String,
    amount: f64,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    order: Order,
    complete: bool,
}

pub struct Exchange {
    time: usize,
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

    fn get_currencies(&self) -> Vec<String> {
        self.data.keys().map(|x|x.clone()).collect()
    }

    fn tick(&mut self) {
        self.time += 1;
    }

    // TODO(deox): return Result<HashMap<String, f64>, String>!
    fn query(&mut self, currency: &str) -> HashMap<String, f64> {
        self.query_at(currency, self.time)
    }

    fn query_history(&self, currency: &str, age: u64) -> Vec<prism::Rate> {
        let mut history = Vec::new();
        for i in (self.time - (age / 1000) as usize)..self.time {
            history.push(prism::Rate {
                values: self.query_at(currency, i),
                timestamp: (i as u64) * 1000
            });
        }
        history
    }

    fn query_at(&self, currency: &str, time: usize) -> HashMap<String, f64> {
        let mut map = HashMap::new();

        // TODO(deox): use try!
        let reference_value = self.data.get(currency)
            .unwrap().get(self.time).unwrap();
        
        for other_currency in self.get_currencies() {
            if other_currency == currency {
                continue;
            }

            // TODO(deox): use try!
            let value = self.data.get(&other_currency)
                .unwrap().get(self.time).unwrap();

            map.insert(other_currency, reference_value / value);
        }

        map
    }
}


fn main() {
    simplelog::TermLogger::init(log::LogLevelFilter::Trace,
                                simplelog::Config::default()).unwrap();
    info!("Starting simulation exchange...");

    let mut exchange = Exchange::new();
    exchange.load_history("btc", "data/btc.csv");
    exchange.load_history("eth", "data/eth.csv");
    exchange.load_history("xrp", "data/eth.csv");

    let coms = com::Communications::new("tcp://*:1337", "tcp://*:1338");

    info!("Welcome to the SimEx simulation exchange!");


    loop {
        for currency in exchange.get_currencies() {
            let rates = prism::RateUpdate {
                exchange: "9ef31d1e-0d44-444f-b3f9-32ef34156d1d".to_string(),
                currency: currency.clone(),
                rate: prism::Rate {
                    values: exchange.query(&currency),
                    timestamp: exchange.time as u64,
                },
            };

            debug!("Broadcasting {:?}", &rates);

            coms.broadcast_rates(rates);
        }

        // handle 
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

        // this fucks out when we've read one because the socket returns
        // an error immediately
        while let Ok(Some(query)) = coms.pop_query(deadline) {
            info!("Received query: {:?}", query);

            coms.reply(&match query {
                prism::ExchangeQuery::History => exchange.query_history("btc", 5000),
                _ => continue
            }).unwrap();
        }

        // sleep for the rest of the time
        let now = std::time::Instant::now();
        if now < deadline {
            std::thread::sleep(deadline - now);
        }


        exchange.tick();
    }
}
