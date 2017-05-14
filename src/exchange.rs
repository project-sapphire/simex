extern crate csv;
extern crate rand;

use std::collections::HashMap;
use std::vec::Vec;

use self::rand::Rng;

use ::prism;


#[derive(Clone, Debug)]
pub struct Order {
    from: String,
    to: String,
    amount: f64,
    destination: String,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    order: Order,
    complete: bool,
}

pub struct Exchange {
    pub time: usize,
    pending_transactions: HashMap<String, Transaction>,
    data: HashMap<String, Vec<f64>>,
}

impl Exchange {
    pub fn new() -> Exchange {
        Exchange {
            time: 0,
            pending_transactions: HashMap::new(),
            data: HashMap::new(),
        }
    }

    pub fn load_history(&mut self, currency: &str, file: &str) {
        let mut reader = csv::Reader::from_file(file)
            .expect("failed to open history file")
            .has_headers(false);

        self.data.insert(currency.to_string(), reader.decode().map(|row| {
            let (_, value): (String, f64) = row.unwrap(); value
        }).collect::<Vec<f64>>());

        info!("Loaded history for {}", currency.to_uppercase());
    }

    pub fn get_currencies(&self) -> Vec<String> {
        self.data.keys().map(|x|x.clone()).collect()
    }

    pub fn tick(&mut self) {
        self.time += 1;
    }

    // TODO(deox): return Result<HashMap<String, f64>, String>!
    pub fn query(&mut self, currency: &str) -> HashMap<String, f64> {
        self.query_at(currency, self.time)
    }

    pub fn query_history(&self, currency: &str, age: u64) -> Vec<prism::Rate> {
        let mut history = Vec::new();
        for i in (self.time - (age / 1000) as usize)..self.time {
            history.push(prism::Rate {
                values: self.query_at(currency, i),
                timestamp: (i as u64) * 1000
            });
        }
        history
    }

    pub fn query_at(&self, currency: &str, time: usize) -> HashMap<String, f64> {
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

    pub fn initiate_transaction(&mut self, from: &str, to: &str, amount: f64, destination: &str) -> prism::Invoice {
        // create random fake address
        let mut rng = rand::StdRng::new().unwrap();
        let address: String = rng.gen_ascii_chars().take(16).collect();

        info!("Initiating transaction {}: {} {} to {}", address, from.to_uppercase(), amount, to.to_uppercase());

        self.pending_transactions.insert(address.to_string(), Transaction {
            order: Order {
                from: from.to_string(),
                to: to.to_string(),
                amount: amount,
                destination: destination.to_string(),
            },
            complete: false,
        });

        prism::Invoice {
            address: address.to_string(),
            currency: from.to_string(),
            amount: amount,
        }
    }

    pub fn finalize_transaction(&mut self, address: &str) -> f64 {
        let transaction = match self.pending_transactions.get(address) {
            None => return 0.0,
            Some(x) => x.clone(),
        };

        // TODO: maybe make sure that currency and amount match...?

        self.pending_transactions.insert(address.to_string(), Transaction {
            complete: true,
            order: transaction.order.clone(),
        });

        let amount = self.query(&transaction.order.from).get(&transaction.order.to).unwrap() * transaction.order.amount;
        info!("Transaction {} finalized: {} {} to {} {}", address, transaction.order.from, transaction.order.amount, transaction.order.to, amount);
        amount
    }
}
