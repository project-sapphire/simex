extern crate csv;
extern crate json;
extern crate rand;

use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std;
use std::path::PathBuf;
use std::io::Read;
use std::convert::Into;
use std::str::FromStr;

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
    history_index: Vec<PathBuf>,
    currencies: HashSet<String>,
    current_data: HashMap<String, prism::Rate>, 
}

fn find_or_insert<'a, K, V>(map: &'a mut HashMap<K, V>, key: K) -> &'a mut V
    where K: Clone + std::cmp::Eq + std::hash::Hash, V: Default
{
    if !map.contains_key(&key) {
        map.insert(key.clone(), V::default());
    }

    map.get_mut(&key).unwrap()
}

impl Exchange {
    pub fn new(history_dir: &str) -> Exchange {
        let mut index = std::fs::read_dir(history_dir).unwrap().map(|x| x.unwrap().path()).collect::<Vec<PathBuf>>();
        index.sort();

        Exchange {
            time: 0,
            pending_transactions: HashMap::new(),
            history_index: index,
            currencies: HashSet::new(),
            current_data: HashMap::new(),
        }
    }

    /*pub fn load_history(&mut self, currency: &str, file: &str) {
        let mut reader = csv::Reader::from_file(file)
            .expect("failed to open history file")
            .has_headers(false);

        self.data.insert(currency.to_string(), reader.decode().map(|row| {
            let (_, value): (String, f64) = row.unwrap(); value
        }).collect::<Vec<f64>>());

        info!("Loaded history for {}", currency.to_uppercase());
    }*/

    pub fn get_currencies(&self) -> Vec<String> {
        self.currencies.iter().cloned().collect()
    }

    pub fn tick(&mut self) {
        self.current_data = self.load_data(self.time);

        for (ref currency, _) in &self.current_data {
            self.currencies.insert(currency.to_string());
        }

        self.time += 1;
    }

    fn load_data(&self, time: usize) -> HashMap<String, prism::Rate> {
        let filename = &self.history_index[time];
        let mut file = std::fs::File::open(filename).unwrap();
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents).unwrap();
        let data = json::parse(&file_contents).unwrap();

        let mut map = HashMap::new();

        for value in data.members() {
            let pair: &str = value["pair"].as_str().unwrap();
            let mut pair = pair.split('_').map(String::from).collect::<Vec<String>>();
            assert!(pair.len() == 2);
            
            let from = pair[0].to_lowercase();
            let to = pair[1].to_lowercase();
            let value = f64::from_str(value["rate"].as_str().unwrap()).unwrap();

            if from.len() != 3 || to.len() != 3 {
                continue;
            }

            let rate = find_or_insert::<String, prism::Rate>(&mut map, from.clone());
            rate.values.insert(to.clone(), value);
            trace!("Loaded {} 1.0 -> {} {:?}", from, to, value);
            trace!("{:?}", rate);
        }

        map
    }

    // TODO(deox): return Result<HashMap<String, f64>, String>!
    pub fn query(&mut self, currency: &str) -> Result<HashMap<String, f64>, String> {
        self.query_at(currency, self.time)
    }

    pub fn query_history(&self, currency: &str, age: u64) -> Vec<prism::Rate> {
        let mut history = Vec::new();
        for i in (self.time - (age / 1000) as usize)..self.time {
            history.push(prism::Rate {
                values: match self.query_at(currency, i) {
                    Err(_) => HashMap::new(),
                    Ok(x) => x,
                },
                timestamp: (i as u64) * 1000
            });
        }
        history
    }

    pub fn query_at(&self, currency: &str, time: usize) -> Result<HashMap<String, f64>, String> {
        if time == self.time {
            return match self.current_data.get(currency) {
                Some(ref x) => Ok(x.values.clone()),
                None => Err("data unavailable".to_string())
            };
        }

        return match self.load_data(time).get(currency) {
            Some(ref x) => Ok(x.values.clone()),
            None => Err("data unavailable".to_string())
        };
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

    pub fn finalize_transaction(&mut self, address: &str) -> (String, f64) {
        let transaction = match self.pending_transactions.get(address) {
            None => return ("xxx".to_string(), 0.0),
            Some(x) => x.clone(),
        };

        // TODO: maybe make sure that currency and amount match...?

        let amount = match self.query(&transaction.order.from) {
            Ok(data) => data.get(&transaction.order.to).unwrap() * transaction.order.amount,
            Err(_) => panic!("failed to finalize transaction!")
        };

        self.pending_transactions.insert(address.to_string(), Transaction {
            complete: true,
            order: transaction.order.clone(),
        });

        info!("Transaction {} finalized: {} {} to {} {}", address, transaction.order.from, transaction.order.amount, transaction.order.to, amount);
        (transaction.order.to, amount)
    }
}
