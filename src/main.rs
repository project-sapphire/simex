#[macro_use]
extern crate log;
extern crate simplelog;
extern crate prism;

mod com;
mod exchange;

use prism::Message;

use exchange::Exchange;


fn main() {
    simplelog::TermLogger::init(log::LogLevelFilter::Trace,
                                simplelog::Config::default()).unwrap();
    info!("Starting simulation exchange...");

    let mut exchange = Exchange::new();
    exchange.load_history("btc", "data/btc.csv");
    exchange.load_history("eth", "data/eth.csv");

    let coms = com::Communications::new("tcp://*:1337", "tcp://*:1338", "tcp://*:1339");

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
        while let Ok(Some(incoming)) = coms.receive(deadline) {
            match incoming {
                com::Incoming::Request(request) => {
                    info!("Received query: {:?} for {} on {}", request.query, request.currency, request.exchange);
                    match request.query {
                        prism::ExchangeQuery::History(age) => coms.reply(&exchange.query_history(&request.currency, age)),
                        prism::ExchangeQuery::Status(transaction) => panic!("`status' not implemented"),
                        prism::ExchangeQuery::Exchange(from, to, amount) => coms.reply(&exchange.initiate_transaction(&from, &to, amount)),
                    }.unwrap();
                },
                com::Incoming::Payment(address) => {
                    info!("Received payment: {}", address);
                    coms.confirm_payment(exchange.finalize_transaction(&address)).unwrap();
                }
            };
        }

        // sleep for the rest of the time
        let now = std::time::Instant::now();
        if now < deadline {
            std::thread::sleep(deadline - now);
        }

        exchange.tick();
    }
}
