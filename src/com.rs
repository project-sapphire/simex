extern crate zmq;

use super::Rates;


pub struct Communications {
    context: zmq::Context,
    publisher: zmq::Socket,
    replier: zmq::Socket,
}

impl Communications {
    pub fn new(publish_address: &str, reply_address: &str) -> Communications {
        let context = zmq::Context::new();
        
        let publisher = context.socket(zmq::PUB).unwrap();
        let replier = context.socket(zmq::REP).unwrap();

        publisher.bind(publish_address).unwrap();
        info!("Publisher listening on {}", publish_address);

        replier.bind(reply_address).unwrap();
        info!("Replier listening on {}", reply_address);

        Communications {
            context: context,
            publisher: publisher,
            replier: replier,
        }
    }

    pub fn broadcast_rates(&self, rates: Rates) {
        self.publisher.send_str(&rates.currency, 0);
    }
}

