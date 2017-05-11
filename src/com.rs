extern crate zmq;

use std::time::{Instant, Duration};
use std::fmt::Debug;
use std::boxed::Box;

use super::prism;
use prism::Message;


pub struct Communications {
    context: zmq::Context,
    publisher: zmq::Socket,
    replier: zmq::Socket,
    backdoor: zmq::Socket,
}

pub enum Incoming {
    Request(prism::ExchangeRequest),
    Payment(String),
}

impl Communications {
    pub fn new(publish_address: &str, reply_address: &str, backdoor_address: &str) -> Communications {
        let context = zmq::Context::new();
        
        let publisher = context.socket(zmq::PUB).unwrap();
        let replier = context.socket(zmq::REP).unwrap();
        let backdoor = context.socket(zmq::REP).unwrap();

        publisher.bind(publish_address).unwrap();
        info!("Publisher listening on {}", publish_address);

        replier.bind(reply_address).unwrap();
        info!("Replier listening on {}", reply_address);

        backdoor.bind(backdoor_address).unwrap();
        info!("Backdoor listening on {}", backdoor_address);

        Communications {
            context: context,
            publisher: publisher,
            replier: replier,
            backdoor: backdoor,
        }
    }

    pub fn broadcast_rates(&self, rates: prism::RateUpdate) {
        rates.send(&self.publisher, 0);
    }

    pub fn receive(&self, deadline: Instant) -> Result<Option<Incoming>, prism::ReceiveError> {
        // how much time is left until the deadline?
        let time_left = deadline - Instant::now();
        let time_left = time_left.as_secs() * 1000 + (time_left.subsec_nanos() / 1000000) as u64;
        trace!("Time-out in {}ms", time_left);

        //self.replier.set_rcvtimeo(time_left as i32);
        
        let mut items = vec![
            self.replier.as_poll_item(zmq::POLLIN),
            self.backdoor.as_poll_item(zmq::POLLIN), 
        ];

        zmq::poll(&mut items, time_left as i64);

        // replier
        if items[0].is_readable() {
            let r = match prism::ExchangeRequest::receive(&self.replier, 0)? {
                None => return Ok(None),
                Some(x) => x
            };

            debug!("REQUEST: {:?}", &r);
            return Ok(Some(Incoming::Request(r)));
        }

        // payment
        if items[1].is_readable() {
            let r = self.backdoor.recv_string(0)??;
            debug!("PAYMENT: {:?}", &r);
            return Ok(Some(Incoming::Payment(r)));
        }

        Ok(None)
    }

    pub fn reply<T: Message + Debug>(&self, reply: &T) -> Result<(), zmq::Error> {
        debug!("Replying: {:?}", reply);
        reply.send(&self.replier, 0)
    }

    pub fn confirm_payment(&self, amount: f64) -> Result<(), zmq::Error> {
        debug!("Confirming payment: {:?}", amount);
        amount.send(&self.backdoor, 0)
    }
}

