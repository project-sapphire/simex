extern crate zmq;

use std::time::{Instant, Duration};
use std::fmt::Debug;

use super::prism;
use prism::Message;


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

    pub fn broadcast_rates(&self, rates: prism::RateUpdate) {
        rates.send(&self.publisher, 0);
    }

    pub fn pop_query(&self, deadline: Instant) -> Result<Option<prism::ExchangeRequest>, prism::ReceiveError> {
        // how much time is left until the deadline?
        let time_left = deadline - Instant::now();
        let time_left = time_left.as_secs() * 1000 + (time_left.subsec_nanos() / 1000000) as u64;
        trace!("Time-out in {}ms", time_left);

        // set timeout
        self.replier.set_rcvtimeo(time_left as i32);

        // receive, but time out no later than `deadline`
        let r = prism::ExchangeRequest::receive(&self.replier, 0);
        debug!("{:?}", &r);
        r
    }

    pub fn reply<T: Message + Debug>(&self, reply: &T) -> Result<(), zmq::Error> {
        debug!("Replying: {:?}", reply);
        reply.send(&self.replier, 0)
    }
}

