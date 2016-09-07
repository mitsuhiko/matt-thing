extern crate hyper;
extern crate serde_json as json;

use std::thread;
use std::sync::Mutex;
use std::sync::mpsc::{channel, Sender};
use std::collections::VecDeque;
use std::io::Write;

use hyper::method::Method;
use hyper::status::StatusCode;
use hyper::server::{Handler, Server, Request, Response};

const ID_BUFFER_SIZE : usize = 1024;


struct Hook {
    sender: Mutex<Sender<Vec<(Option<String>, String)>>>,
}

impl Handler for Hook {
    fn handle(&self, mut req: Request, mut resp: Response) {
        if req.method != Method::Post {
            *resp.status_mut() = StatusCode::MethodNotAllowed;
            return;
        }

        let rv : Result<Vec<json::Value>, _> = json::from_reader(&mut req);
        let item_iter = match rv {
            Ok(items) => items.into_iter(),
            Err(err) => {
                *resp.status_mut() = StatusCode::BadRequest;
                write!(resp.start().unwrap(), "error: {}\n", err).ok();
                return;
            }
        };

        let to_send : Vec<_> = item_iter.map(|o| (
            o.find("sg_message_id").and_then(|x| x.as_str()).map(|x| x.to_owned()),
            json::to_string(&o).unwrap()
        )).collect();
        self.sender.lock().unwrap().send(to_send).unwrap();
    }
}


pub fn serve() {
    let (tx, rx) = channel();

    thread::spawn(move || {
        let mut seen = VecDeque::with_capacity(ID_BUFFER_SIZE);
        while let Ok(batch) = rx.recv() {
            for (optional_msgid, item) in batch {
                if let Some(msgid) = optional_msgid {
                    if seen.iter().any(|x| x == &msgid) {
                        continue;
                    }
                    seen.push_back(msgid);
                    while seen.len() > ID_BUFFER_SIZE {
                        seen.pop_front();
                    }
                }
                println!("{}", &item);
            }
        }
    });

    Server::http("127.0.0.1:7001").unwrap().handle(Hook {
        sender: Mutex::new(tx),
    }).unwrap();
}
