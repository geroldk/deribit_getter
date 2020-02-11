use chrono::prelude::*;
//use env_logger;
//use log::{debug, error, info};
use log;
use slog::{slog_o, Duplicate};
use slog_term;
use slog_async;
use slog_stdlog;
use slog_scope::{info, error, warn, debug};
use slog::Drain;
use slog_journald;
use slog_envlogger;

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use ws::{connect, CloseCode, Error, Handler, Handshake, Message, Result, Sender};


const URL: &str = "wss://www.deribit.com/ws/api/v2";

const INDEX: [&str; 2] = ["btc_usd", "ethc_usd"];

#[derive(Serialize, Deserialize, Debug)]
struct Currencies {
    result: Vec<Currency>
}

#[derive(Serialize, Deserialize, Debug)]
struct Currency {
    currency: String
}

#[derive(Serialize, Deserialize, Debug)]
struct Instruments {
    result: Vec<Instrument>
}

#[derive(Serialize, Deserialize, Debug)]
struct Instrument {
    instrument_name: String
}

struct Symbol(String);

struct Client<'a> {
    out: Sender,
    instruments: &'a [String],
    fs_ts: Option<String>,
    file: Option<File>,
}

impl Client<'_> {
    fn new(out: Sender, instruments: &[String]) -> Client {
        Client {
            out,
            instruments,
            fs_ts: None,
            file: None,
        }
    }

    fn build_file_name(ts: &str) -> String {
        let n = format!("data/deribit-ws-{}.log", ts);
        info!("{}", n);
        n
    }
    fn create_file(name: &str) -> File {
        let path = Path::new(name);
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap()
    }

    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let utc: DateTime<Utc> = Utc::now();
        let fs_ts = utc.format("%Y-%m-%d_%HZ").to_string();

        match self.fs_ts.as_ref() {
            Some(x) => {
                if x == &fs_ts {
                    //self.file.as_mut().unwrap().write(buf)
                } else {
                    debug!("timestamp: {}", fs_ts);
                    let f = self.file.as_mut().unwrap();

                    f.flush().unwrap();
                    f.sync_all().unwrap();

                    self.fs_ts = Some(fs_ts);
                    self.file = Some(Client::create_file(&Client::build_file_name(
                        &self.fs_ts.as_ref().unwrap(),
                    )));
                    //self.file.as_mut().unwrap().write(buf)
                }
            }
            None => {
                debug!("timestamp: {}", fs_ts);

                self.fs_ts = Some(fs_ts);
                self.file = Some(Client::create_file(&Client::build_file_name(
                    &self.fs_ts.as_ref().unwrap(),
                )));
                //self.file.as_mut().unwrap().write(buf)
            }
        };
        self.file.as_mut().unwrap().write(buf)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribeMessageData {
    channel: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribeMessage {
    event: String,
    data: SubscribeMessageData,
}

fn subscribe_book(instrument:&[String]) -> String {
    let i = instrument.iter().map(|x|format!("book.{}.raw",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 2, \"params\": {{
        \"channels\": {:?}
        }}
    }}", i)
}

fn subscribe_perpetual(instrument:&[String]) -> String {
    let i = instrument.iter().map(|x|format!("perpetual.{}.raw",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 3, \"params\": {{
        \"channels\": {:?}
        }}
    }}", i)
}

fn subscribe_trades(instrument:&[String]) -> String {
    let i = instrument.iter().map(|x|format!("trades.{}.raw",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 4, \"params\": {{
        \"channels\": {:?}
        }}
    }}", i)
}

fn subscribe_deribit_price_index(instrument:&[&str]) -> String {
    let deribit_price_index = instrument.iter().map(|x|format!("deribit_price_index.{}",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 5, \"params\": {{
        \"channels\": {:?}
        }}
    }}", deribit_price_index)
}

fn subscribe_deribit_price_ranking(instrument:&[&str]) -> String {
    let deribit_price_ranking = instrument.iter().map(|x|format!("deribit_price_ranking.{}",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 6, \"params\": {{
        \"channels\": {:?}
        }}
    }}", deribit_price_ranking)
}

fn subscribe_estimated_expiration_price(instrument:&[&str]) -> String {
    let estimated_expiration_price = instrument.iter().map(|x|format!("estimated_expiration_price.{}",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 7, \"params\": {{
        \"channels\": {:?}
        }}
    }}", estimated_expiration_price)
}

fn subscribe_markprice_options(instrument:&[&str]) -> String {
    let markprice_options = instrument.iter().map(|x|format!("markprice.options.{}",x)).collect::<Vec<String>>();
    format!("{{\"jsonrpc\":\"2.0\", \"method\":\"public/subscribe\", \"id\": 8, \"params\": {{
        \"channels\": {:?}
        }}
    }}", markprice_options)
}


impl Handler for Client<'_> {
    fn on_open(&mut self, shake: Handshake) -> Result<()> {
        if let Some(addr) = shake.remote_addr()? {
            info!("Connection with {} now open", addr);
        }
        self.out.send("{\"method\":\"public/subscribe\",\"params\": {\"channels\": [\"announcements\"]},\"jsonrpc\": \"2.0\",\"id\": 1}")?;
        let book = subscribe_book(self.instruments);
        //println!("{}", m);
        self.out.send(book)?;
        let perpetual = subscribe_perpetual(self.instruments);
        self.out.send(perpetual)?;
        let trades = subscribe_trades(self.instruments);
        self.out.send(trades)?;
        let deribit_price_index = subscribe_deribit_price_index(INDEX.to_vec().as_slice());
        self.out.send(deribit_price_index)?;
        let deribit_price_ranking = subscribe_deribit_price_ranking(INDEX.to_vec().as_slice());
        self.out.send(deribit_price_ranking)?;
        let estimated_expiration_price = subscribe_estimated_expiration_price(INDEX.to_vec().as_slice());
        self.out.send(estimated_expiration_price)?;
        let markprice_options = subscribe_markprice_options(INDEX.to_vec().as_slice());
        self.out.send(markprice_options)?;




        info!("subscribed");
        Ok(())
    }
    fn on_message(&mut self, msg: Message) -> Result<()> {
        //println!("{:?}", msg);
        let utc: DateTime<Utc> = Utc::now();
        if let Message::Text(s) = msg {
            let ts = format!("{:?}", utc);
            let mut mm = String::with_capacity(s.len() + ts.len() + 3);
            mm.push_str(&ts);
            mm.push_str(", ");
            mm.push_str(&s);
            mm.push_str("\n");
            self.write(mm.as_bytes()).unwrap();
        } else {
            error!("{:?}", msg);
        }
        Ok(())
    }
    fn on_close(&mut self, code: CloseCode, reason: &str) {
        info!("Connection closing due to ({:?}) {}", code, reason);
        self.out.connect(url::Url::parse(URL).unwrap()).unwrap();
    }
    fn on_error(&mut self, err: Error) {
        error!("{:?}", err);
    }
}

fn setup_logging() -> slog_scope::GlobalLoggerGuard {
    //let decorator = slog_term::TermDecorator::new().build();
    //let drain_term = slog_async::Async::new(slog_term::FullFormat::new(decorator).build().fuse()).build().fuse();
    let drain = slog_async::Async::new(slog_journald::JournaldDrain.fuse()).build().fuse();
    //let drain = Duplicate::new(drain_term, drain).fuse();
    // let drain = slog_envlogger::new( drain).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));
    let _scope_guard = slog_scope::set_global_logger(logger);
    let _log_guard = slog_stdlog::init_with_level(log::Level::Info).unwrap();
    _scope_guard
}

fn main() {
    let _logging_guard = setup_logging();
    let resp = ureq::get("https://www.deribit.com/api/v2/public/get_currencies").call();

    // .ok() tells if response is 200-299.
    if resp.ok() {
        let mut j = serde_json::from_reader::<_, Currencies>(resp.into_reader()).unwrap().result;
        let jj = j.into_iter().map(|x| x.currency).collect::<Vec<String>>();
        //println!("{:?}", jj);
        let mut instruments = Vec::<String>::new();
        //instruments.push("book.ETH-PERPETUAL".to_owned());
        for i in jj {
            let resp = ureq::get(format!("https://www.deribit.com/api/v2/public/get_instruments?currency={}", i).as_str()).call();
            if resp.ok() {
                let mut ins = serde_json::from_reader::<_, Instruments>(resp.into_reader())
                    .unwrap().result.into_iter().map(|x| x.instrument_name)
                    .collect::<Vec<String>>();
                instruments.append(&mut ins);
            } else {
                panic!("REST ERROR {} {} {}", resp.status(), resp.status_line() , resp.status_text())
            }
        }
        //println!("{:?}", instruments);
        //let jj: Vec<Symbol> = j.into_iter().map(|x| Symbol(x.url_symbol)).collect();
        info!("{}", "INFO"; "APP" => "DERIBIT");
        //debug!("{:?}", jj);
        let i = instruments.into_iter().collect::<Vec<String>>();
        connect(URL, |out| Client::new(out, i.as_slice())).unwrap();
    } else {
        panic!("REST ERROR {} {} {}", resp.status(), resp.status_line() , resp.status_text())

    }
    std::process::exit(1);
}
