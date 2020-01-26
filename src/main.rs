use serde::{Serialize, Deserialize};
use ws::{connect, Handler, Sender, Handshake, Result, Message, CloseCode, Error};
use log::{info,error,debug};
use env_logger;
use chrono::prelude::*;
use std::path::Path;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};


#[derive(Serialize, Deserialize, Debug)]
struct TraidingPairs {
    url_symbol: String,
}

#[derive(Debug)]
struct Symbol (String);

struct Client<'a> {
    out: Sender,
    symbols: & 'a Vec<Symbol>,
    fs_ts: Option<String>,
    file: Option<File>,

}


impl Client<'_> {
    fn new(out: Sender, symbols: &Vec<Symbol>) -> Client {
         Client{out, symbols, fs_ts: None, file: None }
    }
    //ts = t.strftime('%Y-%m-%d_%H_%M')
    //new_file_name = "bitstamp2-ws-"+ts+".log"


    fn build_file_name(ts: &str) -> String {

         let n =  format!("bitstamp2-ws-{}.log", ts);
        info!("{}", n);
        n
    }
    fn create_file(name: &str) -> File {
        let path = Path::new(name);
        OpenOptions::new().append(true).create(true).open(path).unwrap()
    }

    fn write(& mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let utc: DateTime<Utc> = Utc::now();
        let fs_ts = utc.format("%Y-%m-%d_%HZ").to_string();

         match self.fs_ts.as_ref() {
            Some(x) => {
                if x == &fs_ts {
                    //self.file.as_mut().unwrap().write(buf)
                } else {
                    info!("timestamp_ {}", fs_ts);
                    let f = self.file.as_mut().unwrap();

                        f.flush().unwrap();
                        f.sync_all().unwrap();

                    self.fs_ts = Some(fs_ts);
                    self.file = Some(Client::create_file(&Client::build_file_name(&self.fs_ts.as_ref().unwrap())));
                    //self.file.as_mut().unwrap().write(buf)
                }
            }
            None => {
                info!("timestamp_ {}", fs_ts);

                self.fs_ts = Some(fs_ts);
                self.file = Some(Client::create_file(&Client::build_file_name(&self.fs_ts.as_ref().unwrap())));
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
const ALL_SUBSCRIPTION_TOPICS: [&'static str; 5] =["live_trades", "live_orders", "order_book", "detail_order_book", "diff_order_book"];


const SUBSCRIBE_TEMPLATE: &str =  r#"{"event": "bts:subscribe","data": {"channel": "{topic}_{symbol}"}}"#;


impl Handler for Client<'_> {
    fn on_open(&mut self, shake: Handshake) -> Result<()> {
        if let Some(addr) = shake.remote_addr()? {
            info!("Connection with {} now open", addr);
        }
        for topic in ALL_SUBSCRIPTION_TOPICS.iter(){
            for symbol in self.symbols.iter() {
                let m = SubscribeMessage{event: "bts:subscribe".to_owned(), data: SubscribeMessageData{channel: format!("{}_{}", topic, symbol.0)} };


                let j = serde_json::to_string(&m).unwrap();
                self.out.send(Message::Text(j)).unwrap();
            }
        }
        info!("subscribed");
        Ok(())
    }
    fn on_message(&mut self, msg: Message) -> Result<()> {
        //println!("{:?}", msg);
        let utc: DateTime<Utc> = Utc::now();
        if let Message::Text(s) = msg {
            let ts = format!("{:?}", utc);
            let mut mm = String::with_capacity(s.len()+ts.len()+3);
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
    }
    fn on_error(&mut self, err: Error) {
        // Ignore connection reset errors by default, but allow library clients to see them by
        // overriding this method if they want
        //if let ErrorKind::Io(ref err) = err.kind {
        //    if let Some(104) = err.raw_os_error() {
        //        return;
        //    }
        //}

        error!("{:?}", err);

    }


}

fn main() {
    env_logger::init();
    let resp = ureq::get("https://www.bitstamp.net/api/v2/trading-pairs-info/").call();


// .ok() tells if response is 200-299.
    if resp.ok() {
        let j: Vec<TraidingPairs> = serde_json::from_reader(resp.into_reader()).unwrap();
        //println!("{:?}",j);
        let jj: Vec<Symbol> = j.into_iter().map(|x|Symbol(x.url_symbol)).collect();
        debug!("{:?}", jj);
        connect("wss://ws.bitstamp.net/", |out| Client::new(out, &jj )).unwrap();
    }
}
