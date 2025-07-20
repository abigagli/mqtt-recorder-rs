use crossterm::{
    event::{self, Event as CrosstermEvent, KeyCode, KeyEvent},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use log::*;
use rumqttc::{ConnectionError, TlsConfiguration, Transport};
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Publish, QoS, Request, Subscribe};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::{
    fs,
    io::{self, BufRead, Read, Write},
    path::PathBuf,
    time::SystemTime,
};
use structopt::StructOpt;
use tokio::sync::mpsc;
// use tokio::fs;
// use tokio::io::AsyncBufReadExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-recorder", about = "mqtt recorder written in rust")]
struct Opt {
    //// The verbosity of the program
    #[structopt(short, long, default_value = "1")]
    verbose: u32,

    /// The address to connect to
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    /// The port to connect to
    #[structopt(short, long, default_value = "1883")]
    port: u16,

    /// certificate of trusted CA
    #[structopt(short, long)]
    cafile: Option<PathBuf>,

    /// Mode to run software in
    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Debug, StructOpt)]
pub enum Mode {
    // Records values from an MQTT Stream
    #[structopt(name = "record")]
    Record(RecordOptions),

    // Replay values from an input file
    #[structopt(name = "replay")]
    Replay(ReplayOtions),
}

#[derive(Debug, StructOpt)]
pub struct RecordOptions {
    #[structopt(short, long, default_value = "#")]
    // Topic to record, can be used multiple times for a set of topics
    topic: Vec<String>,
    // The file to write mqtt messages to
    #[structopt(short, long, parse(from_os_str))]
    filename: PathBuf,
}

#[derive(Debug, StructOpt)]
pub struct ReplayOtions {
    #[structopt(short, long, default_value = "1.0")]
    ///Playback speed factor. Use 0 for single-packet mode
    speed: f64,

    // The file to read replay values from
    #[structopt(short, long, parse(from_os_str))]
    filename: PathBuf,

    #[structopt(
        name = "loop",
        short,
        long,
        parse(try_from_str),
        default_value = "false"
    )]
    loop_replay: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct MqttMessage {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg_b64: String,
}
async fn send_message(
    requests_tx: &rumqttc::Sender<rumqttc::Request>,
    msg: &MqttMessage,
    index: i32,
) -> Result<(), String> {
    let Ok(qos) = rumqttc::qos(msg.qos) else {
        return Err(format!("Invalid qos '{}' in packet {}", msg.qos, index + 1));
    };

    let publish = Publish::new(
        msg.topic.clone(),
        qos,
        base64::decode(&msg.msg_b64).unwrap(),
    );

    if (requests_tx.send(publish.into()).await).is_err() {
        return Err(format!("Failed to send packet {}", index + 1));
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let servername = format!("{}-{}", "mqtt-recorder-rs", now);

    match opt.verbose {
        1 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Info).init();
        }
        2 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Debug).init();
        }
        3 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Trace).init();
        }
        _ => {}
    }

    let mut mqttoptions = MqttOptions::new(servername, &opt.address, opt.port);

    if let Some(cafile) = opt.cafile {
        let mut file = fs::OpenOptions::new();
        let mut file = file.read(true).create_new(false).open(&cafile).unwrap();
        let mut vec = Vec::new();
        let _ = file.read_to_end(&mut vec).unwrap();

        let tlsconfig = TlsConfiguration::Simple {
            ca: vec,
            alpn: None,
            client_auth: None,
        };

        let transport = Transport::Tls(tlsconfig);
        mqttoptions.set_transport(transport);
    }

    mqttoptions.set_keep_alive(5);
    let mut eventloop = EventLoop::new(mqttoptions, 20_usize);
    let requests_tx = eventloop.requests_tx.clone();

    // Enter recording mode and open file readonly
    match opt.mode {
        Mode::Replay(replay) => {
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();

            // Load all messages into memory first
            let mut messages = Vec::new();
            let mut file = fs::OpenOptions::new();
            debug!("{:?}", replay.filename);
            let file = file
                .read(true)
                .create_new(false)
                .open(&replay.filename)
                .unwrap();

            for line in io::BufReader::new(&file).lines().map_while(Result::ok) {
                let msg = serde_json::from_str::<MqttMessage>(&line);
                if let Ok(msg) = msg {
                    messages.push(msg);
                }
            }

            let messages_len = messages.len() as i32;

            if replay.speed == 0.0 {
                // Manual replay mode with arrow keys
                let (key_tx, mut key_rx) = mpsc::unbounded_channel::<KeyCode>();

                // Spawn keyboard listener task in blocking context
                tokio::task::spawn_blocking(move || {
                    if enable_raw_mode().is_err() {
                        error!("Failed to enable raw mode");
                        return;
                    }

                    loop {
                        if let Ok(CrosstermEvent::Key(KeyEvent { code, .. })) = event::read() {
                            match code {
                                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') => {
                                    if key_tx.send(code).is_err() {
                                        break;
                                    }
                                }
                                KeyCode::Esc | KeyCode::Char('q') => {
                                    let _ = key_tx.send(code);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    let _ = disable_raw_mode();
                });

                // Spawn message sender task
                tokio::spawn(async move {
                    // Helper function to print message with proper terminal handling
                    let print_message = |msg: &str| {
                        // Disable raw mode temporarily for clean output
                        let _ = disable_raw_mode();
                        println!("\r{msg}");
                        let _ = enable_raw_mode();
                    };

                    print_message("Manual replay mode enabled.\nUse LEFT/RIGHT arrow keys to navigate packets\nUse SPACE to replay the last sent packet.\nUse ESC or 'q' to quit.");

                    let mut current_index = -1i32;
                    while let Some(key) = key_rx.recv().await {
                        match key {
                            KeyCode::Esc | KeyCode::Char('q') => {
                                print_message("\rExiting manual replay mode...");
                                break;
                            }
                            KeyCode::Right => {
                                // Next packet
                                current_index += 1;
                                if let Some(msg) = messages.get(current_index as usize) {
                                    match send_message(&requests_tx, msg, current_index).await {
                                        Err(err_msg) => {
                                            print_message(&err_msg);
                                            continue;
                                        }
                                        Ok(_) => {
                                            print_message(&format!(
                                                "Sent packet {} of {}: {}",
                                                current_index + 1,
                                                messages_len,
                                                msg.topic
                                            ));
                                        }
                                    }
                                } else {
                                    current_index -= 1;
                                    print_message(&format!(
                                        "Already at the last packet ({} of {messages_len})",
                                        current_index + 1
                                    ));
                                }
                            }
                            KeyCode::Char(' ') => {
                                // Repeat last sent packet
                                if current_index < 0 {
                                    print_message("No packet has been sent yet. Use RIGHT arrow to send the first packet.");
                                    continue;
                                }

                                if let Some(msg) = messages.get(current_index as usize) {
                                    match send_message(&requests_tx, msg, current_index).await {
                                        Err(err_msg) => {
                                            print_message(&err_msg);
                                            continue;
                                        }
                                        Ok(_) => print_message(&format!(
                                            "Replayed packet {} of {}: {}",
                                            current_index + 1,
                                            messages_len,
                                            msg.topic
                                        )),
                                    }
                                }
                            }
                            KeyCode::Left => {
                                // Previous packet (replay current if at start)
                                current_index = (current_index - 1).max(0);
                                if let Some(msg) = messages.get(current_index as usize) {
                                    match send_message(&requests_tx, msg, current_index).await {
                                        Err(err_msg) => {
                                            print_message(&err_msg);
                                            continue;
                                        }
                                        Ok(_) => {
                                            print_message(&format!(
                                                "Replayed packet {} of {}: {}",
                                                current_index + 1,
                                                messages_len,
                                                msg.topic
                                            ));
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    let _e = stop_tx.send(());
                });
            } else {
                // Automatic replay mode (original logic)
                tokio::spawn(async move {
                    loop {
                        let mut previous = -1.0;

                        for (current_index, msg) in messages.iter().enumerate() {
                            if previous < 0.0 {
                                previous = msg.time;
                            }

                            tokio::time::sleep(std::time::Duration::from_millis(
                                ((msg.time - previous) * 1000.0 / replay.speed) as u64,
                            ))
                            .await;

                            previous = msg.time;

                            match send_message(&requests_tx, msg, current_index as _).await {
                                Err(err_msg) => {
                                    error!("{err_msg}");
                                    continue;
                                }
                                Ok(_) => info!(
                                    "Replayed packet {} of {}: {}",
                                    current_index + 1,
                                    messages_len,
                                    msg.topic
                                ),
                            }
                        }

                        if !replay.loop_replay {
                            let _e = stop_tx.send(());
                            break;
                        }
                    }
                });
            }

            // run the eventloop forever
            while let Err(std::sync::mpsc::TryRecvError::Empty) = stop_rx.try_recv() {
                if let Err(e) = eventloop.poll().await {
                    let _ = disable_raw_mode();
                    error!("{e}");
                    std::process::exit(1);
                }
            }

            // Cleanup: disable raw mode if it was enabled
            let _ = disable_raw_mode();
        }
        // Enter recording mode and open file writeable
        Mode::Record(record) => {
            let mut file = fs::OpenOptions::new();
            let mut file = file
                .write(true)
                .create_new(true)
                .open(&record.filename)
                .unwrap();

            loop {
                let res = eventloop.poll().await;

                match res {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let qos = match publish.qos {
                            QoS::AtMostOnce => 0,
                            QoS::AtLeastOnce => 1,
                            QoS::ExactlyOnce => 2,
                        };

                        let msg = MqttMessage {
                            time: SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64(),
                            retain: publish.retain,
                            topic: publish.topic.clone(),
                            msg_b64: base64::encode(&*publish.payload),
                            qos,
                        };

                        let serialized = serde_json::to_string(&msg).unwrap();
                        writeln!(file, "{serialized}").unwrap();

                        debug!("{publish:?}");
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(_connect))) => {
                        info!("Connected to: {}:{}", opt.address, opt.port);

                        for topic in &record.topic {
                            let subscription = Subscribe::new(topic, QoS::AtLeastOnce);
                            let _ = requests_tx.send(Request::Subscribe(subscription)).await;
                        }
                    }
                    Err(e) => {
                        error!("{e:?}");
                        if let ConnectionError::Network(_e) = e {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
