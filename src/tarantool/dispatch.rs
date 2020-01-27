use std::boxed::Box;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::string::ToString;
use std::sync::{Arc, RwLock};
use std::pin::Pin;
use std::task::{ Poll, Context };

use futures::future::BoxFuture;
use futures::future::{ Future, FutureExt, TryFuture, TryFutureExt };
use futures::stream::{ Stream, StreamExt, TryStream, TryStreamExt };
use futures::sink::{ Sink, SinkExt };
use futures::stream::{ SplitSink, SplitStream };
use futures::channel::mpsc;
use futures::channel::oneshot;

use tokio::net::TcpStream;
use tokio::time::delay_queue;
use tokio::time::{Delay, DelayQueue};
use tokio_util::codec::{Decoder, Framed};

use crate::tarantool::codec::{RequestId, TarantoolCodec, TarantoolFramedRequest};
use crate::tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest, TarantoolResponse};

pub type TarantoolCodecItem = (RequestId, TarantoolRequest);
pub type TarantoolFramed = Framed<TcpStream, TarantoolCodec>;
pub type CallbackSender = oneshot::Sender<io::Result<TarantoolResponse>>;
pub type ReconnectNotifySender = mpsc::UnboundedSender<ClientStatus>;

static ERROR_SERVER_DISCONNECT: &str = "SERVER DISCONNECTED!";
pub static ERROR_DISPATCH_THREAD_IS_DEAD: &str = "DISPATCH THREAD IS DEAD!";
pub static ERROR_CLIENT_DISCONNECTED: &str = "CLIENT DISCONNECTED!";
static ERROR_TIMEOUT: &str = "TIMEOUT!";

///
/// Tarantool client config
///
/// # Examples
/// ```text
/// let client = ClientConfig::new(addr, "rust", "rust")
///            .set_timeout_time_ms(1000)
///            .set_reconnect_time_ms(10000)
///            .build();
///
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ClientConfig {
    addr: SocketAddr,
    login: String,
    password: String,
    reconnect_time_ms: u64,
    timeout_time_ms: Option<u64>,
}

impl ClientConfig {
    pub fn new<S, S1>(addr: SocketAddr, login: S, password: S1) -> ClientConfig
    where
        S: Into<String>,
        S1: Into<String>,
    {
        ClientConfig {
            addr,
            login: login.into(),
            password: password.into(),
            reconnect_time_ms: 10000,
            timeout_time_ms: None,
        }
    }

    pub fn set_timeout_time_ms(mut self, timeout_time_ms: u64) -> ClientConfig {
        self.timeout_time_ms = Some(timeout_time_ms);
        self
    }

    pub fn set_reconnect_time_ms(mut self, reconnect_time_ms: u64) -> ClientConfig {
        self.reconnect_time_ms = reconnect_time_ms;
        self
    }
}

#[derive(Clone, Debug)]
pub enum ClientStatus {
    Init,
    Connecting,
    Handshaking,
    Connected,
    Disconnecting(String),
    Disconnected(String),
}

enum DispatchState {
    New,
    // OnConnect(Pin<Box<dyn Future<Output = Result<tokio::net::TcpStream, io::Error> > + Send>>),
    OnConnect(BoxFuture<'static, Result<tokio::net::TcpStream, io::Error>>),
    // OnHandshake(Pin<Box<dyn Future<Output = Result<TarantoolFramed, io::Error> > + Send>>),
    OnHandshake(BoxFuture<'static, Result<TarantoolFramed, io::Error>>),
    OnProcessing((SplitSink<TarantoolFramed, TarantoolCodecItem>, SplitStream<TarantoolFramed>)),

    OnReconnect(String),
    OnSleep(Delay, String),
}

impl fmt::Display for DispatchState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status = match *self {
            DispatchState::New => "New",
            DispatchState::OnConnect(_) => "OnConnect",
            DispatchState::OnHandshake(_) => "OnHandshake",
            DispatchState::OnProcessing(_) => "OnProcessing",
            DispatchState::OnReconnect(_) => "OnReconnect",
            DispatchState::OnSleep(_, _) => "OnSleep",
        };
        write!(f, "{}", status)
    }
}

impl DispatchState {
    fn get_client_status(&self) -> ClientStatus {
        match *self {
            DispatchState::New => ClientStatus::Init,
            DispatchState::OnConnect(_) => ClientStatus::Connecting,
            DispatchState::OnHandshake(_) => ClientStatus::Handshaking,
            DispatchState::OnProcessing(_) => ClientStatus::Connected,
            DispatchState::OnReconnect(ref error_message) => {
                ClientStatus::Disconnecting(error_message.clone())
            }
            DispatchState::OnSleep(_, ref error_message) => {
                ClientStatus::Disconnected(error_message.clone())
            }
        }
    }
}

struct DispatchEngine {
    command_receiver: Pin<Box<dyn Stream<Item = (CommandPacket, CallbackSender)> + Send>>,
    awaiting_callbacks: HashMap<RequestId, CallbackSender>,
    notify_callbacks: Arc<RwLock<Vec<ReconnectNotifySender>>>,

    buffered_command: Option<TarantoolFramedRequest>,
    command_counter: RequestId,

    timeout_time_ms: Option<u64>,
    timeout_queue: Pin<Box<DelayQueue<RequestId>>>,
    timeout_id_to_key: HashMap<RequestId, delay_queue::Key>,
}

impl DispatchEngine {
    fn new(
        command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
        timeout_time_ms: Option<u64>,
        notify_callbacks: Arc<RwLock<Vec<ReconnectNotifySender>>>,
    ) -> DispatchEngine {
        DispatchEngine {
            command_receiver: Box::pin(command_receiver),
            buffered_command: None,
            awaiting_callbacks: HashMap::new(),
            notify_callbacks,
            command_counter: 3,
            timeout_time_ms,
            timeout_queue: Box::pin(DelayQueue::new()),
            timeout_id_to_key: HashMap::new(),
        }
    }

    fn send_notify(&mut self, status: &ClientStatus) {
        let mut guard = self.notify_callbacks.write().unwrap();
        let callbacks: &mut Vec<ReconnectNotifySender> = guard.as_mut();

        //dirty code - send status to all callbacks and remove dead callbacks
        let mut i = 0;
        while i != callbacks.len() {
            if let Ok(_) = &callbacks[i].unbounded_send(status.clone()) {
                i = i + 1;
            } else {
                callbacks.remove(i);
            }
        }
    }

    fn try_send_buffered_command(&mut self, cx: &mut std::task::Context, sink: &mut Pin<&mut SplitSink<TarantoolFramed, TarantoolCodecItem>>) -> bool {
        if let Some(command) = self.buffered_command.take() {
            match sink.as_mut().poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    if let Err(_) = sink.as_mut().start_send(command) {
                        //self.buffered_command = Some(command);
                        return false;
                    }
                    //sink.poll_complete();
                },
                Poll::Ready(Err(err)) => {
                    self.buffered_command = Some(command);
                    return false;
                }
                Poll::Pending => { return false; }
            }
        };
        true
    }

    fn send_error_to_all(&mut self, cx: &mut std::task::Context, error_description: &String) {
        for (_, mut callback_sender) in self.awaiting_callbacks.drain() {
            let _res = callback_sender.send(Err(io::Error::new(
                io::ErrorKind::Other,
                error_description.clone(),
            )));
        }
        self.buffered_command = None;

        if let Some(_) = self.timeout_time_ms {
            self.timeout_id_to_key.clear();
            self.timeout_queue.clear();
        }

        loop {
            match self.command_receiver.as_mut().poll_next(cx) {
                Poll::Ready(Some((_, callback_sender))) => {
                    let _res = callback_sender.send(Err(io::Error::new(
                        io::ErrorKind::Other,
                        error_description.clone(),
                    )));
                }
                _ => break,
            };
        }
    }

    fn process_commands(&mut self, cx: &mut std::task::Context, sink: &mut Pin<&mut SplitSink<TarantoolFramed, TarantoolCodecItem>>) -> Poll<()> {
        let mut continue_send = self.try_send_buffered_command(cx, sink);
        while continue_send {
            continue_send = match self.command_receiver.as_mut().poll_next(cx) {
                Poll::Ready(Some((command_packet, callback_sender))) => {
                    let request_id = self.increment_command_counter();
                    self.awaiting_callbacks.insert(request_id, callback_sender);
                    self.buffered_command =
                        Some((request_id, TarantoolRequest::Command(command_packet)));
                    if let Some(timeout_time_ms) = self.timeout_time_ms {
                        let delay_key = self.timeout_queue.insert_at(
                            request_id,
                            tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_time_ms),
                        );
                        self.timeout_id_to_key.insert(request_id, delay_key);
                    }

                    self.try_send_buffered_command(cx, sink)
                }
                Poll::Ready(None) => {
                    //inbound sink is finished. close coroutine
                    return Poll::Ready(());
                }
                _ => false,
            };
        }
        //skip results of poll complete
        // let _r = sink.poll_flush();
        let _r = sink.as_mut().poll_flush(cx);
        Poll::Pending
    }

    fn process_tarantool_responses(&mut self, cx: &mut std::task::Context, stream: &mut Pin<&mut SplitStream<TarantoolFramed>>) -> bool {
        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok((request_id, command_packet)))) => {
                    debug!("receive command! {} {:?} ", request_id, command_packet);
                    if let Some(_) = self.timeout_time_ms {
                        if let Some(delay_key) = self.timeout_id_to_key.remove(&request_id) {
                            self.timeout_queue.remove(&delay_key);
                        }
                    }

                    self.awaiting_callbacks
                        .remove(&request_id)
                        .map(|mut sender| sender.send(command_packet));
                }
                Poll::Ready(None) | Poll::Ready(Some(Err(_))) => {
                    return true;
                }
                _ => {
                    return false;
                }
            }
        }
    }

    fn process_timeouts(&mut self, cx: &mut std::task::Context) {
        if let Some(_) = self.timeout_time_ms {
            loop {
                match self.timeout_queue.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(request_id_ref))) => {
                        let request_id = request_id_ref.get_ref();
                        info!("timeout command! {} ", request_id);
                        self.timeout_id_to_key.remove(request_id);
                        if let Some(mut callback_sender) = self.awaiting_callbacks.remove(request_id) {
                            //don't process result of send
                            let _res = callback_sender
                                .send(Err(io::Error::new(io::ErrorKind::Other, ERROR_TIMEOUT)));
                        }
                    }
                    _ => {
                        return;
                    }
                }
            }
        }
    }

    fn increment_command_counter(&mut self) -> RequestId {
        self.command_counter = self.command_counter + 1;
        self.command_counter
    }

    fn clean_command_counter(&mut self) {
        self.command_counter = 3;
    }
}

#[pin_project::pin_project]
pub struct Dispatch {
    config: ClientConfig,

    // #[pin]
    state: DispatchState,

    #[pin]
    engine: DispatchEngine,

    status: Arc<RwLock<ClientStatus>>,
}

impl Dispatch {
    pub fn new(
        config: ClientConfig,
        command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
        status: Arc<RwLock<ClientStatus>>,
        notify_callbacks: Arc<RwLock<Vec<ReconnectNotifySender>>>,
    ) -> Dispatch {
        let timeout_time_ms = config.timeout_time_ms.clone();
        Dispatch {
            state: DispatchState::New,
            config,
            engine: DispatchEngine::new(command_receiver, timeout_time_ms, notify_callbacks),
            status,
        }
    }

    async fn get_auth_seq(
                          stream: TcpStream,
                          config: ClientConfig,
    ) -> Result<TarantoolFramed, io::Error> {
        let login = config.login.clone();
        let password = config.password.clone();

        let res = TarantoolCodec::new().framed(stream);
        // TODO auth
        Ok(res)
//        Box::pin(
//            TarantoolCodec::new()
//                .framed(stream)
//                .into_future()
//                .map_err(|e| e.0)
//                .and_then(|(_first_resp, framed_io)| {
//                    framed_io
//                        .send((2, TarantoolRequest::Auth(AuthPacket { login, password })))
//                        .into_future()
//                })
//                .and_then(|framed| framed.into_future().map_err(|e| e.0))
//                .and_then(|(r, framed_io)| match r {
//                    Some((_, Err(e))) => futures::future::err(e),
//                    _ => futures::future::ok(framed_io),
//                }),
//        )
    }
}

#[pin_project::project]
impl Dispatch {
    fn update_status(&mut self) {
        let status_tmp = self.state.get_client_status();
        let mut status = self.status.write().unwrap();
        *status = status_tmp.clone();
        self.engine.send_notify(&status_tmp);
    }
}

impl Future for Dispatch {
    type Output = Result<(), io::Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {

        let mut this = self.project();

        debug!("poll ! {}", this.state);

        let config = this.config.clone();
        loop {
            let new_state = match this.state {
                DispatchState::New => {
                    let addr = config.addr.clone();
                    Some(DispatchState::OnConnect(Box::pin(TcpStream::connect(addr))))
                },
                DispatchState::OnReconnect(ref error_description) => {
                    error!("Reconnect! error={}", error_description);
                    this.engine.send_error_to_all(cx, &error_description.to_string());
                    let delay_future = tokio::time::delay_until(
                        tokio::time::Instant::now() + tokio::time::Duration::from_millis(config.reconnect_time_ms),
                    );
                    Some(DispatchState::OnSleep(
                        delay_future,
                        error_description.clone(),
                    ))
                }
                DispatchState::OnSleep(ref mut delay_future, _) => match delay_future.poll_unpin(cx) {
                    Poll::Ready(()) => Some(DispatchState::New),
                    Poll::Pending => None,
                    //Poll::Ready(Err(err)) => Some(DispatchState::OnReconnect(err.to_string())),
                },
                DispatchState::OnConnect(ref mut connect_future) => {
                    match connect_future.as_mut().poll(cx) {
                        Poll::Ready(Ok(stream)) => Some(DispatchState::OnHandshake(
                            Dispatch::get_auth_seq(stream, this.config.clone()).boxed(),
                        )),
                        Poll::Ready(Err(err)) => Some(DispatchState::OnReconnect(err.to_string())),
                        Poll::Pending => None,
                    }
                }

                DispatchState::OnHandshake(ref mut handshake_future) => {
                    match handshake_future.as_mut().poll(cx) {
                        Poll::Ready(Ok(framed)) => {
                            this.engine.clean_command_counter();
                            Some(DispatchState::OnProcessing(framed.split()))
                        },
                        Poll::Ready(Err(err)) => Some(DispatchState::OnReconnect(err.to_string())),
                        Poll::Pending => None,
                    }
                }

                DispatchState::OnProcessing((ref mut sink, ref mut stream)) => {
                    match this.engine.process_commands(cx, &mut Pin::new(sink)) {
                        Poll::Ready(()) => {
                            //                          stop client !!! exit from event loop !
                            return Poll::Ready(Ok(()));
                        }
                        _ => {}
                    }

                    if this.engine.process_tarantool_responses(cx, &mut Pin::new(stream)) {
                        Some(DispatchState::OnReconnect(
                            ERROR_SERVER_DISCONNECT.to_string(),
                        ))
                    } else {
                        this.engine.process_timeouts(cx);
                        None
                    }
                }
                _ => { unimplemented!() }
            };

            if let Some(new_state_value) = new_state {
                *this.state = new_state_value;
                this.update_status();
            } else {
                break;
            }
        }

        Poll::Pending
    }
}
