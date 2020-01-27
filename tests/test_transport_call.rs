use futures::stream::StreamExt;
use futures::sink::SinkExt;

use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

use rusty_tarantool::tarantool::codec::TarantoolCodec;
use rusty_tarantool::tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest};

#[tokio::test]
async fn test() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let addr : std::net::SocketAddr = "127.0.0.1:3301".parse()?;
    let stream = TcpStream::connect(addr).await?;
    println!("stream={:?}", stream);
    let framed_io = TarantoolCodec::new().framed(stream);
    let (r, mut framed_io) = framed_io.into_future().await;
    println!("Received first packet {:?}", r);
    framed_io.send((
        2,
        TarantoolRequest::Auth(AuthPacket {
            login: String::from("rust"),
            password: String::from("rust"),
        }),
    )).await?;
    let (r, mut framed_io) = framed_io.into_future().await;
    println!("Received auth answer packet {:?}", r);
    framed_io.send((
        2,
        TarantoolRequest::Command(
            CommandPacket::call("test", &(("aa", "aa"), 1i32)).unwrap(),
        ),
    )).await?;
    let (resp, _framed_io) = framed_io.into_future().await;
    println!("Received test result packet {:?}", resp);
    if let Some(Ok((_id, resp_packet))) = resp {
        let s: (Vec<String>, Vec<u64>) = resp_packet?.decode()?;
        println!("resp value={:?}", s);
    }
    Ok(())
}
