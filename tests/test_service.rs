
use futures::future::{ Future, FutureExt, TryFuture, TryFutureExt };
use futures::stream::{ Stream, StreamExt, TryStream, TryStreamExt };

use rusty_tarantool::tarantool::{serialize_to_vec_u8, Client, ClientConfig};
use std::sync::Once;

use std::error::Error;

static INIT: Once = Once::new();

static SPACE_ID: i32 = 1000;

fn setup_logger() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

async fn init_client() -> Result<Client, Box<dyn Error>> {
    setup_logger();

    let addr = "127.0.0.1:3301".parse()?;
    Ok(ClientConfig::new(addr, "rust", "rust").build())
}

pub fn test_result(r: Result<(), std::io::Error>) {
    match r {
        Err(e) => {
            println!("err={:?}", e);
            assert!(false);
        }
        Ok(res) => {
            println!("ressp={:?}", res);
        }
    }
}

#[tokio::test]
async fn test_call_fn() -> Result<(), Box<dyn Error>> {
    let client = init_client().await?;
    let response = client.call_fn("test", &(("aa", "aa"), 1)).await?;
    println!("response2: {:?}", response);
    let s: (Vec<String>, Vec<u64>) = response.decode_pair()?;
    println!("resp value={:?}", s);
    assert_eq!((vec!["aa".to_string(), "aa".to_string()], vec![1]), s);
    Ok(())
}

#[tokio::test]
async fn test_select() -> Result<(), Box<dyn Error>> {
    let client = init_client().await?;
    let key = (1,);

    let response = client.select(SPACE_ID, 0, &key, 0, 100, 0).await?;
    println!("response2: {:?}", response);
    let s: Vec<(u32, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(vec![(1, "test-row".to_string())], s);
    Ok(())
}

#[tokio::test]
async fn test_delete_insert_update() -> Result<(), Box<dyn Error>> {
    let client = init_client().await?;
    let tuple = (3, "test_insert");
    let tuple_replace = (3, "test_insert", "replace");
    let tuple_replace_raw = (3, "test_insert", "replace", "replace_raw");
    let update_op = (('=', 2, "test_update"),);

    client.delete(SPACE_ID, &tuple).await?;
    let response = client.insert(SPACE_ID, &tuple).await?;
    println!("response2: {:?}", response);
    let s: Vec<(u32, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(vec![(3, "test_insert".to_string())], s);

    let response = client.update(SPACE_ID, &tuple, &update_op).await?;
    let s: Vec<(u32, String, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(
        vec![(3, "test_insert".to_string(), "test_update".to_string())],
        s
    );
    client.replace(SPACE_ID, &tuple_replace).await?;

    let raw_buf = serialize_to_vec_u8(&tuple_replace_raw)?;
    let response = client.replace_raw(SPACE_ID, raw_buf).await?;
    let s: Vec<(u32, String, String, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(
        vec![(
            3,
            "test_insert".to_string(),
            "replace".to_string(),
            "replace_raw".to_string()
        )],
        s
    );
    Ok(())
}

#[tokio::test]
async fn test_upsert() -> Result<(), Box<dyn Error>> {
    let client = init_client().await?;
    let key = (4, "test_upsert");
    let update_op = (('=', 2, "test_update_upsert"),);

    let response = client.upsert(SPACE_ID, &key, &key, &update_op).await?;
    println!("response2: {:?}", response);
    let s: Vec<u8> = response.decode()?;
    let empty: Vec<u8> = vec![];
    println!("resp value={:?}", s);
    assert_eq!(empty, s);
    Ok(())
}

#[tokio::test]
async fn test_eval() -> Result<(), Box<dyn Error>> {
    let client = init_client().await?;
    let response = client.eval("return ...\n".to_string(), &(1, 2)).await?;
    println!("response2: {:?}", response);
    let s: (u32, u32) = response.decode()?;
    let id: (u32, u32) = (1, 2);
    println!("resp value={:?}", s);
    assert_eq!(id, s);
    Ok(())
}

#[tokio::test]
async fn test_ping() -> Result<(), Box<dyn Error>> {
    let client = init_client().await?;
    let response = client.ping().await?;
    println!("response ping: {:?}", response);
    Ok(())
}
