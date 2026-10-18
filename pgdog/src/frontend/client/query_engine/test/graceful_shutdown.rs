use std::time::Duration;

use tokio::time::{sleep, timeout};

use crate::{
    config::{config, load_test, set},
    expect_message,
    frontend::comms::comms,
    net::{
        BindComplete, CommandComplete, DataRow, ErrorResponse, Format, Parameters, ParseComplete,
        ReadyForQuery,
    },
};

use super::prelude::*;

async fn spawn_draining_client() -> SpawnedClient {
    load_test();
    let mut config = (*config()).clone();
    config.config.general.shutdown_drain_clients = true;
    set(config).unwrap();

    SpawnedClient::new(Parameters::default()).await
}

#[tokio::test]
async fn test_graceful_shutdown_drain_serves_idle_client() {
    let mut client = spawn_draining_client().await;

    comms().shutdown();

    client.send(Query::new("SELECT 1")).await;
    let messages = client.read_until('Z').await;
    assert!(messages.iter().all(|message| message.code() != 'E'));
}

#[tokio::test]
async fn test_graceful_shutdown_drain_keeps_partial_request() {
    let mut client = spawn_draining_client().await;

    client.send(Parse::named("drain_test", "SELECT $1")).await;
    sleep(Duration::from_millis(100)).await;
    comms().shutdown();
    sleep(Duration::from_millis(100)).await;

    client
        .send(Bind::new_params(
            "drain_test",
            &[Parameter {
                len: 3,
                data: "123".into(),
            }],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get::<String>(0, Format::Text), Some("123".into()));
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);
}

/// Test that a graceful shutdown sends the "shutting down" error to the client
/// and the client loop exits.
#[tokio::test]
async fn test_graceful_shutdown_sends_error() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    // Trigger global shutdown.
    comms().shutdown();

    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.code, "57P01");
    assert_eq!(err.message, "PgDog is shutting down");

    // Client loop should exit promptly.
    timeout(Duration::from_secs(2), client.join())
        .await
        .expect("client loop did not exit after shutdown");
}

/// Test that a client inside a transaction is allowed to finish
/// before receiving the shutdown error.
#[tokio::test]
async fn test_graceful_shutdown_waits_for_transaction() {
    let mut client = SpawnedClient::new_default(Parameters::default()).await;

    // Start a transaction.
    client.send(Query::new("BEGIN")).await;
    client.read_until('Z').await;

    // Trigger shutdown while in transaction.
    comms().shutdown();

    // Client should still be able to complete the transaction.
    client.send(Query::new("SELECT 1")).await;
    client.read_until('Z').await;

    client.send(Query::new("COMMIT")).await;
    client.read_until('Z').await;

    // Now that the transaction is done, client receives the shutdown error.
    let err = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(err.code, "57P01");
    assert_eq!(err.message, "PgDog is shutting down");

    // Client loop should exit promptly.
    timeout(Duration::from_secs(2), client.join())
        .await
        .expect("client loop did not exit after shutdown");
}
