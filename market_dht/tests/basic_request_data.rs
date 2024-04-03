use std::{thread, time::Duration};

use market_dht::{config::Config, multiaddr, net::spawn_bridge, ResponseData};
use pretty_assertions::{self, assert_eq};
use tokio::runtime::Runtime;

#[tokio::test]
async fn test_should_not_panic_in_async_context() {
    let _ = spawn_bridge(
        Config::builder()
            .with_listener(multiaddr!(Ip4([127, 0, 0, 1]), Tcp(4444u16)))
            .with_thread_name("peer1".to_owned())
            .build(),
    )
    .unwrap();
}

#[test]
fn test_get_connected_peers() {
    let peer1 = spawn_bridge(
        Config::builder()
            .with_listener(multiaddr!(Ip4([127, 0, 0, 1]), Tcp(1233u16)))
            .with_thread_name("peer1".to_owned())
            .build(),
    )
    .unwrap();

    let _peer2 = spawn_bridge(
        Config::builder()
            .with_listener(multiaddr!(Ip4([127, 0, 0, 1]), Tcp(1234u16)))
            .with_thread_name("peer2".to_owned())
            .with_boot_nodes(
                vec![("/ip4/127.0.0.1/tcp/1233".to_owned(), peer1.id().to_string())]
                    .try_into()
                    .unwrap(),
            )
            .build(),
    )
    .unwrap();

    thread::sleep(Duration::from_secs(1));
    Runtime::new().unwrap().block_on(async move {
        let response = peer1.get_connected_peers().await.unwrap();
        if let ResponseData::ConnectedPeers { connected_peers } = response {
            assert_eq!(1, connected_peers.len());
        } else {
            panic!("Didn't get the correct response!")
        }
    });
}

// Each peer has their own swarm and port they listen to so each peer has exactly ONE listener
// TODO(?): eventually allow peers to use more than one listener 
// Further TODO(?): test the case of expired listeners
#[test]
fn test_get_all_listeners(){
    let peer1 = spawn_bridge(
        Config::builder()
            .with_listener(multiaddr!(Ip4([127, 0, 0, 1]), Tcp(1235u16)))
            .with_thread_name("peer1".to_owned())
            .build(),
    )
    .unwrap();

    thread::sleep(Duration::from_secs(1));
    Runtime::new().unwrap().block_on(async move {
        let response = peer1.get_all_listeners().await.unwrap();
        if let ResponseData::AllListeners { listeners } = response {
            assert_eq!(1, listeners.len());
        } else {
            panic!("Listeners amount not correct");
        }
    });
}

// Returns a bool if there's an established conenction to a peer
#[test]
fn test_is_connected_to(){
    let peer1 = spawn_bridge(
        Config::builder()
            .with_listener(multiaddr!(Ip4([127, 0, 0, 1]), Tcp(1236u16)))
            .with_thread_name("peer1".to_owned())
            .build(),
    )
    .unwrap();

    let peer2 = spawn_bridge(
        Config::builder()
            .with_listener(multiaddr!(Ip4([127, 0, 0, 1]), Tcp(1237u16)))
            .with_thread_name("peer2".to_owned())
            .with_boot_nodes(
                vec![("/ip4/127.0.0.1/tcp/1236".to_owned(), peer1.id().to_string())]
                    .try_into()
                    .unwrap(),
            )
            .build(),
    )
    .unwrap();

    thread::sleep(Duration::from_secs(1));
    Runtime::new().unwrap().block_on(async move {
        let response = peer1.is_connected_to(*peer2.id()).await.unwrap();
        if let ResponseData::IsConnectedTo { is_connected } = response {
            assert_eq!(true, is_connected);
        } else {
            panic!("Isn't connected to peer 2")
        }
    });
}