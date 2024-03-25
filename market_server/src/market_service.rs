use std::net::Ipv4Addr;

use market_dht::{dht_client::DhtClient, file::new_cidv0, CommandOk};
use market_proto::market_proto_rpc::{
    market_server::Market, CheckHoldersRequest, HoldersResponse, RegisterFileRequest, User,
};
use tonic::{Request, Response, Status};

// NOTE: Market server is essentially the peer that starts the DHT. For a peer, then,
// we can probably simply just drop some of the fields in the User struct. We'll do so with a new
// struct called Peer.
// Market server will assume the role of a DHT node and will already have an IP address and Port
// that it's listening on I believe. Market server will also no longer need the peer ID provided by
// the peer node client since the peer ID will be generated by the market_server. I think all the
// market server needs then is just the price per MB and the Username from the client.

// TODO: replace this with a DHT
// type MarketStore = HashMap<String, HashSet<User>>;

#[derive(Debug)]
pub struct MarketService {
    dht_store: DhtClient,
}

impl MarketService {
    pub fn new(dht_store: DhtClient) -> Self {
        MarketService { dht_store }
    }
}

#[tonic::async_trait]
impl Market for MarketService {
    async fn register_file(
        &self,
        request: Request<RegisterFileRequest>,
    ) -> Result<Response<()>, Status> {
        let file_req = request.into_inner();

        let file_hash = file_req.file_hash;
        // TODO: ehhh not exactly right since we're now cidv0ing the hashes
        let file_hash = new_cidv0(file_hash.as_bytes())
            .map_err(|err| Status::internal(err.to_string()))?
            .to_bytes();
        let user = file_req.user.ok_or(Status::invalid_argument(
            "The user field is required for this request",
        ))?;
        let ip = user
            .ip
            .as_str()
            .parse::<Ipv4Addr>()
            .map_err(|err| Status::internal(format!("Internal Server Error: {}", err)))?;
        self.dht_store
            .register(
                &file_hash,
                ip,
                user.port
                    .try_into()
                    .map_err(|err| Status::internal(format!("Internal Server Error: {}", err)))?,
                user.price
                    .try_into()
                    .map_err(|err| Status::internal(format!("Internal Server Error: {}", err)))?,
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(()))
    }

    async fn check_holders(
        &self,
        request: Request<CheckHoldersRequest>,
    ) -> Result<Response<HoldersResponse>, Status> {
        let holders_req = request.into_inner();
        let file_hash = holders_req.file_hash;
        // TODO: ehhh not exactly right since we're now cidv0ing the hashes
        let file_hash = new_cidv0(file_hash.as_bytes())
            .map_err(|err| Status::internal(err.to_string()))?
            .to_bytes();
        if let CommandOk::GetFile {
            metadata,
            owner_peer,
            ..
        } = self
            .dht_store
            .get_file(&file_hash)
            .await
            .map_err(|err| Status::internal(format!("Internal Server Error: {}", err)))?
        {
            let holders = vec![User::new(
                owner_peer.to_string(),
                owner_peer.to_string(),
                metadata.ip().to_string(),
                metadata.port() as i32,
                metadata
                    .price_per_mb()
                    .try_into()
                    .map_err(|err| Status::internal(format!("Internal server error: {err}")))?,
            )];
            Ok(Response::new(HoldersResponse { holders }))
        } else {
            panic!("Didn't match for some reason when it should've have mapped");
        }
    }
}
