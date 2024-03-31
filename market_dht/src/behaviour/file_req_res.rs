use std::{collections::HashMap, net::Ipv4Addr};

use libp2p::{
    request_response::{self, cbor, Config, OutboundRequestId, ProtocolSupport},
    swarm::NetworkBehaviour,
    StreamProtocol,
};
use log::{error, info};
use serde::{Deserialize, Serialize};

use crate::req_res::{FileReqResRequestData, FileReqResResponseData, RequestHandler, ResponseData};

use super::macros::send_response;

#[derive(Debug, Default)]
#[non_exhaustive]
// NOTE: maybe useful in the future later for some fields?
pub(crate) struct FileReqResHandler {
    pending_requests: HashMap<OutboundRequestId, RequestHandler>,
}

impl FileReqResHandler {
    pub(crate) fn handle_request(
        &mut self,
        event: FileReqResRequestData,
        request_handler: RequestHandler,
        FileReqResBehaviour { req_res }: &mut FileReqResBehaviour,
    ) {
        match event {
            FileReqResRequestData::GetSupplierInfo { file_hash, peer_id } => {
                let qid = req_res.send_request(&peer_id, FileHash(file_hash));
                self.pending_requests.insert(qid, request_handler);
            }
        }
    }

    pub(crate) fn handle_event(
        &mut self,
        FileReqResBehaviourEvent::ReqRes(event): FileReqResBehaviourEvent,
        market_map: &mut HashMap<FileHash, SupplierInfo>,
        FileReqResBehaviour { req_res }: &mut FileReqResBehaviour,
    ) {
        match event {
            request_response::Event::Message { peer, message } => match message {
                request_response::Message::Request {
                    request_id,
                    request,
                    channel,
                } => {
                    if let Some(supplier_info) = market_map.get(&request) {
                        // NOTE: maybe in the future use Cow
                        if req_res
                            .send_response(channel, supplier_info.clone())
                            .is_err()
                        {
                            error!("[RequestId {request_id}] Failed to send response to {peer}!");
                        }
                    }
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let response =
                        ResponseData::ReqResResponse(FileReqResResponseData::GetSupplierInfo {
                            supplier_info: response,
                        });
                    send_response!(self.pending_requests, request_id, Ok(response));
                    info!("[RequestId {request_id}] Response sent to {peer}");
                }
            },
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                error!("Outbound failure: {}", error);
                send_response!(self.pending_requests, request_id, Err(error.into()));
            }
            request_response::Event::InboundFailure {
                peer,
                request_id,
                error,
            } => {
                error!(
                    "[RequestId {request_id}] Could not send response to {peer}: {}",
                    error
                );
            }
            request_response::Event::ResponseSent { peer, request_id } => {
                info!("[RequestId {request_id}] Response sent to {peer}");
            }
        }
    }
}

pub(crate) const FILE_REQ_RES_PROTOCOL: [(StreamProtocol, ProtocolSupport); 1] = [(
    StreamProtocol::new("/file_req_res/1.0.0"),
    ProtocolSupport::Full,
)];

#[derive(NetworkBehaviour)]
pub(crate) struct FileReqResBehaviour {
    req_res: cbor::Behaviour<FileHash, SupplierInfo>,
}

impl FileReqResBehaviour {
    pub(crate) fn new<I: IntoIterator<Item = (StreamProtocol, ProtocolSupport)>>(
        protocols: I,
        config: Config,
    ) -> Self {
        Self {
            req_res: cbor::Behaviour::new(protocols, config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub(crate) struct FileHash(pub(crate) Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FileMetadata {
    pub(crate) file_hash: FileHash,
    pub(crate) supplier_info: SupplierInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SupplierInfo {
    pub(crate) ip: Ipv4Addr,
    pub(crate) port: u16,
    pub(crate) price: i64,
    pub(crate) username: String,
}
