// src/rpc_client.rs

//! Client for sending requests to the KRPC mod.

use crate::codec;
use crate::error;
use crate::krpc;

use crate::krpc::ConnectionResponse_Status;
use crate::prpc;
use crate::stream::StreamHandle;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use prost::Message;
use protobuf::ProtobufEnum;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A client to the RPC server.
#[derive(Debug)]
pub struct RPCClient {
    sock: Framed<TcpStream, LengthDelimitedCodec>,
    pub(crate) client_id: Vec<u8>,
}

/// Represents a request that can be submitted to the RPCServer.
#[derive(Clone, Default)]
pub struct RPCRequest {
    calls: protobuf::RepeatedField<krpc::ProcedureCall>,
}

impl RPCRequest {
    pub fn add_call<T: codec::RPCExtractable>(&mut self, handle: &CallHandle<T>) {
        self.calls.push(handle.get_call().clone())
    }

    fn build(self) -> krpc::Request {
        let mut req = krpc::Request::new();
        req.set_calls(self.calls);
        req
    }
}

/// A response from the RPC Server
#[derive(Clone)]
pub struct RPCResponse {
    results: prost::alloc::vec::Vec<prpc::ProcedureResult>,
}

/// Represents a procedure call.
#[derive(Clone)]
pub struct CallHandle<T> {
    proc_call: krpc::ProcedureCall,
    _phantom: PhantomData<T>,
}

impl<T> CallHandle<T>
where
    T: codec::RPCExtractable,
{
    pub fn new(proc_call: krpc::ProcedureCall) -> Self {
        Self {
            proc_call,
            _phantom: PhantomData,
        }
    }

    pub fn to_stream(&self) -> CallHandle<StreamHandle<T>> {
        crate::stream::mk_stream(self)
    }

    pub fn get_result(&self, resp: &RPCResponse, idx: usize) -> Result<T, error::RPCError> {
        let result = resp
            .results
            .get(idx)
            .ok_or(error::RPCError::InvalidResponseIndex)?;

        codec::extract_result::<T>(result)
    }

    pub(crate) fn get_call(&self) -> &krpc::ProcedureCall {
        &self.proc_call
    }

    pub async fn mk_call(&self, client: &mut RPCClient) -> Result<T, error::RPCError> {
        client.mk_call(self).await
    }
}

impl RPCClient {
    /// Connects to the KRPC server.
    pub async fn connect<A: ToSocketAddrs + tokio::net::ToSocketAddrs>(
        client_name: &str,
        addr: A,
    ) -> Result<Self, error::ConnectionError> {
        let stream = TcpStream::connect(addr).await?;
        let mut sock = Framed::new(stream, LengthDelimitedCodec::new());

        let mut conn_req = prpc::ConnectionRequest::default();
        conn_req.set_type(prpc::connection_request::Type::Rpc);
        conn_req.client_name = client_name.to_string();

        let mut buf = Vec::new();
        conn_req.encode(&mut buf).unwrap();
        sock.send(Bytes::from(buf)).await?;

        let response_bytes = sock
            .next()
            .await
            .ok_or(error::ConnectionError::ConnectionClosed)??;

        let response = prpc::ConnectionResponse::decode(response_bytes)?;

        match response.status() {
            prpc::connection_response::Status::Ok => Ok(Self {
                sock,
                client_id: response.client_identifier,
            }),
            _ => Err(error::ConnectionError::ConnectionRefused {
                error: response.message,
                status: ConnectionResponse_Status::from_i32(response.status)
                    .expect("invalid status"),
            }),
        }
    }

    /// Sends a single RPC request.
    pub async fn mk_call<T: codec::RPCExtractable>(
        &mut self,
        call: &CallHandle<T>,
    ) -> Result<T, error::RPCError> {
        let (result,) = crate::batch_call!(self, (call))?;
        result
    }

    /// Sends an RPCRequest to the server.
    pub async fn submit_request(
        &mut self,
        request: RPCRequest,
    ) -> Result<RPCResponse, error::RPCError> {
        use protobuf::Message;

        let raw_request = request.build();
        let bytes = raw_request.write_to_bytes()?;
        self.sock.send(Bytes::from(bytes)).await?;

        let response_bytes = self
            .sock
            .next()
            .await
            .ok_or(error::RPCError::ConnectionClosed)??;

        let resp = prpc::Response::decode(response_bytes)?;

        if let Some(err) = resp.error {
            Err(error::RPCError::KRPCRequestErr(err))
        } else {
            Ok(RPCResponse {
                results: resp.results,
            })
        }
    }
}
