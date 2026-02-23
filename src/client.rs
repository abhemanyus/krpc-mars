// src/rpc_client.rs

//! Client for sending requests to the KRPC mod.

use crate::codec;
use crate::error;
use crate::krpc;

use crate::krpc::ConnectionResponse_Status;
use crate::prpc;
use crate::stream::StreamHandle;

use prost::Message;
use protobuf::ProtobufEnum;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A client to the RPC server.
#[derive(Debug)]
pub struct RPCClient {
    sock: TcpStream,
    pub(crate) client_id: Vec<u8>,
}

/// Represents a request that can be submitted to the RPCServer.
#[derive(Clone, Default)]
pub struct RPCRequest {
    calls: protobuf::RepeatedField<krpc::ProcedureCall>,
}

impl RPCRequest {
    pub fn add_call<T: codec::RPCExtractable>(&mut self, handle: &CallHandle<T>) {
        self.calls.push(handle.proc_call.clone())
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
    T: Message + std::default::Default,
{
    #[doc(hidden)]
    pub fn new(proc_call: krpc::ProcedureCall) -> Self {
        Self {
            proc_call,
            _phantom: PhantomData,
        }
    }

    pub fn to_stream(&self) -> CallHandle<StreamHandle<T>> {
        todo!();
        // crate::stream::mk_stream(self)
    }

    /// Extract the i-th result from a response.
    pub fn get_result(&self, resp: &RPCResponse, idx: usize) -> Result<T, error::RPCError> {
        let result = resp
            .results
            .get(idx)
            .ok_or(error::RPCError::InvalidResponseIndex)?;
        Ok(T::decode(result.value.as_slice()).unwrap())
    }

    pub(crate) fn get_call(&self) -> &krpc::ProcedureCall {
        &self.proc_call
    }

    /// Executes this call asynchronously.
    pub async fn mk_call(&self, client: &mut RPCClient) -> Result<T, error::RPCError> {
        todo!();
        // client.mk_call(self).await
    }
}

impl RPCClient {
    /// Connects to the KRPC server.
    pub async fn connect<A: ToSocketAddrs + tokio::net::ToSocketAddrs>(
        client_name: &str,
        addr: A,
    ) -> Result<Self, error::ConnectionError> {
        let mut sock = TcpStream::connect(addr).await?;

        let mut conn_req = prpc::ConnectionRequest::default();
        conn_req.set_type(prpc::connection_request::Type::Rpc);
        conn_req.client_name = client_name.to_string();

        let data = conn_req.encode_length_delimited_to_vec();
        sock.write_all(&data).await?;

        let mut buf = Vec::with_capacity(128);
        sock.read_buf(&mut buf).await?;

        let response = prpc::ConnectionResponse::decode_length_delimited(buf.as_slice())
            .expect("failed to decode connection response");

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

    /// Sends a single RPC request to the server.
    pub async fn mk_call<T: codec::RPCExtractable>(
        &mut self,
        call: &CallHandle<T>,
    ) -> Result<T, error::RPCError> {
        let (result,) = crate::batch_call!(self, (call))?;
        result
    }

    /// Sends an [`RPCRequest`] to the server.
    pub async fn submit_request(
        &mut self,
        request: RPCRequest,
    ) -> Result<RPCResponse, error::RPCError> {
        use protobuf::Message;

        let raw_request = request.build();
        let data = raw_request.write_length_delimited_to_bytes()?;
        self.sock.write_all(&data).await?;

        let mut buf = Vec::with_capacity(128);
        self.sock.read_buf(&mut buf).await?;

        let resp = prpc::Response::decode_length_delimited(buf.as_slice())
            .expect("failed to decode response");

        if let Some(err) = resp.error {
            Err(error::RPCError::KRPCRequestErr(err))
        } else {
            Ok(RPCResponse {
                results: resp.results,
            })
        }
    }
}
