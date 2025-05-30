//! Client for sending requests to the KRPC mod.
use crate::codec;
use crate::error;
use crate::krpc;

use crate::stream::StreamHandle;

use std::net::TcpStream;
use std::net::ToSocketAddrs;

use std::marker::PhantomData;

use protobuf::Message;

/// A client to the RPC server.
#[derive(Debug)]
pub struct RPCClient {
    sock: TcpStream,
    pub(crate) client_id: Vec<u8>,
}

/// Represents a request that can be submitted to the RPCServer. This object is clonable so that
/// you can perform the same request multiple times. For one-off requests, use
/// [`batch_call!`](crate::batch_call!), [`batch_call_unwrap!`](crate::batch_call_unwrap) or
/// [`RPCClient::mk_call`] which provide a nicer API.
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
    results: protobuf::RepeatedField<krpc::ProcedureResult>,
}

/// Represents a procedure call. The type parameter is the type of the value to be extracted from
/// the server's response.
#[derive(Clone)]
pub struct CallHandle<T> {
    proc_call: krpc::ProcedureCall,
    _phantom: PhantomData<T>,
}

impl<T> CallHandle<T>
where
    T: codec::RPCExtractable,
{
    #[doc(hidden)]
    /// Creates a new CallHandle. The function is public so that the generated code from
    /// krpc-mars-terraformer can use it but it is hidden from user docs.
    pub fn new(proc_call: krpc::ProcedureCall) -> Self {
        CallHandle {
            proc_call,
            _phantom: PhantomData,
        }
    }

    /// Creates a streamed version of this call.
    pub fn to_stream(&self) -> CallHandle<StreamHandle<T>> {
        crate::stream::mk_stream(self)
    }

    /// Extract the i-th result from a response.
    pub fn get_result(&self, resp: &RPCResponse, idx: usize) -> Result<T, error::RPCError> {
        codec::extract_result(&resp.results[idx])
    }

    pub(crate) fn get_call(&self) -> &krpc::ProcedureCall {
        &self.proc_call
    }

    pub fn mk_call(&self, client: &mut RPCClient) -> Result<T, error::RPCError> {
        let (result,) = crate::batch_call!(client, (self))?;
        result
    }
}

impl RPCClient {
    /// Connects to the KRPC server. The client will show up in the KRPC UI with the given client name.
    pub fn connect<A: ToSocketAddrs>(
        client_name: &str,
        addr: A,
    ) -> Result<Self, error::ConnectionError> {
        let mut sock = TcpStream::connect(addr)?;

        let mut conn_req = krpc::ConnectionRequest::new();
        conn_req.set_field_type(krpc::ConnectionRequest_Type::RPC);
        conn_req.set_client_name(client_name.to_string());

        conn_req.write_length_delimited_to_writer(&mut sock)?;

        let mut response = codec::read_message::<krpc::ConnectionResponse>(&mut sock)?;

        match response.status {
            krpc::ConnectionResponse_Status::OK => Ok(RPCClient {
                sock,
                client_id: response.client_identifier,
            }),
            s => Err(error::ConnectionError::ConnectionRefused {
                error: response.take_message(),
                status: s,
            }),
        }
    }

    /// Sends a single RPC request to the server.
    pub fn mk_call<T: codec::RPCExtractable>(
        &mut self,
        call: &CallHandle<T>,
    ) -> Result<T, error::RPCError> {
        let (result,) = crate::batch_call!(self, (call))?;
        result
    }

    /// Sends an [`RPCRequest`] to the server. A single RPCRequest may contain multiple RPC calls.
    /// It is recommended to use the [`batch_call!`](crate::batch_call) or
    /// [`batch_call_unwrap!`](crate::batch_call_unwrap) for one-off requests.
    pub fn submit_request(&mut self, request: RPCRequest) -> Result<RPCResponse, error::RPCError> {
        let raw_request = request.build();
        raw_request.write_length_delimited_to_writer(&mut self.sock)?;
        let mut resp = codec::read_message::<krpc::Response>(&mut self.sock)?;
        if resp.has_error() {
            Err(error::RPCError::KRPCRequestErr(resp.take_error()))
        } else {
            let results = resp.take_results();
            Ok(RPCResponse { results })
        }
    }
}
