use crate::{krpc, prpc};

/// Errors that can occur while trying to connect to the KRPC server.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// Could not connect to the server
    #[error(transparent)]
    ConnectionFailed(#[from] std::io::Error),

    /// Protobuf error when doing the handshake
    #[error(transparent)]
    ProtobufErr(#[from] protobuf::ProtobufError),

    /// Server refused the connection
    #[error("Connection refused by the server (status {status:?}): {error}")]
    ConnectionRefused {
        error: String,
        status: krpc::ConnectionResponse_Status,
    },
}

/// Errors that can occur when performing an RPC.
#[derive(Debug, thiserror::Error)]
pub enum RPCError {
    /// IO Error while sending/reading
    #[error(transparent)]
    IOErr(#[from] std::io::Error),
    /// An error raised by the kRPC mod
    #[error(
        "The RPC request failed: service={} procedure={} description={}",
        .0.service, .0.name, .0.description)
    ]
    KRPCRequestErr(prpc::Error),
    /// Some protobuf error on the request/response level
    #[error(transparent)]
    ProtobufErr(#[from] protobuf::ProtobufError),
}
