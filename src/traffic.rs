//! Byte accounting at the transport level.
//!
//! A [`TransportObserver`] set with [`ClientBuilder::transport_observer`] sees every transport
//! connection the client opens (TCP and Unix sockets), before its first byte, and every byte
//! that connection moves: proxy handshakes (HTTP `CONNECT`, SOCKS), TLS handshakes and records,
//! HTTP/1 heads and bodies, HTTP/2 frames, including connections that fail later (for example a
//! rejected `CONNECT`).
//!
//! # What is counted
//!
//! - **Written:** the bytes the socket accepted.
//! - **Read:** the bytes the socket delivered, plus, on Unix, the bytes still queued in the
//!   kernel when the connection closes: they crossed the wire although nobody read them.
//!
//! Connection attempts that never connect move no bytes and are not reported. TCP/IP headers
//! are not part of the count, and neither are bytes still in flight when a connection closes.
//!
//! # Runtimes
//!
//! With the tokio runtime the count is taken at the socket itself. The compio runtime buffers
//! reads and writes in user space; there the count is what that buffer accepted and delivered,
//! and data left in it at close is not included.
//!
//! [`ClientBuilder::transport_observer`]: crate::ClientBuilder::transport_observer

use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// The endpoints of one transport connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransportInfo {
    /// The peer the socket is connected to: the proxy, or the origin without a proxy.
    pub remote_addr: Option<SocketAddr>,
    /// The local end of the socket.
    pub local_addr: Option<SocketAddr>,
}

/// Receives the bytes one transport connection moves.
pub trait TransportTraffic: Send + Sync + 'static {
    /// `bytes` were delivered by the socket.
    fn read(&self, bytes: usize);
    /// `bytes` were accepted by the socket.
    fn written(&self, bytes: usize);
}

/// Observes every transport connection a client opens.
pub trait TransportObserver: Send + Sync + 'static {
    /// Called once per established connection, before any byte moves over it. The returned
    /// [`TransportTraffic`] receives every byte of exactly this connection.
    fn connected(&self, info: &TransportInfo) -> Arc<dyn TransportTraffic>;
}

/// Totals of a [`TrafficCounter`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TrafficTotals {
    /// Bytes delivered by the sockets.
    pub read: u64,
    /// Bytes accepted by the sockets.
    pub written: u64,
    /// Established connections.
    pub connections: u64,
}

/// A [`TransportObserver`] that sums the traffic of all connections of a client.
///
/// Cloning shares the totals.
#[derive(Clone, Default)]
pub struct TrafficCounter {
    totals: Arc<Totals>,
}

#[derive(Default)]
struct Totals {
    read: AtomicU64,
    written: AtomicU64,
    connections: AtomicU64,
}

impl TrafficCounter {
    /// Creates a counter at zero.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// The totals so far.
    pub fn totals(&self) -> TrafficTotals {
        TrafficTotals {
            read: self.totals.read.load(Ordering::Acquire),
            written: self.totals.written.load(Ordering::Acquire),
            connections: self.totals.connections.load(Ordering::Acquire),
        }
    }
}

impl fmt::Debug for TrafficCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TrafficCounter")
            .field(&self.totals())
            .finish()
    }
}

impl TransportTraffic for Totals {
    #[inline]
    fn read(&self, bytes: usize) {
        self.read.fetch_add(bytes as u64, Ordering::AcqRel);
    }

    #[inline]
    fn written(&self, bytes: usize) {
        self.written.fetch_add(bytes as u64, Ordering::AcqRel);
    }
}

impl TransportObserver for TrafficCounter {
    fn connected(&self, _: &TransportInfo) -> Arc<dyn TransportTraffic> {
        self.totals.connections.fetch_add(1, Ordering::AcqRel);
        self.totals.clone()
    }
}
