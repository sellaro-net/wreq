//! Compio-based unix socket connector.

#![allow(dead_code)]

use std::{
    io,
    path::Path,
    sync::Arc,
    task::{Context, Poll},
};

use compio::net::UnixStream;
use http::Uri;
use wreq_rt::rt::compio::{future::SendFuture, io::CompioIO};

use super::BoxConnecting;
use crate::{
    conn::{
        Connected, Connection,
        net::{
            counted::{Counted, ReceiveQueue},
            io::CompioConnection,
        },
        tls_info::TlsInfoFactory,
    },
    traffic::TransportObserver,
};

#[derive(Clone)]
pub struct UnixConnector {
    path: Arc<Path>,
    observer: Option<Arc<dyn TransportObserver>>,
}

impl UnixConnector {
    /// Creates a new [`UnixConnector`] for the specified socket path; `observer` sees every
    /// byte of its connections.
    #[inline]
    pub fn new(
        path: impl Into<Arc<Path>>,
        observer: Option<Arc<dyn TransportObserver>>,
    ) -> Self {
        Self {
            path: path.into(),
            observer,
        }
    }
}

impl tower::Service<Uri> for UnixConnector {
    type Response = Counted<CompioConnection<UnixStream>>;
    type Error = io::Error;
    type Future = BoxConnecting<Self::Response>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let fut = UnixStream::connect(self.path.clone());
        let observer = self.observer.clone();
        Box::pin(SendFuture::new(async move {
            let io = fut.await?;
            let connection = CompioConnection {
                peer_addr: None,
                local_addr: None,
                inner: CompioIO::new(io),
            };
            Ok(Counted::new(connection, observer.as_ref()))
        }))
    }
}

impl Connection for CompioConnection<UnixStream> {
    #[inline]
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl TlsInfoFactory for CompioConnection<UnixStream> {}

impl ReceiveQueue for CompioConnection<UnixStream> {
    /// compio reads through its own buffer; the kernel queue is not the whole story.
    #[cfg(unix)]
    #[inline]
    fn receive_queue_fd(&self) -> Option<std::os::fd::RawFd> {
        None
    }
}
