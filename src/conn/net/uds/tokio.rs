//! Tokio-based unix socket connector.

use std::{
    io,
    path::Path,
    sync::Arc,
    task::{Context, Poll},
};

use http::Uri;
use tokio::net::UnixStream;

use super::BoxConnecting;
use crate::{
    conn::{Connected, Connection, net::counted::{Counted, ReceiveQueue}, tls_info::TlsInfoFactory},
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
    type Response = Counted<UnixStream>;
    type Error = io::Error;
    type Future = BoxConnecting<Self::Response>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let path = self.path.clone();
        let observer = self.observer.clone();
        Box::pin(async move {
            let stream = UnixStream::connect(path).await?;
            Ok(Counted::new(stream, observer.as_ref()))
        })
    }
}

impl Connection for UnixStream {
    #[inline]
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl TlsInfoFactory for UnixStream {}

impl ReceiveQueue for UnixStream {
    #[cfg(unix)]
    #[inline]
    fn receive_queue_fd(&self) -> Option<std::os::fd::RawFd> {
        Some(std::os::fd::AsRawFd::as_raw_fd(self))
    }
}
