//! The transport wrapper behind [`crate::traffic`].

use std::{
    fmt,
    io::{self, IoSlice},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    conn::{Connected, Connection, http::HttpInfo, tls_info::TlsInfoFactory},
    traffic::{TransportInfo, TransportObserver, TransportTraffic},
};

/// The kernel receive queue of a socket: bytes that arrived but were not read yet.
///
/// A connection can close with such bytes queued (a late frame, a TLS session ticket); they
/// crossed the wire, so [`Counted`] reports them as read when it closes.
pub trait ReceiveQueue {
    /// The socket to ask, or `None` when the runtime reads through buffers of its own.
    #[cfg(unix)]
    fn receive_queue_fd(&self) -> Option<std::os::fd::RawFd>;
}

/// A transport connection whose bytes are reported to its [`TransportTraffic`].
pub struct Counted<C> {
    inner: C,
    traffic: Option<Arc<dyn TransportTraffic>>,
    #[cfg(unix)]
    receive_queue: Option<std::os::fd::RawFd>,
}

impl<C: Connection + ReceiveQueue> Counted<C> {
    /// Wraps an established connection; `observer` learns about it before its first byte.
    pub(crate) fn new(inner: C, observer: Option<&Arc<dyn TransportObserver>>) -> Self {
        let traffic = observer.map(|observer| {
            let mut extensions = http::Extensions::new();
            inner.connected().set_extras(&mut extensions);
            let http = extensions.get::<HttpInfo>();
            observer.connected(&TransportInfo {
                remote_addr: http.map(|http| http.remote_addr),
                local_addr: http.map(|http| http.local_addr),
            })
        });
        Self {
            #[cfg(unix)]
            receive_queue: traffic.as_ref().and(inner.receive_queue_fd()),
            inner,
            traffic,
        }
    }
}

impl<C> Drop for Counted<C> {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let (Some(traffic), Some(fd)) = (&self.traffic, self.receive_queue) {
            let unread = unread(fd);
            if unread > 0 {
                traffic.read(unread);
            }
        }
    }
}

/// Bytes queued for reading on `fd`; 0 if the kernel cannot tell.
#[cfg(unix)]
fn unread(fd: std::os::fd::RawFd) -> usize {
    let mut queued: libc::c_int = 0;
    // SAFETY: `fd` is the socket `Counted::inner` owns. It is still open here: fields are
    // dropped only after `Drop::drop` returns. FIONREAD writes one `c_int`.
    #[allow(unsafe_code)]
    let status = unsafe { libc::ioctl(fd, libc::FIONREAD, &mut queued) };
    if status == 0 {
        usize::try_from(queued).unwrap_or(0)
    } else {
        0
    }
}

impl<C> Deref for Counted<C> {
    type Target = C;

    #[inline]
    fn deref(&self) -> &C {
        &self.inner
    }
}

impl<C: Connection> Connection for Counted<C> {
    #[inline]
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}

impl<C: TlsInfoFactory> TlsInfoFactory for Counted<C> {
    #[inline]
    fn tls_info(&self) -> Option<crate::tls::TlsInfo> {
        self.inner.tls_info()
    }
}

impl<C: AsyncRead + Unpin> AsyncRead for Counted<C> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let (Poll::Ready(Ok(())), Some(traffic)) = (&polled, &self.traffic) {
            let delivered = buf.filled().len() - before;
            if delivered > 0 {
                traffic.read(delivered);
            }
        }
        polled
    }
}

impl<C: AsyncWrite + Unpin> AsyncWrite for Counted<C> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.count_written(&polled);
        polled
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.count_written(&polled);
        polled
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<C> Counted<C> {
    #[inline]
    fn count_written(&self, polled: &Poll<io::Result<usize>>) {
        if let (Poll::Ready(Ok(accepted)), Some(traffic)) = (polled, &self.traffic)
            && *accepted > 0
        {
            traffic.written(*accepted);
        }
    }
}

impl<C: fmt::Debug> fmt::Debug for Counted<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counted")
            .field("inner", &self.inner)
            .field("observed", &self.traffic.is_some())
            .finish()
    }
}

#[cfg(all(test, unix, feature = "tokio-rt"))]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::traffic::TrafficCounter;

    #[tokio::test]
    async fn bytes_left_in_the_receive_queue_count_at_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let counter = TrafficCounter::new();
        let observer: Arc<dyn TransportObserver> = Arc::new(counter.clone());
        let mut counted = Counted::new(client, Some(&observer));

        peer.write_all(&[7; 1000]).await.unwrap();
        let mut head = [0; 100];
        counted.read_exact(&mut head).await.unwrap();
        let fd = counted.receive_queue.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while unread(fd) < 900 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(counter.totals().read, 100);

        drop(counted);
        assert_eq!(counter.totals().read, 1000);
    }
}
