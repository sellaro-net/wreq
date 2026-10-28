//! The transport observer reports exactly the bytes on the wire.
//!
//! Every test puts a relay between the client and its first hop. The relay counts what crosses
//! the client-facing socket; after all connections closed, the client's totals must equal it.
mod support;

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use btls::{
    pkey::PKey,
    ssl::{AlpnError, Ssl, SslAcceptor, SslMethod, select_next_proto},
    x509::X509,
};
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use support::server;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_btls::SslStream;
use wreq::{
    Body, Client, Proxy,
    traffic::{TrafficCounter, TransportInfo, TransportObserver, TransportTraffic},
};

const BODY: usize = 48 * 1024;

/// Byte counts of the client-facing sockets of a [`Relay`].
#[derive(Default)]
struct Wire {
    from_client: AtomicU64,
    to_client: AtomicU64,
    accepted: AtomicU64,
    closed: AtomicU64,
    changed: Notify,
}

/// Forwards every accepted connection to `target`, counting the client side.
struct Relay {
    addr: SocketAddr,
    wire: Arc<Wire>,
}

impl Relay {
    async fn tcp(target: SocketAddr) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let wire = Arc::new(Wire::default());
        let counts = wire.clone();
        tokio::spawn(async move {
            loop {
                let (client, _) = listener.accept().await.unwrap();
                tokio::spawn(relay(client, target, counts.clone()));
            }
        });
        Relay { addr, wire }
    }

    /// Waits until every accepted connection closed in both directions.
    async fn settled(&self) -> (u64, u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let changed = self.wire.changed.notified();
                let accepted = self.wire.accepted.load(Ordering::Acquire);
                if accepted > 0 && self.wire.closed.load(Ordering::Acquire) == accepted {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("relay connections close");
        (
            self.wire.from_client.load(Ordering::Acquire),
            self.wire.to_client.load(Ordering::Acquire),
        )
    }

    fn connections(&self) -> u64 {
        self.wire.accepted.load(Ordering::Acquire)
    }
}

async fn relay<C>(client: C, target: SocketAddr, wire: Arc<Wire>)
where
    C: AsyncRead + AsyncWrite + Send + 'static,
{
    wire.accepted.fetch_add(1, Ordering::AcqRel);
    let upstream = TcpStream::connect(target).await.unwrap();
    let (client_read, client_write) = tokio::io::split(client);
    let (upstream_read, upstream_write) = upstream.into_split();
    // Client to upstream: counted when read from the client socket.
    let up = pump(
        client_read,
        upstream_write,
        &wire.from_client,
        Counted::OnRead,
    );
    // Upstream to client: counted once the client socket accepted it.
    let down = pump(
        upstream_read,
        client_write,
        &wire.to_client,
        Counted::OnWrite,
    );
    tokio::pin!(up, down);
    tokio::select! {
        biased;
        // The client closed: what the upstream sends in answer (a TLS close_notify, an h2
        // GOAWAY) can no longer reach it, so it is not forwarded.
        () = &mut up => {}
        () = &mut down => up.await,
    }
    wire.closed.fetch_add(1, Ordering::AcqRel);
    wire.changed.notify_waiters();
}

enum Counted {
    OnRead,
    OnWrite,
}

async fn pump<R, W>(mut from: R, mut to: W, count: &AtomicU64, counted: Counted)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0_u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if matches!(counted, Counted::OnRead) {
            count.fetch_add(n as u64, Ordering::AcqRel);
        }
        let mut sent = 0;
        while sent < n {
            match to.write(&buf[sent..n]).await {
                Ok(0) | Err(_) => {
                    let _ = to.shutdown().await;
                    return;
                }
                Ok(accepted) => {
                    sent += accepted;
                    if matches!(counted, Counted::OnWrite) {
                        count.fetch_add(accepted as u64, Ordering::AcqRel);
                    }
                }
            }
        }
    }
    let _ = to.shutdown().await;
}

/// Asserts the client's totals against the relay once everything closed.
async fn assert_wire_exact(client: Client, counter: &TrafficCounter, relay: &Relay) {
    drop(client);
    let (from_client, to_client) = relay.settled().await;
    let totals = counter.totals();
    assert_eq!(totals.written, from_client, "written vs wire");
    assert_eq!(totals.read, to_client, "read vs wire");
    assert_eq!(totals.connections, relay.connections(), "connections");
    assert!(from_client > 0 && to_client > 0);
}

fn origin() -> server::Server {
    server::http(|req| async move {
        let received = req.into_body().collect().await.unwrap().to_bytes().len();
        let body = if received > 0 {
            received.to_string().into_bytes()
        } else {
            vec![b'a'; BODY]
        };
        http::Response::new(Body::from(body))
    })
}

/// Two GETs and a POST, so the connection is reused and carries bodies both ways.
async fn exchange(client: &Client, base: &str) {
    for _ in 0..2 {
        let body = client
            .get(format!("{base}/get"))
            .send()
            .await
            .unwrap()
            .bytes();
        assert_eq!(body.await.unwrap().len(), BODY);
    }
    let echoed = client
        .post(format!("{base}/post"))
        .body(vec![b'p'; BODY])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(echoed, BODY.to_string());
}

fn client(counter: &TrafficCounter) -> wreq::ClientBuilder {
    Client::builder()
        .no_proxy()
        .transport_observer(counter.clone())
        .timeout(Duration::from_secs(10))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http1_counts_match_the_wire() {
    let origin = origin();
    let relay = Relay::tcp(origin.addr()).await;
    let counter = TrafficCounter::new();
    let client = client(&counter).http1_only().build().unwrap();

    exchange(&client, &format!("http://{}", relay.addr)).await;

    assert_wire_exact(client, &counter, &relay).await;
    assert_eq!(relay.connections(), 1, "the connection was reused");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http2_counts_match_the_wire() {
    let origin = origin();
    let relay = Relay::tcp(origin.addr()).await;
    let counter = TrafficCounter::new();
    let client = client(&counter).http2_only().build().unwrap();

    exchange(&client, &format!("http://{}", relay.addr)).await;

    assert_wire_exact(client, &counter, &relay).await;
}

/// A TLS origin speaking HTTP/1.1 or h2, chosen by ALPN.
async fn tls_origin() -> SocketAddr {
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    let cert = X509::from_der(include_bytes!("support/server.cert")).unwrap();
    let key = PKey::private_key_from_der(include_bytes!("support/server.key")).unwrap();
    builder.set_certificate(&cert).unwrap();
    builder.set_private_key(&key).unwrap();
    builder.set_alpn_select_callback(|_, offered| {
        select_next_proto(b"\x02h2\x08http/1.1", offered).ok_or(AlpnError::NOACK)
    });
    let acceptor = builder.build();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let ssl = Ssl::new(acceptor.context()).unwrap();
            tokio::spawn(async move {
                let mut tls = SslStream::new(ssl, tcp).unwrap();
                if std::pin::Pin::new(&mut tls).accept().await.is_err() {
                    return;
                }
                let service = hyper::service::service_fn(
                    |req: http::Request<hyper::body::Incoming>| async move {
                        let received = BodyExt::collect(req.into_body()).await?.to_bytes().len();
                        let body = if received > 0 {
                            received.to_string().into_bytes()
                        } else {
                            vec![b'a'; BODY]
                        };
                        Ok::<_, hyper::Error>(http::Response::new(http_body_util::Full::<
                            bytes::Bytes,
                        >::from(
                            body
                        )))
                    },
                );
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(tls), service)
                    .await;
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_counts_include_handshake_and_records() {
    let relay = Relay::tcp(tls_origin().await).await;
    let counter = TrafficCounter::new();
    let client = client(&counter)
        .tls_cert_verification(false)
        .build()
        .unwrap();

    exchange(&client, &format!("https://{}", relay.addr)).await;

    let wire = counter.totals();
    assert_wire_exact(client, &counter, &relay).await;
    // The TLS overhead is part of the count: more than the HTTP payload alone.
    assert!(wire.read > 2 * BODY as u64);
}

/// Answers `CONNECT` with 200 and tunnels, or rejects it with 407.
async fn connect_proxy(accept: bool) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let target = read_connect(&mut client).await;
                if !accept {
                    let _ = client
                        .write_all(
                            b"HTTP/1.1 407 Proxy Authentication Required\r\ncontent-length: 0\r\n\r\n",
                        )
                        .await;
                    let _ = client.shutdown().await;
                    return;
                }
                let mut upstream = TcpStream::connect(target).await.unwrap();
                client
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    addr
}

async fn read_connect(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap();
    assert!(head.starts_with("CONNECT "), "{head}");
    head.split_whitespace().nth(1).unwrap().to_owned()
}

/// Records which peer each connection went to, and sums like [`TrafficCounter`].
#[derive(Clone, Default)]
struct Peers {
    counter: TrafficCounter,
    remotes: Arc<Mutex<Vec<Option<SocketAddr>>>>,
}

impl TransportObserver for Peers {
    fn connected(&self, info: &TransportInfo) -> Arc<dyn TransportTraffic> {
        self.remotes.lock().unwrap().push(info.remote_addr);
        self.counter.connected(info)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_tunnel_counts_the_proxy_leg() {
    let relay = Relay::tcp(connect_proxy(true).await).await;
    let target = tls_origin().await;
    let peers = Peers::default();
    let client = Client::builder()
        .proxy(Proxy::all(format!("http://{}", relay.addr)).unwrap())
        .transport_observer(peers.clone())
        .tls_cert_verification(false)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    exchange(&client, &format!("https://{target}")).await;

    assert_wire_exact(client, &peers.counter, &relay).await;
    let remotes = peers.remotes.lock().unwrap().clone();
    assert_eq!(
        remotes,
        vec![Some(relay.addr)],
        "the socket peer is the proxy"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_connect_still_counts_its_bytes() {
    let relay = Relay::tcp(connect_proxy(false).await).await;
    let counter = TrafficCounter::new();
    let client = Client::builder()
        .proxy(Proxy::all(format!("http://{}", relay.addr)).unwrap())
        .transport_observer(counter.clone())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let error = client
        .get("https://origin.invalid/")
        .send()
        .await
        .unwrap_err();
    assert!(error.is_proxy_connect() || error.is_connect(), "{error:?}");

    assert_wire_exact(client, &counter, &relay).await;
}

#[cfg(feature = "socks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socks5_counts_the_handshake() {
    let origin = origin();
    let relay = Relay::tcp(socks5_proxy().await).await;
    let counter = TrafficCounter::new();
    let client = Client::builder()
        .proxy(Proxy::all(format!("socks5://{}", relay.addr)).unwrap())
        .transport_observer(counter.clone())
        .http1_only()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    exchange(&client, &format!("http://{}", origin.addr())).await;

    assert_wire_exact(client, &counter, &relay).await;
}

/// A SOCKS5 proxy without authentication for IPv4 targets.
#[cfg(feature = "socks")]
async fn socks5_proxy() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut greeting = [0_u8; 2];
                client.read_exact(&mut greeting).await.unwrap();
                let mut methods = vec![0_u8; usize::from(greeting[1])];
                client.read_exact(&mut methods).await.unwrap();
                client.write_all(&[5, 0]).await.unwrap();
                let mut request = [0_u8; 4];
                client.read_exact(&mut request).await.unwrap();
                assert_eq!(request[3], 1, "IPv4 target");
                let mut target = [0_u8; 6];
                client.read_exact(&mut target).await.unwrap();
                let ip = std::net::Ipv4Addr::new(target[0], target[1], target[2], target[3]);
                let port = u16::from_be_bytes([target[4], target[5]]);
                let mut upstream = TcpStream::connect((ip, port)).await.unwrap();
                client
                    .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    addr
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unix_socket_counts_match_the_wire() {
    let origin = origin();
    let path = std::env::temp_dir().join(format!("wreq-traffic-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let wire = Arc::new(Wire::default());
    let counts = wire.clone();
    let target = origin.addr();
    tokio::spawn(async move {
        loop {
            let (client, _) = listener.accept().await.unwrap();
            tokio::spawn(relay(client, target, counts.clone()));
        }
    });
    let relay = Relay { addr: target, wire };
    let counter = TrafficCounter::new();
    let client = Client::builder()
        .proxy(Proxy::unix(path.clone()).unwrap())
        .transport_observer(counter.clone())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    exchange(&client, "http://localhost").await;

    assert_wire_exact(client, &counter, &relay).await;
    let _ = std::fs::remove_file(&path);
}
