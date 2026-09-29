mod support;
use std::{env, sync::LazyLock};

use support::server;
use tokio::sync::Mutex;
use wreq::Client;

// serialize tests that read from / write to environment variables
static HTTP_PROXY_ENV_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[tokio::test]
async fn http_proxy() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");

        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());

    let res = Client::builder()
        .proxy(wreq::Proxy::http(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn http_proxy_basic_auth() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );

        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());

    let res = Client::builder()
        .proxy(
            wreq::Proxy::http(&proxy)
                .unwrap()
                .basic_auth("Aladdin", "open sesame"),
        )
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn http_proxy_basic_auth_parsed() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );

        async { http::Response::default() }
    });

    let proxy = format!("http://Aladdin:open%20sesame@{}", server.addr());

    let res = Client::builder()
        .proxy(wreq::Proxy::http(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);

    let res = wreq::get(url)
        .proxy(wreq::Proxy::http(&proxy).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn system_http_proxy_basic_auth_parsed() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuc2VzYW1l"
        );

        async { http::Response::default() }
    });

    // avoid races with other tests that change "http_proxy"
    let _env_lock = HTTP_PROXY_ENV_MUTEX.lock().await;

    // save system setting first.
    let system_proxy = env::var("http_proxy");

    // set-up http proxy.
    unsafe {
        env::set_var(
            "http_proxy",
            format!("http://Aladdin:opensesame@{}", server.addr()),
        )
    }

    let res = Client::builder()
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);

    // reset user setting.
    unsafe {
        match system_proxy {
            Err(_) => env::remove_var("http_proxy"),
            Ok(proxy) => env::set_var("http_proxy", proxy),
        }
    }
}

#[tokio::test]
async fn test_no_proxy() {
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), "/4");

        async { http::Response::default() }
    });
    let proxy = format!("http://{}", server.addr());
    let url = format!("http://{}/4", server.addr());

    // set up proxy and use no_proxy to clear up client builder proxies.
    let res = Client::builder()
        .proxy(wreq::Proxy::http(&proxy).unwrap())
        .no_proxy()
        .build()
        .unwrap()
        .get(&url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url.as_str());
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn test_using_system_proxy() {
    let url = "http://not.a.real.sub.hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "not.a.real.sub.hyper.rs.local");

        async { http::Response::default() }
    });

    // avoid races with other tests that change "http_proxy"
    let _env_lock = HTTP_PROXY_ENV_MUTEX.lock().await;

    // save system setting first.
    let system_proxy = env::var("http_proxy");
    // set-up http proxy.
    unsafe {
        env::set_var("http_proxy", format!("http://{}", server.addr()));
    }
    // system proxy is used by default
    let res = wreq::get(url).send().await.unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);

    // reset user setting.
    unsafe {
        match system_proxy {
            Err(_) => env::remove_var("http_proxy"),
            Ok(proxy) => env::set_var("http_proxy", proxy),
        }
    }
}

#[tokio::test]
async fn http_over_http() {
    let url = "http://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");

        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());

    let res = Client::builder()
        .proxy(wreq::Proxy::http(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn http_proxy_custom_headers() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );
        assert_eq!(req.headers()["x-custom-header"], "value");

        async { http::Response::default() }
    });

    let proxy = format!("http://Aladdin:open%20sesame@{}", server.addr());

    let proxy = wreq::Proxy::http(&proxy).unwrap().custom_http_headers({
        let mut headers = http::HeaderMap::new();
        headers.insert("x-custom-header", "value".parse().unwrap());
        headers
    });

    let res = Client::builder()
        .proxy(proxy.clone())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);

    let res = wreq::get(url).proxy(proxy).send().await.unwrap();

    assert_eq!(res.uri(), url);
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn tunnel_detects_auth_required() {
    let url = "https://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "CONNECT");
        assert_eq!(req.uri(), "hyper.rs.local:443");
        assert!(
            !req.headers()
                .contains_key(http::header::PROXY_AUTHORIZATION)
        );

        async {
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::PROXY_AUTHENTICATION_REQUIRED;
            res
        }
    });

    let proxy = format!("http://{}", server.addr());

    let err = Client::builder()
        .proxy(wreq::Proxy::https(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();

    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("auth"),
        "proxy auth err expected, got: {err:?}"
    );
}

#[tokio::test]
async fn tunnel_includes_proxy_auth() {
    let url = "https://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "CONNECT");
        assert_eq!(req.uri(), "hyper.rs.local:443");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );

        async {
            // return 400 to not actually deal with TLS tunneling
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::BAD_REQUEST;
            res
        }
    });

    let proxy = format!("http://Aladdin:open%20sesame@{}", server.addr());

    let err = Client::builder()
        .proxy(wreq::Proxy::https(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();

    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("unsuccessful"),
        "tunnel unsuccessful expected, got: {err:?}"
    );
}

#[tokio::test]
async fn tunnel_includes_user_agent() {
    let url = "https://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "CONNECT");
        assert_eq!(req.uri(), "hyper.rs.local:443");
        assert_eq!(req.headers()["user-agent"], "wreq-test");

        async {
            // return 400 to not actually deal with TLS tunneling
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::BAD_REQUEST;
            res
        }
    });

    let proxy = format!("http://{}", server.addr());

    let err = Client::builder()
        .proxy(wreq::Proxy::https(&proxy).unwrap().custom_http_headers({
            let mut headers = http::HeaderMap::new();
            headers.insert("user-agent", "wreq-test".parse().unwrap());
            headers
        }))
        .user_agent("wreq-test")
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();

    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("unsuccessful"),
        "tunnel unsuccessful expected, got: {err:?}"
    );
}

#[tokio::test]
async fn proxy_tunnel_connect_error() {
    let client = Client::builder()
        .tls_cert_verification(false)
        .no_proxy()
        .build()
        .unwrap();

    let invalid_proxies = vec![
        "http://invalid.proxy:8080",
        "https://invalid.proxy:8080",
        "socks4://invalid.proxy:8080",
        "socks4a://invalid.proxy:8080",
        "socks5://invalid.proxy:8080",
        "socks5h://invalid.proxy:8080",
    ];

    let target_urls = ["https://example.com", "http://example.com"];

    for proxy in invalid_proxies {
        for url in target_urls {
            let err = client
                .get(url)
                .proxy(wreq::Proxy::all(proxy).unwrap())
                .send()
                .await
                .unwrap_err();

            assert!(
                err.is_proxy_connect(),
                "proxy connect error expected, got: {err:?}"
            );
        }
    }
}

/// Resolver that maps a fixed name table and records every name it was asked for.
#[derive(Clone, Default)]
struct TableResolver {
    table: std::sync::Arc<std::collections::HashMap<&'static str, std::net::SocketAddr>>,
    queried: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl TableResolver {
    fn new(entries: &[(&'static str, std::net::SocketAddr)]) -> Self {
        Self {
            table: std::sync::Arc::new(entries.iter().copied().collect()),
            queried: Default::default(),
        }
    }

    fn queried(&self) -> Vec<String> {
        self.queried.lock().unwrap().clone()
    }
}

impl wreq::dns::Resolve for TableResolver {
    fn resolve(&self, name: wreq::dns::Name) -> wreq::dns::Resolving {
        self.queried.lock().unwrap().push(name.as_str().to_owned());
        let answer = self.table.get(name.as_str()).copied();
        Box::pin(async move {
            match answer {
                Some(addr) => Ok(Box::new(std::iter::once(addr)) as wreq::dns::Addrs),
                None => Err("name rejected by resolver".into()),
            }
        })
    }
}

#[tokio::test]
async fn resolver_enforced_egress_resolves_ip_literals() {
    let server = server::http(move |_req| async { http::Response::default() });
    let port = server.addr().port();
    let resolver = TableResolver::new(&[("192.0.2.10", ([127, 0, 0, 1], 0).into())]);

    let client = Client::builder()
        .no_proxy()
        .dns_resolver(resolver.clone())
        .resolver_enforced_egress(true)
        .build()
        .unwrap();

    // The literal is unreachable; only the resolver answer leads to the test server.
    let res = client
        .get(format!("http://192.0.2.10:{port}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), wreq::StatusCode::OK);
    assert_eq!(resolver.queried(), ["192.0.2.10"]);

    // A literal the resolver rejects must fail instead of being dialed directly.
    let err = client
        .get(format!("http://127.0.0.1:{port}/"))
        .send()
        .await
        .unwrap_err();
    assert!(err.is_connect(), "connect error expected, got: {err:?}");
}

#[tokio::test]
async fn resolver_enforced_egress_tunnels_http_to_numeric_target() {
    let connect_target = std::sync::Arc::new(std::sync::Mutex::new(None));
    let recorded = connect_target.clone();
    let server = server::http(move |req| {
        *recorded.lock().unwrap() = Some((
            req.method().clone(),
            req.uri().to_string(),
            req.headers().get("host").cloned(),
        ));
        async {
            // return 400 to not actually deal with tunneling
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::BAD_REQUEST;
            res
        }
    });

    let resolver = TableResolver::new(&[
        ("127.0.0.1", server.addr()),
        ("hyper.rs.local", ([192, 0, 2, 10], 0).into()),
    ]);
    let proxy = format!("http://{}", server.addr());

    let err = Client::builder()
        .proxy(wreq::Proxy::http(&proxy).unwrap())
        .dns_resolver(resolver.clone())
        .resolver_enforced_egress(true)
        .build()
        .unwrap()
        .get("http://hyper.rs.local/prox")
        .send()
        .await
        .unwrap_err();

    // No fallback to forward-proxying after the rejected CONNECT.
    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("unsuccessful"),
        "tunnel unsuccessful expected, got: {err:?}"
    );
    let (method, uri, host) = connect_target.lock().unwrap().take().unwrap();
    assert_eq!(method, http::Method::CONNECT);
    assert_eq!(uri, "192.0.2.10:80");
    assert_eq!(host.unwrap(), "192.0.2.10:80");
    assert!(
        resolver
            .queried()
            .iter()
            .any(|name| name == "hyper.rs.local")
    );
}
