# Patches

This fork follows upstream `wreq` and carries the patches below. Each patch is one commit on
`main`. Behavior without the new options is unchanged unless stated otherwise.

| Commit | Area | Change | Default |
|---|---|---|---|
| `66aa85c` | connect | `ClientBuilder::resolver_enforced_egress(bool)`: the configured resolver decides the address of every outgoing socket, including IP-literal hosts. HTTP proxies are used through a CONNECT tunnel to the resolved numeric address; SOCKS proxies receive a locally resolved numeric address. Host, SNI and certificate verification keep the request authority. | off |
| `f3b8142` | decoder | `ClientBuilder::auto_accept_encoding(bool)`: when off, the decompression layer neither adds `Accept-Encoding` nor rewrites `Range` requests; supported encodings are still decoded. | on |
| `40e1053` | socks | `SocksError` exposes its inner error through `Error::source`. With local resolution, SOCKS4 uses the first IPv4 answer instead of failing on an IPv6-first answer. | – |
| `ebfd6a8` | socks | SOCKS4 sends the proxy URL username as the user ID. | – |
| `d628e51` | tunnel | The CONNECT response head is bounded to 16 KiB; a longer head fails the tunnel. | – |
| `68be7d5` | pool | A never-retry policy also disables the pool's transparent retry of requests canceled on a reused connection, and a speculative connection that loses against a pooled one is dropped instead of completed in the background. | unchanged for the default policy |
| `0689b69` | tls | `TlsOptions::server_padding_request(Option<u16>)` sends the `server_padding` ClientHello extension (0x12e0) on origin handshakes. | `None` |
| `1aa7226` | http2 | `RequestBuilder::headers_priority(HeadersPriority)` overrides the HEADERS frame priority of one request; `wreq::http2` re-exports `HeadersPriority`. | connection default |
| `36b819b` | deps | `http2` is pinned to the upstream repository revision that provides `ext::HeadersPriority`. | – |
| `5884ca9` | traffic | `ClientBuilder::transport_observer(impl TransportObserver)` reports each transport connection (TCP, Unix) before its first byte and every byte it moves, at the socket below proxy handshakes and TLS; bytes still queued in the kernel at close count as read (Unix). `TrafficCounter` sums a client's connections. | no observer |

## Dependencies outside crates.io

Dependents must apply the same `[patch.crates-io]` entries as this workspace:

| Crate | Source | Revision |
|---|---|---|
| `http2` | `https://github.com/0x676e67/http2` | `bdb2c60cd9b4125c28913191b6f758d00e5f136c` |
| `btls`, `btls-sys` | `https://github.com/0x676e67/btls` | branch `main`, locked at `1e980877343872b671d18a185395e31c10cc51d5`; pin this revision |

## Maintenance

- Upstream changes are taken in deliberately: `git merge` from upstream `main`, never a rebase, so
  pinned revisions stay reachable.
- A new patch is one commit with its own entry in this file.
- Releases are tags `v<upstream version>-sellaro.<n>`; nothing is published to crates.io.
