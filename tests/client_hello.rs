use tokio::{io::AsyncReadExt, net::TcpListener};
use wreq::{Client, Emulation, tls::TlsOptions};

/// Accepts one connection and returns the extensions of its TLS ClientHello as
/// `(type, data)` pairs. The connection is closed afterwards, so the handshake fails.
async fn capture_client_hello_extensions(options: TlsOptions) -> Vec<(u16, Vec<u8>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // TLS record header: type (1), version (2), length (2).
        let mut header = [0; 5];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0], 22, "expected a handshake record");
        let mut record = vec![0; u16::from_be_bytes([header[3], header[4]]) as usize];
        stream.read_exact(&mut record).await.unwrap();
        record
    });

    let client = Client::builder()
        .emulation(
            Emulation::builder()
                .tls_options(options)
                .build(Default::default()),
        )
        .no_proxy()
        .build()
        .unwrap();
    let _ = client
        .get(format!("https://localhost:{}/", addr.port()))
        .send()
        .await;

    parse_extensions(&server.await.unwrap())
}

fn parse_extensions(record: &[u8]) -> Vec<(u16, Vec<u8>)> {
    fn take<'a>(input: &mut &'a [u8], len: usize) -> &'a [u8] {
        let (head, tail) = input.split_at(len);
        *input = tail;
        head
    }
    fn take_u16(input: &mut &[u8]) -> usize {
        let bytes = take(input, 2);
        u16::from_be_bytes([bytes[0], bytes[1]]) as usize
    }

    let mut input = record;
    // Handshake header: type (1) and length (3).
    assert_eq!(take(&mut input, 4)[0], 1, "expected a ClientHello");
    // Legacy version and random.
    take(&mut input, 2 + 32);
    let session_id_len = take(&mut input, 1)[0] as usize;
    take(&mut input, session_id_len);
    let cipher_suites_len = take_u16(&mut input);
    take(&mut input, cipher_suites_len);
    let compression_len = take(&mut input, 1)[0] as usize;
    take(&mut input, compression_len);

    let extensions_len = take_u16(&mut input);
    let mut extensions = take(&mut input, extensions_len);
    let mut parsed = Vec::new();
    while !extensions.is_empty() {
        let kind = take_u16(&mut extensions) as u16;
        let len = take_u16(&mut extensions);
        parsed.push((kind, take(&mut extensions, len).to_vec()));
    }
    parsed
}

const SERVER_PADDING: u16 = 0x12e0;

#[tokio::test]
async fn server_padding_request_is_sent_when_configured() {
    let extensions =
        capture_client_hello_extensions(TlsOptions::builder().server_padding_request(512).build())
            .await;

    let padding = extensions
        .iter()
        .find(|(kind, _)| *kind == SERVER_PADDING)
        .map(|(_, data)| data.as_slice());
    assert_eq!(padding, Some(&[0x02, 0x00][..]));
}

#[tokio::test]
async fn server_padding_request_is_omitted_by_default() {
    let extensions = capture_client_hello_extensions(TlsOptions::default()).await;

    assert!(
        extensions.iter().all(|(kind, _)| *kind != SERVER_PADDING),
        "unexpected server_padding extension"
    );
}
