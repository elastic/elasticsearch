Test TLS material for `FakeUsageApi`. Test-only, committed on purpose: the keys are unencrypted and protect nothing.
All certificates are valid for 9999 days from 2026-11-24 (until 2054) and use RSA 2048.

| File | What it is |
|---|---|
| `ca.crt` | Test CA (`CN=Usage API Test CA`). The fixture trusts it for client certificates; the plugin trusts it for the server. |
| `server.crt`, `server.key` | Fixture server certificate (`CN=usage-api`), SAN `localhost`, `127.0.0.1`, `::1`, `serverAuth`. |
| `client.crt`, `client.key` | The identity the plugin presents (`CN=es-hosted-node`, `clientAuth`), signed by `ca.crt`. |
| `other-client.crt`, `other-client.key` | A second valid identity (`CN=other-node`), for tests that accept only one client. |
| `rogue-ca.crt` | A CA the fixture does not trust (only needed to sign `rogue-client`; the key was discarded). |
| `rogue-client.crt`, `rogue-client.key` | `CN=es-hosted-node` signed by `rogue-ca`: the TLS handshake must fail. |

The CA private keys were discarded after signing, so to change anything regenerate the whole set with `openssl req -x509` for the two
CAs and `openssl req` plus `openssl x509 -req -CA` for each leaf (PKCS#8 keys: `openssl pkcs8 -topk8 -nocrypt`), keeping the subjects and
extensions above.
