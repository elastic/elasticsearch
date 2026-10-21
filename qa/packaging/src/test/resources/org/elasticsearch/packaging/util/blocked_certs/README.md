# Blocked certificate check

`BlockedCertificateCheck.java` and the certificates in this directory are used by `BlockedCertificates` in the packaging tests.
The tests run the program with the JDK bundled in each distribution and expect it to reject both intermediates below as untrusted, because the build adds them to the JDK's `lib/security/blocked.certs`.
See `BundledJdkBlockedCertsTask` and `distribution/build.gradle`.

## Certificates

The private keys of eight eMudhra intermediate CAs were compromised in October 2026, see https://bugzilla.mozilla.org/show_bug.cgi?id=2083449.
Each of them was issued by an emSign G3 root and cross-signed by Sectigo's AAA Certificate Services root.
The test uses one of the eight, `EM DV TLS CA - G3B`, in both forms, each paired with the root that issued it.

| File | Certificate | SHA-256 fingerprint |
|---|---|---|
| `aaa-certificate-services.pem` | AAA Certificate Services (root) | `D7A7A0FB5D7E2731D771E9484EBCDEF71D5F0C3E0A2948782BC83EE0EA699EF4` |
| `em-dv-tls-ca-g3b-aaa.pem` | EM DV TLS CA - G3B, issued by AAA Certificate Services | `45A883F349AEDFD5F3A649B5498FFB121712BA8B448F3F2A91DBC5A766FCDDBE` |
| `emsign-root-tls-ca-g3.pem` | emSign Root TLS CA - G3 (root) | `7DD78D5F4F13459A83DFF9ABBBA62EDBAF6F2D102BF257FD712F4D9F2746ED8D` |
| `em-dv-tls-ca-g3b-emsign.pem` | EM DV TLS CA - G3B, issued by emSign Root TLS CA - G3 | `42C4ACCD12C4F59597925E1A08A7A90102608543809308D50EC427E118BA1493` |

## Provenance

The PEM files were taken from the incident disclosure site, https://emudhra.we-have-a-problem.com/ (`impacted-intermediates.json` and `roots.json`), and checked against independent sources on 2026-10-21:

- Both intermediates, and all 16 fingerprints listed in `distribution/build.gradle`, are in the CCADB list of revoked intermediates (https://ccadb.my.salesforce-sites.com/mozilla/PublicIntermediateCertsRevokedWithPEMCSV) with revocation reason `cACompromise`, and the PEM published there is byte for byte identical.
- CCADB lists no other eMudhra intermediate revoked for `cACompromise`.
- The 16 certificates carry 8 distinct public keys, which are exactly the 8 SPKI hashes Go distrusts in https://github.com/golang/go/commit/7f86f957633ee31a1c17a1eb8452ca7a3d9bc141 (CVE-2026-111943).
- Mozilla's OneCRL lists the 6 emSign-issued TLS intermediates by issuer and serial number.
- AAA Certificate Services matches the `comodoaaaca` root in the OpenJDK 27 `cacerts`.
- emSign Root TLS CA - G3 matches the root in the CCADB report of roots included by Microsoft (https://ccadb.my.salesforce-sites.com/microsoft/IncludedCACertificateReportForMSFTCSV).
- Each intermediate's signature verifies with the root it is paired with (`openssl verify -partial_chain`).
