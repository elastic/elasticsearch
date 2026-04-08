# Deep Dive: Inline Encrypted Credentials for ES|QL External Data Sources

## Executive Summary

Elasticsearch has **three distinct encryption systems** already in the codebase, plus a hash-based consistency checking mechanism. The Watcher `CryptoService` is the closest precedent to what external data source credential encryption needs: it encrypts secrets at rest using a symmetric key stored in the keystore, and the encrypted values live in an Elasticsearch index (`.watches`). The `TokenService` provides a more modern precedent using AES-GCM with key distribution via cluster state. Both provide patterns that can be directly reused.

---

## 1. KeyStoreWrapper Encryption

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/KeyStoreWrapper.java`

### Algorithm Details

| Parameter | Value | Line |
|-----------|-------|------|
| KDF Algorithm | `PBKDF2WithHmacSHA512` | Line 127 |
| KDF Iterations | 210,000 | Line 130 |
| Cipher Key Bits | 256 (as of v7/ES 9.0) | Lines 123, 133 |
| Cipher Algorithm | `AES` | Line 139 |
| Cipher Mode | `GCM` | Line 142 |
| Cipher Padding | `NoPadding` | Line 145 |
| GCM Tag Bits | 128 | Line 136 |
| Salt Size | 64 bytes | Line 462 |
| IV Size | 12 bytes (96 bits, per NIST SP 800-38D) | Line 466 |

### Key Derivation Flow (lines 317-336)

```java
private static Cipher createCipher(int opmode, char[] password, byte[] salt, byte[] iv, int kdfIters, int cipherKeyBits)
    throws GeneralSecurityException {
    PBEKeySpec keySpec = new PBEKeySpec(password, salt, kdfIters, cipherKeyBits);
    SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KDF_ALGO);
    SecretKey secretKey = keyFactory.generateSecret(keySpec);
    SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), CIPHER_ALGO);
    GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_BITS, iv);
    Cipher cipher = Cipher.getInstance(CIPHER_ALGO + "/" + CIPHER_MODE + "/" + CIPHER_PADDING);
    cipher.init(opmode, secret, spec);
    cipher.updateAAD(salt);  // Additional Authenticated Data = salt
    return cipher;
}
```

**Key Points**:
- Password-derived key via PBKDF2 (password can be empty for default keystores)
- AES-256-GCM provides both confidentiality and integrity (AEAD)
- Salt used as Additional Authenticated Data (AAD) for GCM
- Fresh random salt + IV generated on every save (lines 459-467)
- File format versioned (currently v7), with backward compatibility for older iteration counts (10k) and key sizes (128-bit)

### Storage Format (lines 454-487)

The encrypted keystore is a Lucene file with header/footer checksums:
1. Lucene codec header
2. `hasPassword` byte (0 or 1)
3. Data block: `[salt_len][salt][iv_len][iv][encrypted_len][encrypted_bytes]`
4. Lucene codec footer (CRC32 checksum)

### Relevance for Data Source Credentials

The `KeyStoreWrapper` is a **local file per node** -- it cannot be used directly for storing data source credentials that need to be replicated across the cluster. However, the cryptographic primitives (AES-256-GCM, PBKDF2, salt/IV management) are the gold standard in the codebase and should be reused.

---

## 2. Cluster State Encryption Precedent: TokenService

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/authc/TokenService.java`

### The TokenMetadata Pattern

**TokenMetadata** (lines 23-104 in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/security/authc/TokenMetadata.java`) stores encryption keys **directly in cluster state**:

```java
public final class TokenMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {
    public static final String TYPE = "security_tokens";
    private final List<KeyAndTimestamp> keys;
    private final byte[] currentKeyHash;
}
```

- Keys are stored as `SecureString` values with timestamps (via `KeyAndTimestamp` at `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/security/authc/KeyAndTimestamp.java`)
- `isPrivate()` returns `true` (line 100-103) -- never sent to clients via the cluster state API
- `toXContentChunked()` returns empty iterator (lines 68-71) -- never serialized to REST responses
- Published to cluster state via `installTokenMetadata()` (lines 2460-2491 of TokenService.java)

### TokenService Encryption Algorithm (lines 164-178 of TokenService.java)

| Parameter | Value | Line |
|-----------|-------|------|
| KDF Algorithm | `PBKDF2withHMACSHA512` | Line 173 |
| Service Key Iterations | 100,000 | Line 171 |
| Token Encryption Key Iterations | 1,024 | Line 172 |
| Salt Bytes | 32 | Line 174 |
| Key Bytes | 64 | Line 175 |
| IV Bytes | 12 | Line 176 |
| Cipher | `AES/GCM/NoPadding` | Line 178 |
| GCM Tag Bits | 128 | Line 2116 |

### Key Generation and Distribution (lines 2404-2414)

```java
private SecureString generateTokenKey() {
    byte[] keyBytes = new byte[KEY_BYTES];  // 64 bytes
    secureRandom.nextBytes(keyBytes);
    encode = Strings.BASE_64_NO_PADDING_URL_ENCODER.encode(keyBytes);
    // ... convert to SecureString
}
```

- Each node generates a key on startup
- Master node publishes keys to cluster state via `ClusterStateUpdateTask` (line 2463)
- Non-master nodes receive keys via cluster state updates and call `refreshMetadata()` (line 2380)
- Key cache uses `Cache<BytesKey, SecretKey>` for derived key caching (line 2503-2530)

### Key Rotation (lines 2380-2401)

The `refreshMetadata()` method handles key rotation:
- Old keys retained in cache for decrypting previously encrypted tokens
- Current key identified by `currentKeyHash`
- Timestamps track key creation time

### Superseding Token Encryption (lines 1282-1304, 1549-1559)

When tokens are refreshed, the new tokens are encrypted and stored alongside:
```java
updateMap.put("superseding.encrypted_tokens", encryptedAccessAndRefreshToken);
updateMap.put("superseding.encryption_iv", Base64.getEncoder().encodeToString(iv));
updateMap.put("superseding.encryption_salt", Base64.getEncoder().encodeToString(salt));
```

This is a direct precedent for storing encrypted credential blobs alongside metadata in an index document.

---

## 3. Security Plugin Credential Storage

**LDAP Bind Passwords**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/security/authc/ldap/PoolingSessionFactorySettings.java`

The security plugin stores all sensitive credentials in the **node-local keystore**, NOT in cluster state:

```java
// Line 43-47
public static final Function<String, Setting.AffixSetting<SecureString>> SECURE_BIND_PASSWORD = realmType -> Setting.affixKeySetting(
    RealmSettings.realmSettingPrefix(realmType),
    "secure_bind_password",
    key -> secureString(key, null)
);
```

Pattern: `xpack.security.authc.realms.<type>.<name>.secure_bind_password` stored in `elasticsearch.keystore`.

**API Keys**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/authc/ApiKeyService.java`
- API key secrets are **hashed** (not encrypted) using SSHA256 by default (line 167)
- Hashes stored in the `.security-7` index
- Hashing is done via the `Hasher` enum, which supports bcrypt and PBKDF2
- This is one-way -- API keys can only be verified, never recovered

**Conclusion**: The security plugin has NO precedent for storing encrypted (recoverable) credentials in cluster state or indices. All sensitive configs use the keystore (per-node, not cluster-replicated). API keys use one-way hashing.

---

## 4. Watcher Encrypted Credentials

**CryptoService**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/crypto/CryptoService.java`

### Algorithm

| Parameter | Value | Line |
|-----------|-------|------|
| Encryption Algorithm | `AES/CTR/NoPadding` | Line 53 |
| Key Algorithm | `AES` | Line 54 |
| Key Length | 128 bits | Line 55 |
| System Key Algorithm | `HmacSHA512` | Line 41 |
| System Key Size | 1024 bits | Line 42 |
| Key Derivation | SHA-256 hash of system key, truncated | Lines 205-221 |

### Encryption Flow

1. **System key** generated via `SystemKeyTool` (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/crypto/tool/SystemKeyTool.java`, lines 84-96): `KeyGenerator.getInstance("HmacSHA512")` with 1024-bit key
2. System key stored in keystore as `xpack.watcher.encryption_key` (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/WatcherField.java`, line 18)
3. Encryption key derived: `SHA-256(systemKey)` truncated to 128 bits (lines 205-221)
4. Random IV generated per encryption (line 162-163)
5. IV prepended to ciphertext, base64-encoded, prefixed with `::es_encrypted::` (lines 124-127)

### Storage Model: Encrypted Secrets in Index Documents

The **critical** pattern is how Watcher stores encrypted passwords in the `.watches` index:

**WatcherXContentParser** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/support/xcontent/WatcherXContentParser.java`, lines 34-61):

```java
public static Secret secretOrNull(XContentParser parser) throws IOException {
    String text = parser.textOrNull();
    // ... check if already encrypted ...
    if (parser instanceof WatcherXContentParser watcherParser) {
        if (watcherParser.cryptoService != null) {
            char[] encryptedChars = watcherParser.cryptoService.encrypt(chars);
            Arrays.fill(chars, '\0'); // Clear plaintext
            return new Secret(encryptedChars);
        }
    }
    return new Secret(chars);
}
```

**Secret** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/common/secret/Secret.java`): Wraps encrypted chars, decrypts on demand:

```java
public char[] text(CryptoService service) {
    if (service == null) return text;
    return service.decrypt(text);
}
```

**BasicAuth** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/common/http/BasicAuth.java`, lines 62-69): On serialization, encrypted passwords are stored as-is; unencrypted ones are redacted:

```java
if (WatcherParams.hideSecrets(params) && password.value().startsWith(CryptoService.ENCRYPTED_TEXT_PREFIX) == false) {
    builder.field(Field.PASSWORD.getPreferredName(), WatcherXContentParser.REDACTED_PASSWORD);
} else {
    builder.field(Field.PASSWORD.getPreferredName(), password.value());
}
```

**Integration test proof** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/watcher/src/internalClusterTest/java/org/elasticsearch/xpack/watcher/test/integration/HttpSecretsIntegrationTests.java`, lines 109-126):
- When `encrypt_sensitive_data=true`, password is stored as `::es_encrypted::...` in the `.watches` index
- When `false`, password stored as plaintext
- CryptoService can decrypt the stored value back to the original password

**Bootstrap Check** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/EncryptSensitiveDataBootstrapCheck.java`):
- If `xpack.watcher.encrypt_sensitive_data=true`, requires `xpack.watcher.encryption_key` in keystore
- Opt-in behavior (default is `false`)

### Important Design Weakness in Watcher's CryptoService

The code itself contains a TODO acknowledging the weakness (line 50-52 of CryptoService.java):
```java
// TODO: with better support in Java 8, we should consider moving to use AES GCM as it
// also provides authentication of the encrypted data, which is something that we are
// missing here.
```

AES-CTR provides **no authentication** -- ciphertexts can be tampered with without detection. A new credential encryption service MUST use AES-GCM instead.

---

## 5. Java Cryptography Infrastructure in the Codebase

### Files Using JCE/JCA

| File | Algorithms Used |
|------|----------------|
| `KeyStoreWrapper.java` | AES-256-GCM, PBKDF2WithHmacSHA512 |
| `TokenService.java` | AES/GCM/NoPadding, PBKDF2withHMACSHA512 |
| `CryptoService.java` | AES/CTR/NoPadding, HmacSHA512 |
| `CryptUtils.java` | AES (ECB mode!), PBKDF2WithHmacSHA512, RSA |
| `Hasher.java` | bcrypt, PBKDF2WithHmacSHA512, SHA-256 |
| `ConsistentSettingsService.java` | PBKDF2WithHmacSHA512 |
| `PemUtils.java` | AES, DESede, DES (PEM key decryption) |

### FIPS 140-2 Considerations

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/XPackSettings.java` (line 185-189)

```java
public static final Setting<Boolean> FIPS_MODE_ENABLED = Setting.boolSetting(
    "xpack.security.fips_mode.enabled", false, Property.NodeScope
);
```

- FIPS mode is opt-in
- When enabled, Bouncy Castle FIPS provider is used
- Salt must be >= 128 bits for FIPS compliance (see `CryptUtils.java` line 33)
- Any new encryption service must work with both standard JCE and BC-FIPS

---

## 6. Master Key Management

### Existing Precedents

There are **two** master key patterns in the codebase:

#### Pattern A: Node-local keystore key (Watcher)

- System key generated offline via `SystemKeyTool`
- Stored in `elasticsearch.keystore` on each node (`xpack.watcher.encryption_key`)
- **Same key must be manually deployed to every node**
- Used for: encrypting secrets at rest in the `.watches` index
- Pro: Simple, key never leaves the node
- Con: Manual key distribution, no rotation mechanism

#### Pattern B: Cluster state distributed key (TokenService)

- Key generated randomly on node startup
- Distributed to all nodes via `TokenMetadata` in cluster state
- `isPrivate() = true` prevents key from being exposed via REST API
- Multiple keys cached for rotation (old keys kept for decryption)
- Pro: Automatic distribution, key rotation support
- Con: Key in cluster state (encrypted in transit but not at rest in cluster state persistence)

### ConsistentSettingsService Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/ConsistentSettingsService.java`

This is NOT encryption but a **verification** mechanism:
- Master node publishes PBKDF2 hashes of keystore settings to cluster state (`hashesOfConsistentSettings` in Metadata)
- Other nodes verify their local settings match the master's hashes
- Prevents configuration drift across nodes
- Could be used to verify that all nodes have the same encryption key

---

## 7. What Exactly Needs to Be Built

### Required Components

#### 7.1 CredentialEncryptionService

A new service modeled on `CryptoService` but using modern AES-GCM:

**Reusable patterns from existing code:**

| Component | Source Pattern | What to Keep | What to Change |
|-----------|---------------|--------------|----------------|
| Cipher setup | `KeyStoreWrapper.createCipher()` (line 317) | AES-256-GCM, 128-bit tag, 12-byte IV | Key source: use cluster-distributed key instead of password-derived |
| Key distribution | `TokenService.installTokenMetadata()` (line 2460) | Cluster state Custom, `isPrivate()=true` | Store encryption master key instead of token keys |
| Key rotation | `TokenService.refreshMetadata()` (line 2380) | Keep old keys for decryption, track current by hash | Same pattern |
| Encrypted value format | `CryptoService.encrypt()` (line 123) | Prefix marker, base64 encoding | Use `::es_datasource_encrypted::` prefix, include key version |
| Secret type | `Secret` class (watcher) | Wrap encrypted text, decrypt-on-demand | New `EncryptedCredential` type that carries key version |
| Bootstrap check | `EncryptSensitiveDataBootstrapCheck` (watcher) | Fail startup if encryption required but no key | Similar check for data source encryption |

**Recommended algorithm (matching KeyStoreWrapper/TokenService best practices):**

```
Algorithm:    AES/GCM/NoPadding
Key size:     256 bits
IV:           12 bytes (random per encryption)
GCM tag:      128 bits
Key source:   Random, distributed via cluster state (Pattern B)
KDF:          Not needed if using raw random key (no password)
```

#### 7.2 EncryptedValue Type

A serializable type to store in data source configuration documents:

```java
public record EncryptedValue(
    int keyVersion,      // which key was used (for rotation)
    byte[] iv,           // 12 bytes
    byte[] ciphertext,   // encrypted data + GCM tag
    byte[] aad           // optional: data source name for binding
) implements Writeable, ToXContentObject { }
```

**Serialization precedent**: The `superseding.encrypted_tokens` / `superseding.encryption_iv` / `superseding.encryption_salt` fields in TokenService (lines 1299-1302) show encrypted blobs stored as base64 strings in index documents.

#### 7.3 Key Rotation Mechanism

Based on `TokenService` pattern:

1. **Master node generates key**: Random 256-bit key on first startup
2. **Distribute via cluster state Custom**: Like `TokenMetadata`, with `isPrivate()=true`
3. **Key versioning**: Each key has a version number and timestamp
4. **Rotation trigger**: Admin API to rotate key (re-encrypts all stored credentials)
5. **Backward compatibility**: Keep old keys for decrypting existing values

**Key difference from TokenService**: Token keys are ephemeral (tokens expire). Data source credentials are long-lived, so key rotation must re-encrypt existing values.

#### 7.4 Where Credentials Currently Live (the Gap)

Currently, external data source credentials flow through `SourceMetadata.config()` (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceMetadata.java`, lines 99-118):

```java
default Map<String, Object> config() {
    return Map.of();
}
```

With documented keys including `"access_key"` and `"secret_key"` (lines 107-108). These are currently passed as **plaintext** through the system. There is no CRUD API yet for persisting data source configurations (confirmed by searching for `createDataSource`/`CRUD` -- only `VIEWS_CRUD_AS_INDEX_ACTIONS` capability exists for Views, not data sources).

### Implementation Priority

1. **CredentialEncryptionService** -- core service, can reuse `KeyStoreWrapper.createCipher()` almost verbatim
2. **EncryptedValue** type -- simple record, serialization follows `TokenService` superseding pattern
3. **Cluster state key distribution** -- follows `TokenMetadata` pattern exactly
4. **CRUD API integration** -- encrypt on write, decrypt on read (follows `WatcherXContentParser.secretOrNull()` pattern)
5. **Key rotation API** -- admin endpoint to trigger re-encryption
6. **ConsistentSettingsService check** -- verify all nodes have same master key

### Estimated Complexity

- The cryptographic primitives are all proven in the codebase (AES-GCM, PBKDF2, SecureRandom)
- The cluster state distribution pattern is proven (`TokenMetadata`)
- The encrypt-on-write/decrypt-on-read pattern is proven (Watcher `CryptoService` + `WatcherXContentParser`)
- The main new work is: wiring these together for data source configs, building the rotation API, and integration with the not-yet-built CRUD API for data source definitions
