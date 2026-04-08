# Secrets Storage in Elasticsearch: Deep Dive

## Executive Summary

Elasticsearch has a **systemic plaintext secrets problem**. Inference endpoints store API keys as plaintext in `.secrets-inference`. Connector secrets are plaintext in `.connector-secrets`. ESQL datasource plugins have zero credential storage — secrets travel as plaintext strings in query text. Meanwhile, partial infrastructure exists: `ClusterSecrets` can distribute secrets across nodes (but requires orchestrator provisioning), `RotatableSecret` handles key rotation, and `TokenService` demonstrates production AES-128-GCM encryption with PBKDF2 key derivation (our service would upgrade to 256-bit).

The core challenge is **encryption key bootstrap**: how to get a shared master key onto all nodes. `keystore.seed` is per-node and unique — it cannot be used as a shared key. Three viable sources exist: orchestrator-provisioned (`ClusterSecrets`, serverless), admin-provisioned (keystore setting, stateful), and **auto-generated** (master generates key, stores as `Metadata.ClusterCustom` with `GATEWAY`-only context — persists across restarts via the Lucene gateway, never exposed in API/snapshots). The recommended approach: build a shared **`SecretsService`** that encrypts credentials with AES-256-GCM, stores them in a system index, and supports all three key sources with a priority order.

---

## 1. The Problem: Three Plaintext Stores

### 1.1 Inference Endpoints — Plaintext in `.secrets-inference`

Inference stores credentials in a dedicated system index with **no encryption**:

- **Index**: `.secrets-inference` (`InferenceSecretsIndex.java:25`)
- **Mapping**: `secret_settings` has `"dynamic": false` — fields aren't indexed but are stored in `_source` as plaintext
- **All 6 secret settings classes** (5 with actual secrets + `EmptySecretSettings`) call `SecureString.toString()` in `toXContent()`:
  - `DefaultSecretSettings.java:97` — `builder.field(API_KEY, apiKey.toString())`
  - `AzureOpenAiSecretSettings.java:95` — `builder.field(API_KEY, apiKey.toString())`
  - `GoogleVertexAiSecretSettings.java:77` — `builder.field(SERVICE_ACCOUNT_JSON, serviceAccountJson.toString())`
  - `AwsSecretSettings.java:98-99` — `builder.field(ACCESS_KEY_FIELD, accessKey.toString())`
  - `CustomSecretSettings.java:77` — `builder.field(entry.getKey(), entry.getValue().toString())`

**Impact**: Data directory access, snapshots, backups, and cluster exports all contain plaintext API keys.

### 1.2 Connector Secrets — Plaintext in `.connector-secrets`

- **Index**: `.connector-secrets` (`ConnectorSecretsIndexService.java`)
- **No encryption** — relies entirely on document-level security ACLs
- Same exposure as inference: data directory, snapshots, backups

### 1.3 ESQL External Sources — No Storage At All

- Credentials passed inline in query text: `EXTERNAL "s3://..." WITH { "access_key": "AKIA...", "secret_key": "..." }`
- `ExternalSourceResolver.paramsToConfigMap()` converts Expression literals to plain `Map<String, Object>`
- All cloud storage datasource plugins (S3, Azure, GCS) plus HTTP and Iceberg extract credentials as plain strings
- **Zero** `SecureSetting` or keystore usage in any datasource plugin
- Credentials visible in query text, audit logs, slow logs

---

## 2. Existing Encryption Patterns in Elasticsearch

### 2.1 Watcher CryptoService — AES/CTR (Legacy)

**File**: `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/crypto/CryptoService.java`

| Property | Value |
|----------|-------|
| Algorithm | AES/CTR/NoPadding (128-bit key) |
| Key derivation | SHA-256(master_key).truncate(128) — done once at startup |
| IV | Random 16 bytes per encryption |
| Storage format | `::es_encrypted::<base64(IV\|\|ciphertext)>` |
| Master key source | `xpack.watcher.encryption_key` secure file in keystore |
| Master key generation | `bin/elasticsearch-syskeygen` → 1024-bit HmacSHA512 |

**Known weaknesses** (acknowledged in code at lines 46-52):
- **No authentication tag** — CTR mode cannot detect tampering
- **128-bit keys** — acceptable but could be 256-bit
- **Manual key distribution** — admin must copy system key to every node's keystore
- Code comment: "with better support in Java 8, we should consider moving to use AES GCM"

### 2.2 TokenService — AES/GCM (Modern Standard)

**File**: `x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/authc/TokenService.java` (lines 158-250)

| Property | Value |
|----------|-------|
| Algorithm | AES/GCM/NoPadding (**128-bit** derived key) |
| Key derivation | PBKDF2WithHmacSHA512, **100,000 iterations** |
| Passphrase | 64 random bytes (512 bits), base64-encoded — input to PBKDF2 |
| Derived key | 128-bit (`PBEKeySpec(..., 128)` at line 2224) |
| Salt | 32 random bytes per derivation |
| IV | 12 random bytes per encryption |
| Authentication | Yes — GCM provides authenticated encryption |
| Standard | NIST Digital Identity Guidelines, OWASP Password Storage |

**What to follow**: The encryption pattern (AES/GCM, PBKDF2 100k iterations, per-operation salt+IV) and the `KeyAndCache` pattern (caches derived keys to avoid re-running PBKDF2 on every operation). This is production-proven, NIST-compliant, and OWASP-recommended. **For our SecretsService, use 256-bit derived keys** (change the `PBEKeySpec` length parameter from 128 to 256) — this is a one-line change and provides a stronger security margin.

**What NOT to follow**: The key management OR the key size. TokenService generates a random 64-byte passphrase at startup (`generateTokenKey()`, line 2404) and derives a **128-bit** AES key via PBKDF2 (`PBEKeySpec(..., 128)` at line 2224). It distributes the passphrase via `TokenMetadata`, a `ClusterState.Custom` with `isPrivate() = true`. This is an **ephemeral** custom — NOT persisted to the Lucene gateway. The key vanishes on full cluster restart. This works for TokenService because tokens are short-lived (20 min default) — users just re-authenticate after restart. **We cannot use this pattern** — our encrypted credentials must survive restarts. A lost encryption key means permanently undecryptable credentials. And we should use 256-bit derived keys for a stronger security margin.

**Key distribution mechanism in TokenService**:
1. First master publishes its key to cluster state via `installTokenMetadata()` (line 2460)
2. All nodes pick up the key via `clusterChanged()` → `refreshMetadata()`
3. `toXContentChunked()` returns empty — never rendered to clients
4. Key supports rotation via `KeyAndTimestamp` list with active key hash

This proves that broadcasting a key via cluster state works. Our approach uses the same broadcast mechanism but with `Metadata.ClusterCustom` (GATEWAY context) instead of `ClusterState.Custom` for persistence across restarts.

### 2.3 License CryptUtils — AES/PBKDF2

**File**: `x-pack/plugin/core/src/main/java/org/elasticsearch/license/CryptUtils.java`

- AES with PBKDF2WithHmacSHA512, 10,000 iterations, 128-bit key
- Hardcoded salt (not ideal)
- Used for license encryption only

### Comparison

| Pattern | Cipher | Auth | KDF | Key Size | Key Persistence | Production Use |
|---------|--------|------|-----|----------|----------------|---------------|
| Watcher | AES/CTR | No | SHA-256 once | 128-bit | Admin keystore (persistent) | `.watches` |
| **TokenService** | **AES/GCM** | **Yes** | **PBKDF2 100k** | **128-bit** | **ClusterState.Custom (ephemeral)** | **`.security-tokens`** |
| License | AES | No | PBKDF2 10k | 128-bit | Hardcoded salt (persistent) | License data |

**For our SecretsService**: Use TokenService's encryption pattern (AES/GCM, PBKDF2 100k) but upgrade to **256-bit** derived keys and use `Metadata.ClusterCustom` with `GATEWAY` context for key persistence instead of ephemeral `ClusterState.Custom`.

---

## 3. Existing Key Distribution Infrastructure

### 3.1 `keystore.seed` — Per-Node Bootstrap Secret (NOT Shared)

**File**: `KeyStoreWrapper.java:107,196-214`

Every Elasticsearch keystore auto-generates a 20-character random seed (`keystore.seed`) on creation. **Critically, each node generates its own unique seed independently.**

- `KeyStoreWrapper.create()` (line 196) calls `addBootstrapSeed()` which uses `SecureRandom` to generate 20 random characters
- The seed is persisted to the `elasticsearch.keystore` file on disk and is stable for the life of the node
- On first boot: no keystore → `create()` generates new seed → `save()` writes to disk
- On subsequent boots: `load()` + `decrypt()` reads the same seed from disk
- On upgrade from old format: seed is added once via `upgrade()` (line 302)
- Is registered in `ClusterSettings.java:552` but does NOT have `Property.Consistent` — each node's seed is independent

**This means `keystore.seed` CANNOT be used as a shared encryption key.** Every node has a different seed. Deriving an encryption key from `keystore.seed` would produce a different key on each node — encrypting on one node and decrypting on another would fail.

`keystore.seed` is useful as a **per-node** unique identifier or entropy source, but not as a cluster-wide encryption key.

### 3.2 `ClusterSecrets` / `SecureClusterStateSettings` — Cluster-Replicated Keys

**Files**:
- `server/src/main/java/org/elasticsearch/common/settings/ClusterSecrets.java`
- `server/src/main/java/org/elasticsearch/common/settings/SecureClusterStateSettings.java`

A mechanism to store secrets in cluster state itself:
- Initialized from reserved state file on disk
- Broadcast by master node to all data nodes via transport layer
- Marked as "private custom" — never exposed to clients
- `SecureClusterStateSettings` explicitly states it "does not provide any encryption" — it's a distribution mechanism, not an encryption mechanism

### 3.3 `ProjectSecrets` — Multi-Tenant Secret Isolation

**File**: `server/src/main/java/org/elasticsearch/common/settings/ProjectSecrets.java`

For serverless mode with multiple projects:
- Secrets stored per-project in `Metadata.ProjectCustom`
- Master watches project secret files and propagates changes via transport
- Not persisted to saved cluster state (master always has source of truth on disk)

### 3.4 `RotatableSecret` — Key Rotation

**File**: `server/src/main/java/org/elasticsearch/common/settings/RotatableSecret.java`

Manages current and prior (grace period) versions of secrets:
- Thread-safe with optimistic locking
- Allows key rotation without downtime
- Used by security token service for JWT key rotation

### 3.5 `ConsistentSettingsService` — Cross-Node Verification

**File**: `server/src/main/java/org/elasticsearch/common/settings/ConsistentSettingsService.java`

- Master computes PBKDF2WithHmacSHA512 hashes (with random salt, 5000 iterations) of secure settings marked with `Property.Consistent`
- Hashes published to cluster state metadata
- All nodes verify their local values match the published hashes
- `keystore.seed` is **NOT** marked as consistent — each node has its own independent seed. Only settings explicitly created with `Property.Consistent` are verified.

### 3.6 Persisted Cluster State Metadata (`Metadata.ClusterCustom`)

Elasticsearch has two layers of cluster state with different persistence behavior:

1. **`ClusterState.Custom`** (what `ClusterSecrets` uses) — broadcast via transport, held in memory only, **NOT persisted to disk**. Vanishes on restart. Master must always reload from its own source file.

2. **`Metadata.ClusterCustom`** — serialized to XContent and **persisted to a Lucene index** on every master-eligible and data-holding node's data directory by `PersistedClusterStateService` (`GatewayMetaState.java:147`: `isMasterNode || canContainData`). Masters write synchronously (`LucenePersistedState`); data nodes write asynchronously (`AsyncPersistedState`). Survives restarts, master elections, and rolling upgrades.

Each `Metadata.ClusterCustom` declares a `context()` that controls where it appears:

| Context | Meaning |
|---------|---------|
| `GATEWAY` | Persisted to disk — survives restarts |
| `API` | Returned in `GET _cluster/state` responses |
| `SNAPSHOT` | Included in snapshot global state |

**Precedent: `LicensesMetadata`** uses `context() = EnumSet.of(GATEWAY)` — persisted to disk but NOT visible in API responses or snapshots. This is the exact pattern for sensitive data that must survive restarts but should never be exposed to clients.

This means a `Metadata.ClusterCustom` with `GATEWAY`-only context can:
- Be generated by the elected master and published as a cluster state update
- Be persisted on every master-eligible AND data-holding node's data directory
- Survive restarts, elections, rolling upgrades
- Never appear in REST API responses
- Never appear in snapshots

### 3.6.1 Downsides of `Metadata.ClusterCustom` for Key Storage

| Downside | Severity | Notes |
|----------|----------|-------|
| **Key unencrypted on disk** | Medium | Lucene gateway files in `data/` on every master-eligible AND data-holding node contain the key in cleartext. Same security boundary as the node keystore — filesystem access = key access. Only coordinating-only nodes (no master, no data roles) are excluded. |
| **Key in memory on every node** | Medium | `Metadata` is deserialized on ALL nodes (not just master-eligible). The key lives in the JVM heap of every node in the cluster, including coordinating-only nodes that may not need it. |
| **No per-node access control** | Low | Every node receives the full `Metadata`. No way to restrict which nodes see the key. In mixed clusters (dedicated masters, data nodes, coordinating-only), all nodes get it. |
| **Filesystem backups capture it** | Medium | Filesystem-level backups of ANY master-eligible or data-holding node's data directory include the Lucene gateway files containing the key. (Elasticsearch snapshots do NOT — `GATEWAY`-only context excludes `SNAPSHOT`.) |
| **Cluster state size** | Negligible | A 256-bit key is 32 bytes. Cluster state is broadcast atomically on every update, so it should stay small — but 32 bytes is trivially small. |
| **Internal debug exposure** | Low | Debug logging on `PersistedClusterStateService` or direct Lucene index inspection reveals the key. Not a realistic production threat. |

### 3.6.2 Threat Model: Why Encryption Still Helps

The auto-bootstrap key and the encrypted credentials are stored in **different places**:

| Store | Contains | Who Has Access |
|-------|----------|---------------|
| Lucene gateway (`data/`) | Encryption key (auto-bootstrap only) | Filesystem access to ANY master-eligible or data-holding node |
| `.secrets` system index | Encrypted credentials | Shard replicas on data nodes, snapshots, backups |

**What encryption defends against:**

- **Snapshot exfiltration**: An Elasticsearch snapshot contains the `.secrets` index but NOT the `GATEWAY`-only metadata key. Credentials are opaque without the key.
- **Backup of coordinating-only nodes**: Coordinating-only nodes (no master, no data roles) hold neither gateway files nor `.secrets` shards. However, this is a rare topology. **Data nodes DO persist the gateway** (via `AsyncPersistedState`), so a filesystem backup of any data node yields both the key (in gateway files) and potentially the `.secrets` shards — no separation in the common case.
- **System index browsing**: A user or process with read access to `.secrets` sees ciphertext, not plaintext API keys.
- **Audit/compliance**: Credentials are encrypted at rest in the index, satisfying "encryption at rest" requirements even when the underlying filesystem is not encrypted.

**What encryption does NOT defend against:**

- **Full node compromise**: An attacker with filesystem access to any master-eligible or data-holding node can extract the key from the gateway and decrypt credentials from the system index. On data nodes, both the key (gateway) and the `.secrets` shards may be on the same filesystem.
- **JVM memory dump**: The key is in heap on every node. A heap dump exposes it.
- **Admin with cluster access**: A cluster admin who can read both the gateway files and the system index can decrypt everything.

**Bottom line**: The auto-bootstrap path provides **separation of concerns** — compromising any single store (snapshot, data node backup, system index read access) does not yield plaintext credentials. It does not protect against full infrastructure compromise, which requires the explicit key provisioning paths (keystore or orchestrator) where the key never touches cluster state at all.

### 3.7 The Key Bootstrap Problem

The fundamental challenge: how does a shared encryption key get onto all nodes?

| Mechanism | How Key Gets There | Persists Restarts? | Stateful Self-Managed | Serverless/Cloud |
|-----------|-------------------|--------------------|----------------------|-----------------|
| `keystore.seed` | Auto-generated per node | Yes (per-node file) | **Unique per node — cannot share** | **Unique per node — cannot share** |
| Node keystore setting | Admin runs `elasticsearch-keystore add` on each node | Yes (per-node file) | Works (same as Watcher pattern) | **Doesn't work** (no node filesystem access) |
| `ClusterSecrets` (`ClusterState.Custom`) | Orchestrator provisions reserved state file, master distributes | **No** — master reloads from disk | Requires operator/automation | **Works** (orchestrator provisions) |
| `Metadata.ClusterCustom` (GATEWAY) | Master generates, persisted by gateway | **Yes** — Lucene on each master + data node | Works (zero-config) | Works (zero-config) |

**No single mechanism works everywhere without trade-offs.** The practical approach is to support multiple key sources with a priority order:

1. **Serverless/Cloud**: Orchestrator provisions the key via reserved state → `ClusterSecrets` distributes. This is the standard pattern. Zero user action.
2. **Stateful self-managed (explicit)**: Admin provisions via keystore (`xpack.secrets.encryption_key`), same as Watcher's `xpack.watcher.encryption_key`. Requires same key on all nodes. `ConsistentSettingsService` verifies consistency.
3. **Auto-bootstrap (zero-config fallback)**: If neither is configured, the elected master generates a 256-bit random key and stores it as a `Metadata.ClusterCustom` with `GATEWAY`-only context. This persists across restarts (Lucene gateway on every master-eligible and data-holding node), is never exposed in API responses or snapshots, and provides a zero-config path. A warning is logged recommending explicit provisioning for production. **Important**: the key ends up on disk on every data node (not just masters), so the auto-bootstrap path provides weaker separation than the orchestrator or keystore paths.

The auto-bootstrap path uses the same persistence mechanism as `LicensesMetadata` — proven, battle-tested, and already part of every Elasticsearch cluster's lifecycle.

### Summary: Partial Plumbing Exists

| Component | What It Does | Cross-SKU? | Limitation |
|-----------|-------------|------------|-----------|
| `keystore.seed` | Per-node unique secret | Yes | **Not shared — cannot use for cluster encryption** |
| `ClusterSecrets` | Distribute secrets via cluster state (ephemeral) | Yes | Requires orchestrator; NOT persisted across restarts |
| `Metadata.ClusterCustom` | Persisted cluster state metadata (Lucene gateway) | Yes | Unencrypted on disk on ALL master + data nodes |
| `ProjectSecrets` | Per-project isolation | Yes (serverless) | Serverless only |
| `RotatableSecret` | Key rotation with grace period | Yes | Needs a key source to rotate |
| `ConsistentSettingsService` | Verify all nodes have same secrets | Yes | Verification only, not distribution |
| `TokenService` | AES-128-GCM encryption pattern (upgrade to 256 for our service) | Yes | Proven pattern, reusable |

The **distribution and rotation** infrastructure exists. The **encryption pattern** is proven. The **persistence mechanism** exists (`Metadata.ClusterCustom` with `GATEWAY` context). What's missing is: a **reusable encryption service** that composes these pieces with a multi-strategy key bootstrap.

---

## 4. Storage: Cluster State vs System Index

### 4.1 Trade-offs

| Factor | Cluster State | System Index |
|--------|--------------|-------------|
| Replication | Master broadcasts to ALL nodes | Shard-based (1 primary + N replicas) |
| Memory | In memory on EVERY node | Only on data nodes holding shards |
| Latency | Synchronous — all nodes see immediately | Eventually consistent (1s refresh) |
| Scale | Should be small (impacts cluster health) | Can be large (standard index) |
| Access pattern | Always in memory | On-demand (search/get) |
| Audit | Cluster-state level (coarse) | Per-document (fine-grained) |
| Serverless | Broadcast to all nodes (wasteful) | Shard-local (natural fit) |

### 4.2 For Encrypted Credentials Specifically

**Cluster state is a poor fit:**
- Encrypted credentials in memory on ALL nodes = large exposure surface
- Any node compromise = all credentials exposed (even encrypted, the key is also in memory)
- No fine-grained access control (all-or-nothing)
- Cluster state size is a health concern — 100 datasources with credentials adds up

**System index is the right choice:**
- Credentials fetched on-demand only when a query needs them
- Per-document security (fine-grained access control)
- Natural audit trail (which user/query accessed which credential)
- Shard-based replication (not broadcast to every node)
- Aligns with serverless architecture (stateless compute nodes query for credentials)

### 4.3 Precedent

| Feature | Where Stored | Has Secrets? |
|---------|-------------|-------------|
| Views | Cluster state | No |
| Ingest pipelines | Cluster state | No |
| Repository metadata | Cluster state | No (creds in keystore) |
| ML configs | System index (`.ml-config`) | No |
| Transform configs | System index (`.transform-internal-*`) | No |
| Watcher | System index (`.watches`) | Yes (encrypted with CryptoService) |
| Inference | System index (`.secrets-inference`) | Yes (**plaintext**) |
| Connectors | System index (`.connector-secrets`) | Yes (**plaintext**) |

**Pattern**: Everything with secrets uses system indices. The problem is those secrets are plaintext.

### 4.4 Recommended Approach

**Split storage:**
- **Cluster state** (`DatasourceMetadata`): Lightweight registry — name, type, status. Used for fast lookups and schema resolution. No credentials. Following the Views pattern.
- **System index** (`.esql-datasource-secrets`): Encrypted credentials. Fetched on-demand at query execution time only. Following the inference pattern, but **with encryption**.

---

## 5. Options

### Option A: Shared SecretsService (Recommended)

Build a reusable `SecretsService` that any Elasticsearch feature can use to store encrypted credentials in a system index. Fix the problem once, for everyone.

**Architecture:**

```
                    ┌─────────────────────────────────┐
                    │         SecretsService           │
                    │  encrypt(name, Map<String,       │
                    │    SecureString>) → void          │
                    │  decrypt(name) → Map<String,     │
                    │    SecureString>                  │
                    │  delete(name) → void              │
                    │  rotate() → void                 │
                    └───────────┬─────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
              Encryption Layer        Storage Layer
              (AES-256-GCM,           (System Index:
               PBKDF2 100k,           `.secrets`)
               per-secret IV+salt)
                    │
              Key Source (priority order):
              1. ClusterSecrets (serverless — orchestrator provisions)
              2. Keystore setting (stateful — admin provisions)
              3. Metadata.ClusterCustom (zero-config — master auto-generates,
                 GATEWAY-only context, persists across restarts,
                 never in API/snapshots)
```

**How it works:**

1. **Key bootstrap** (multi-strategy, priority order):
   - **Serverless/Cloud**: Orchestrator provisions master key in reserved state file → `ClusterSecrets` distributes to all nodes. Zero user action.
   - **Stateful self-managed (explicit)**: Admin runs `elasticsearch-keystore add xpack.secrets.encryption_key` on each node (same pattern as Watcher's `xpack.watcher.encryption_key`). `ConsistentSettingsService` verifies all nodes have the same key.
   - **Auto-bootstrap (zero-config)**: If neither is configured, the elected master generates a 256-bit random key and publishes it as a `Metadata.ClusterCustom` with `GATEWAY`-only context. This persists to the Lucene gateway store on every master-eligible and data-holding node — survives restarts, master elections, and rolling upgrades. Never appears in API responses or snapshots (no `API` or `SNAPSHOT` context). Same persistence mechanism as `LicensesMetadata`. A warning is logged recommending explicit provisioning for production use. **Note**: key is on disk on data nodes too (not just masters), so no separation from `.secrets` shards on the same node.
   - Key wrapped in `RotatableSecret` for rotation support.

2. **Encrypt**: `SecretsService.encrypt("my-s3-creds", {"access_key": "AKIA...", "secret_key": "wJal..."})`:
   - Generate random 32-byte salt + 12-byte IV
   - Derive per-secret key via PBKDF2(master_key, salt, 100k iterations)
   - Encrypt with AES-256-GCM
   - Store `{salt, iv, ciphertext, key_version}` in `.secrets` system index

3. **Decrypt**: `SecretsService.decrypt("my-s3-creds")`:
   - Fetch document from `.secrets` by ID
   - Load master key from whichever source was configured (ClusterSecrets, keystore, or cluster state)
   - Re-derive key from master key + stored salt
   - Decrypt with AES-256-GCM (authenticated — detects tampering)
   - Return `Map<String, SecureString>` (zeroed after use)

4. **Rotation**: `SecretsService.rotate()`:
   - Generate new master key
   - Store in `RotatableSecret` (current + prior with grace period)
   - Re-encrypt all secrets with new key (background task)
   - Retire old key after grace period

**Who uses it:**
- **ESQL datasources**: `EXTERNAL ... WITH (credentials_ref="my-s3-creds")` → resolve from SecretsService
- **Inference endpoints**: Replace plaintext `toXContent()` with `SecretsService.encrypt()` in `ModelRegistry`
- **Connectors**: Replace plaintext `.connector-secrets` with SecretsService
- **Future**: Any feature that needs encrypted credential storage

**Estimate**: ~4-5 weeks AI-assisted for the core service + ESQL integration (includes multi-strategy key bootstrap). Retrofitting inference and connectors is additional ~1-2 weeks each.

**Pros:**
- Solves the problem for everyone, not just ESQL
- Modern encryption (AES-256-GCM, authenticated, PBKDF2)
- Multi-strategy key bootstrap works across all SKUs
- Key rotation built in
- System index = on-demand access, fine-grained audit
- Auto-bootstrap fallback means zero-config path exists

**Cons:**
- Larger scope than ESQL-only solution
- Requires coordination with inference and connector teams
- Auto-bootstrap stores key in Lucene gateway files on ALL master-eligible and data-holding nodes (unencrypted on disk — key and `.secrets` shards co-located on data nodes, no separation)
- Multi-strategy key source adds complexity (three code paths to maintain)

**Threat model (per key source):**

| Attack Vector | Orchestrator-provisioned | Admin keystore | Auto-bootstrap (Metadata.ClusterCustom) |
|---------------|------------------------|----------------|----------------------------------------|
| **Snapshot exfiltration** | **Safe** — key not in snapshot, creds encrypted | **Safe** — key not in snapshot, creds encrypted | **Safe** — `GATEWAY`-only excludes `SNAPSHOT`, creds encrypted |
| **Data node filesystem backup** | **Safe** — key in reserved state on master only | **Safe** — key in keystore file, not in data dir | **Compromised** — data nodes also persist GATEWAY-context metadata via `AsyncPersistedState`. Key is in gateway files on every data node alongside `.secrets` shards. No separation. |
| **Master node filesystem access** | **Compromised** — reserved state file has key | **Compromised** — keystore file has key | **Compromised** — Lucene gateway has key |
| **JVM heap dump (any node)** | **Compromised** — key in memory on all nodes (ClusterSecrets distributes) | **Compromised** — key in memory on all nodes (loaded at startup) | **Compromised** — key in memory on all nodes (Metadata broadcast) |
| **System index read access** | **Safe** — ciphertext only | **Safe** — ciphertext only | **Safe** — ciphertext only |
| **`GET _cluster/state` API** | **Safe** — ClusterSecrets is `isPrivate()` | **Safe** — keystore is node-local, not in cluster state | **Safe** — `GATEWAY`-only, no `API` context |
| **Elasticsearch snapshot restore to new cluster** | **Safe** — new cluster lacks key | **Safe** — new cluster lacks key | **Safe** — new cluster lacks key |
| **Full infrastructure compromise** | Compromised | Compromised | Compromised |

**Key insight**: All three sources have the same vulnerability to full-node compromise (filesystem + heap). The differences are in **partial compromise** scenarios. The orchestrator and keystore paths are marginally stronger because the key is confined to a single well-known file rather than spread across the Lucene gateway. The auto-bootstrap path trades this for zero-config convenience.

### Option B: ESQL-Only Encryption in System Index

Same encryption approach as Option A, but scoped to ESQL only. Don't build a shared service — just encrypt ESQL datasource credentials.

**Architecture:**
- New `EsqlCryptoService` (modeled on TokenService's AES-GCM)
- Store encrypted credentials in `.esql-datasource-secrets` system index
- Key from dedicated `esql.encryption_key` keystore setting (admin-provisioned on each node)

**Estimate**: ~2-3 weeks AI-assisted.

**Pros:**
- Smaller scope, faster to ship
- No cross-team coordination needed
- Self-contained in ESQL plugin

**Cons:**
- Doesn't fix inference or connector plaintext problem
- Another one-off crypto implementation (Watcher, TokenService, now ESQL — pattern fragmentation)
- If a shared service is built later, this becomes throwaway work

**Threat model:**

| Attack Vector | Result |
|---------------|--------|
| **Snapshot exfiltration** | **Safe** — key not in snapshot, creds encrypted |
| **Data node filesystem backup** | **Safe** — key is in keystore, not in data dir |
| **Master node filesystem access** | **Compromised** — keystore file has key |
| **JVM heap dump** | **Compromised** — key loaded into memory at startup |
| **System index read access** | **Safe** — ciphertext only |
| **Serverless deployment** | **Broken** — no node filesystem access to provision keystore |
| **Full infrastructure compromise** | Compromised |

Same security profile as Option A's admin-keystore path, but only protects ESQL credentials. Inference and connector secrets remain plaintext. **Does not work on serverless** without adding the orchestrator or auto-bootstrap key paths (at which point you've built most of Option A anyway).

### Option C: Keystore References Only (No Encryption Service)

Don't encrypt anything. Let users pre-configure credentials in the node keystore via CLI, and reference them by name in queries.

**Usage:**
```bash
bin/elasticsearch-keystore add esql.datasource.my_s3.access_key
bin/elasticsearch-keystore add esql.datasource.my_s3.secret_key
```
```
EXTERNAL "s3://bucket/file.parquet" WITH (credentials_ref="my_s3")
```

**Estimate**: ~1-1.5 weeks AI-assisted.

**Pros:**
- Simplest implementation
- Reuses battle-tested keystore infrastructure
- Secrets never leave the node filesystem

**Cons:**
- **Doesn't work on serverless/managed deployments** (users can't access node filesystem)
- Requires node restart to add/change credentials
- No API for credential management
- Operational burden (configure every node manually)
- Doesn't address the broader plaintext problem

**Threat model:**

| Attack Vector | Result |
|---------------|--------|
| **Snapshot exfiltration** | **Safe** — credentials never in any index |
| **Data node filesystem backup** | **Safe** — keystore is node-local, encrypted with PBKDF2 |
| **Master node filesystem access** | **Compromised** — keystore file can be brute-forced (PBKDF2 with password, or passwordless = trivially extractable) |
| **JVM heap dump** | **Compromised** — credentials loaded into memory at startup |
| **System index read access** | **N/A** — no credential storage in indices |
| **Serverless deployment** | **Broken** — no node filesystem access |
| **Full infrastructure compromise** | Compromised |

**Strongest security profile** of all options — credentials never touch a system index, snapshot, or cluster state. The attack surface is limited to the node filesystem and JVM memory. However, the operational cost (manual per-node provisioning, node restart for changes) and serverless incompatibility make this impractical as the sole solution.

### Option D: Store Credentials in Cluster State (Like Views)

Store datasource definitions including encrypted credentials in cluster state custom metadata.

**Estimate**: ~2-3 weeks AI-assisted.

**Cons (significant):**
- Encrypted credentials in memory on every node
- Cluster state size concern
- Poor fit for serverless (broadcast to all nodes)
- Coarse audit granularity
- **Not recommended by the storage analysis**

**Threat model:**

| Attack Vector | Result |
|---------------|--------|
| **Snapshot exfiltration** | **Depends on context()** — if `SNAPSHOT` included, encrypted creds in snapshot. If `GATEWAY`-only, safe. |
| **Data node filesystem backup** | **Compromised** — every node persists cluster state; encrypted creds + encryption key both in Lucene gateway on every node |
| **Any node filesystem access** | **Compromised** — both the encrypted credentials AND the encryption key sit in the same Lucene gateway files on every node. No separation. |
| **JVM heap dump** | **Compromised** — all credentials (encrypted) + key in memory on every node |
| **`GET _cluster/state` API** | **Depends on context()** — if `API` included, encrypted creds visible. If `GATEWAY`-only, safe from API but still on disk everywhere. |
| **Full infrastructure compromise** | Compromised |

**Worst security profile.** The key and the encrypted credentials co-locate on every node — filesystem access to ANY node (not just master-eligible) yields both. No separation of concerns. The system index approach (Options A/B) is strictly better because the key and the ciphertext are in different stores.

### Comparative Threat Model Summary

| Attack Vector | Option A (Shared Service) | Option B (ESQL-only) | Option C (Keystore refs) | Option D (Cluster state) |
|---------------|--------------------------|---------------------|-------------------------|------------------------|
| Snapshot exfiltration | **Safe** | **Safe** | **Safe** | Depends on context |
| Data node backup | **Safe** (orchestrator) / **Compromised** (auto-bootstrap) | **Safe** | **Safe** | **Compromised** (all nodes) |
| Master filesystem | Compromised | Compromised | Compromised | Compromised |
| Any-node filesystem | Depends on key source | Compromised (keystore) | Compromised (keystore) | **Compromised** (key + creds co-located) |
| JVM heap dump | Compromised | Compromised | Compromised | Compromised |
| System index read | **Safe** (ciphertext) | **Safe** (ciphertext) | N/A | N/A |
| Serverless | **Works** (all 3 key sources) | **Broken** | **Broken** | Works |
| Separation of key & data | **Yes** (different stores) | **Yes** | **Yes** (strongest) | **No** (same store) |

**Option C has the strongest security** (credentials never leave the node) but is operationally impractical and serverless-incompatible. **Option A provides the best balance** — separation of key and data, works across all SKUs, and the auto-bootstrap path trades marginal security for zero-config convenience. **Option D is strictly dominated** by A and B on security.

---

## 6. Recommendation

**Option A: Shared SecretsService.**

The reasoning:

1. **The problem is cross-cutting.** Inference, connectors, and ESQL datasources all need encrypted credential storage. Building three separate solutions is more work than building one shared service.

2. **Partial infrastructure exists.** `ClusterSecrets` distributes keys (serverless). `RotatableSecret` handles rotation. `TokenService` proves the AES/GCM + PBKDF2 encryption pattern (128-bit; we upgrade to 256-bit). These can be composed into a service. The missing piece is the key bootstrap strategy and the reusable service itself.

3. **The encryption pattern is proven.** TokenService's AES/GCM with PBKDF2 (100k iterations) is production-tested, NIST-compliant, and OWASP-recommended. TokenService uses 128-bit derived keys; we would upgrade to 256-bit (a one-line change in `PBEKeySpec`). We're not inventing cryptography.

4. **System index is the right storage.** On-demand access (not broadcast to all nodes), fine-grained audit, natural fit for serverless, and precedent from Watcher/inference/connectors.

5. **The key bootstrap problem is solvable.** No single mechanism works everywhere, but a multi-strategy approach (orchestrator-provisioned for serverless, keystore-provisioned for stateful, auto-generated fallback) covers all SKUs. The auto-generated fallback is weaker than originally hoped — the gateway persists on data nodes too, so the key and `.secrets` shards co-locate on data nodes. It still prevents snapshot exfiltration and system-index browsing, but doesn't protect against data-node filesystem backup. For production, the orchestrator or keystore paths are recommended.

If scope is a concern, a reasonable phasing:

| Phase | Scope | Estimate |
|-------|-------|----------|
| 1 | Core SecretsService + ESQL integration | 4-5w |
| 2 | Retrofit inference endpoints | 1-2w |
| 3 | Retrofit connectors | 1w |
| 4 | Key rotation, credential CRUD API | 2w |

Phase 1 alone delivers value for ESQL datasources. Phases 2-3 fix the existing plaintext problems. Phase 4 adds operational maturity.

---

## 7. Key Design Questions

1. **Where does the master key live?** `keystore.seed` is per-node and unique — it cannot serve as a shared encryption key. Three viable sources: (a) `ClusterSecrets` / reserved state file (serverless — orchestrator provisions), (b) dedicated keystore setting `xpack.secrets.encryption_key` (stateful — admin provisions same key on all nodes), (c) auto-generated and stored as `Metadata.ClusterCustom` with `GATEWAY`-only context (zero-config fallback — persists across restarts, never in API/snapshots, but key ends up on all master + data nodes alongside `.secrets` shards). Recommendation: support all three with a priority order, but note that (c) provides convenience, not security separation.

2. **Per-secret keys or single key?** PBKDF2 with per-secret salt effectively gives per-secret keys derived from a single master. This is the TokenService pattern and the right approach.

3. **Who owns the `.secrets` index?** Should be a core server capability (like `.security`), not an x-pack plugin. This allows any feature to use it.

4. **GET response masking?** Encrypted credentials should never be returned in GET responses. Return `"configured": true` only (not even masked partial values like `AKIA***MPLE`).

5. **Backward compatibility?** Inference and connectors currently store plaintext. Migration: on upgrade, SecretsService reads plaintext, encrypts, writes back. One-time migration during rolling upgrade.

6. **Scope for ESQL MVP?** If shared SecretsService is too much scope for ESQL GA, Option B (ESQL-only) is viable as a stepping stone. But design the interface so it can be replaced by a shared service later.

---

## 8. File Reference

### Current Plaintext Storage
| Component | File | Line |
|-----------|------|------|
| Inference secrets index | `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferenceSecretsIndex.java` | 25 |
| Inference model registry | `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/registry/ModelRegistry.java` | 647-653 |
| Default API key (plaintext) | `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/settings/DefaultSecretSettings.java` | 97 |
| AWS secrets (plaintext) | `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/common/amazon/AwsSecretSettings.java` | 98-99 |
| Connector secrets | `x-pack/plugin/ent-search/src/main/java/org/elasticsearch/xpack/application/connector/secrets/ConnectorSecretsIndexService.java` | — |
| ESQL S3 (no storage) | `x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3DataSourcePlugin.java` | 39-48 |
| ESQL Azure (no storage) | `x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureDataSourcePlugin.java` | 52-56 |
| ESQL GCS (no storage) | `x-pack/plugin/esql-datasource-gcs/src/main/java/org/elasticsearch/xpack/esql/datasource/gcs/GcsDataSourcePlugin.java` | 50-54 |

### Existing Encryption Patterns
| Component | File | Algorithm |
|-----------|------|-----------|
| Watcher CryptoService | `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/crypto/CryptoService.java` | AES/CTR (legacy) |
| TokenService | `x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/authc/TokenService.java:158-250` | AES/GCM (modern) |
| License CryptUtils | `x-pack/plugin/core/src/main/java/org/elasticsearch/license/CryptUtils.java` | AES/PBKDF2 |

### Key Distribution Infrastructure
| Component | File | Purpose |
|-----------|------|---------|
| KeyStoreWrapper | `server/src/main/java/org/elasticsearch/common/settings/KeyStoreWrapper.java` | Node-local encrypted keystore |
| ClusterSecrets | `server/src/main/java/org/elasticsearch/common/settings/ClusterSecrets.java` | Cluster-state secret distribution (ephemeral `ClusterState.Custom`, NOT persisted) |
| SecureClusterStateSettings | `server/src/main/java/org/elasticsearch/common/settings/SecureClusterStateSettings.java` | Serialize/deserialize cluster secrets |
| ProjectSecrets | `server/src/main/java/org/elasticsearch/common/settings/ProjectSecrets.java` | Per-project isolation (serverless) |
| RotatableSecret | `server/src/main/java/org/elasticsearch/common/settings/RotatableSecret.java` | Key rotation with grace period |
| ConsistentSettingsService | `server/src/main/java/org/elasticsearch/common/settings/ConsistentSettingsService.java` | Cross-node secret verification |

### Metadata Persistence Infrastructure
| Component | File | Purpose |
|-----------|------|---------|
| Metadata | `server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java:81-157` | `ClusterCustom`, `ProjectCustom`, `XContentContext` enum |
| PersistedClusterStateService | `server/src/main/java/org/elasticsearch/gateway/PersistedClusterStateService.java` | Lucene-based gateway; persists `Metadata` with `GATEWAY` context to disk on all master + data nodes |
| GatewayMetaState | `server/src/main/java/org/elasticsearch/gateway/GatewayMetaState.java:147` | `isMasterNode \|\| canContainData` — controls which nodes persist gateway |
| TokenMetadata | `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/security/authc/TokenMetadata.java` | Example: `ClusterState.Custom` with `isPrivate()=true` — ephemeral, NOT persisted |
| LicensesMetadata | `x-pack/plugin/core/src/main/java/org/elasticsearch/license/LicensesMetadata.java` | Example: `context() = GATEWAY` only — persisted, not in API/snapshots |
| AutoscalingMetadata | `x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/AutoscalingMetadata.java` | Example: `context() = ALL_CONTEXTS` — persisted, in API, in snapshots |
