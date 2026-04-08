# Deep Dive: Datasource CRUD API & Secret Management

## Executive Summary

The CRUD API and secret management are **loosely coupled**: you can ship a useful CRUD API
that stores only non-sensitive config, while deferring inline-encrypted secrets to a later
phase.  The inverse (secrets without CRUD) is not meaningful -- encrypted secrets need
a cluster-state-persisted definition to live in.

**Recommended phasing:**
1. **MVP TP**: CRUD API with keystore-reference-only credential support (2w H+AI)
2. **MVP GA**: Add inline encrypted credentials in cluster state (3-4w H+AI)
3. **Post-MVP**: Credential rotation, per-credential audit, external secret stores

---

## 1. Current Config Flow

### 1.1 WITH Clause -> Map<String, Object>

The end-to-end path for credentials today:

```
ESQL query text (WITH access_key="AKIA...", secret_key="...")
        |
        v
Parser (LogicalPlanBuilder.visitExternalCommand)
    -> Map<String, Expression> params (from commandNamedParameters)
    -> UnresolvedExternalRelation(tablePath, params)
        |
        v
EsqlSession.resolveExternalSources()
    -> ExternalSourceResolver.resolve(paths, pathParams, ...)
        |
        v
ExternalSourceResolver.paramsToConfigMap(params)
    -> Map<String, Object> config
    (Literal values converted to strings, BytesRef->String)
        |
        v
ExternalSourceFactory.resolveMetadata(path, config)
    OR StorageProviderRegistry.createProvider(scheme, settings, config)
        |
        v
StorageProviderFactory.create(settings, config)
    (e.g. S3DataSourcePlugin extracts config.get("access_key"))
        |
        v
S3Configuration / AzureConfiguration / GcsConfiguration
    (Plain Java objects holding credentials as Strings)
        |
        v
S3Client / BlobServiceClient / Storage
    (Cloud SDK client built with credentials)
```

**Key files:**
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java` (line ~781)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` (line 286)
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3DataSourcePlugin.java` (line 39-48)

### 1.2 Credential Fields Per Provider

| Provider | Sensitive Fields | Non-Sensitive Fields |
|----------|-----------------|---------------------|
| S3       | `access_key`, `secret_key` | `endpoint`, `region` |
| Azure    | `connection_string`, `key`, `sas_token` | `account`, `endpoint` |
| GCS      | `credentials` (full JSON) | `project_id`, `endpoint`, `token_uri` |

### 1.3 Keystore Integration -- NONE

There is **zero** keystore integration in the ESQL datasource path today.
- `ExternalSourceResolver` receives `Settings settings` in its constructor but only passes it through to `StorageProviderRegistry`
- `StorageProviderFactory.create(Settings settings)` receives node settings but **no** datasource plugin reads secure settings from keystore
- The `S3DataSourcePlugin`, `AzureDataSourcePlugin`, `GcsDataSourcePlugin` all declare **zero** `SecureSetting` entries
- Compare with `S3ClientSettings` in the repository-s3 module which declares `ACCESS_KEY_SETTING`, `SECRET_KEY_SETTING`, `SESSION_TOKEN_SETTING` as `SecureSetting.secureString()` with affix pattern `s3.client.{clientName}.access_key`

**Implication**: Today, credentials can ONLY be passed via `WITH` clause (plaintext in query). There is no way to reference pre-configured credentials.

---

## 2. Existing CRUD Patterns in Elasticsearch

### 2.1 ViewMetadata / ViewService (Best Model)

The Views system is the closest precedent and was designed specifically for ESQL.

**Architecture:**
- `ViewMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>` -- stored in cluster state
- `ViewService` -- CRUD operations via `AckedClusterStateUpdateTask`
- REST endpoints: `PUT /_query/view/{name}`, `GET /_query/view/{name}`, `DELETE /_query/view/{name}`
- Transport actions: `TransportPutViewAction`, `TransportGetViewAction`, `TransportDeleteViewAction`
- `View` implements `Writeable, ToXContentObject, IndexAbstraction` -- views appear in the indices lookup

**Key files:**
- `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/ViewMetadata.java`
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/view/ViewService.java`
- `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/View.java`

**What Views store:** Only non-sensitive data (name + query string). No credentials.

**Pattern to follow for datasources:**
- `DatasourceMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>` (cluster state)
- `DatasourceService` with CRUD via `AckedClusterStateUpdateTask`
- A `Datasource` record with name, type, URI pattern, non-sensitive config, credential-reference

### 2.2 Ingest Pipelines (Cluster State, No Secrets)

- Stored in cluster state as `IngestMetadata extends Metadata.Custom`
- `PUT /_ingest/pipeline/{id}`, etc.
- No sensitive fields -- processors don't store credentials

### 2.3 Transforms (Index-Based, No Secrets)

- Stored in `.transform-internal-*` system indices, NOT cluster state
- TransformConfig has no sensitive fields at all
- Uses index for storage because configs can be large (source/dest/pivot definitions)

### 2.4 Watcher (Index-Based + Optional Encryption)

- Watch definitions stored in `.watches` system index
- Sensitive data: webhook credentials, email passwords, API keys
- Optional encryption via `CryptoService` (AES/CTR) using a keystore-stored system key
- `xpack.watcher.encrypt_sensitive_data=true` enables encryption at the field level
- `CryptoService` derives encryption key from `xpack.watcher.encryption_key` (stored as `SecureSetting.secureFile` in keystore)
- Encrypted values are prefixed with `::es_encrypted::` + base64

**Key file:**
- `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/watcher/crypto/CryptoService.java`

**Watcher's approach has known weaknesses:**
- Uses AES/CTR without authentication (no GCM) -- acknowledged as legacy in the code
- System key must be manually distributed to all nodes
- Encryption key derived from system key is deterministic (no per-secret salt)
- The open issue #136001 requests keystore variable interpolation as a better approach

### 2.5 ML Jobs (Headers in Internal Index)

- ML job configs stored in `.ml-config` system index
- Security headers (auth tokens) captured at job creation time and stored alongside the job
- Headers are stored encrypted when security is enabled
- Not user-visible -- internal plumbing

---

## 3. Keystore Architecture

### 3.1 How It Works

`KeyStoreWrapper` is a **node-local** encrypted file (`elasticsearch.keystore`):
- File: `{config_dir}/elasticsearch.keystore`
- Encryption: PBKDF2WithHmacSHA512 key derivation, AES-256-GCM encryption (v7+)
- Loaded at node startup, decrypted once, held in memory
- Settings are accessed via `SecureSettings` interface through `Settings.getSecureSettings()`

**Key file:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/KeyStoreWrapper.java`

### 3.2 Node-Local, NOT Cluster-Replicated

The keystore is explicitly **per-node**:
- Each node has its own `elasticsearch.keystore` file
- Keystore is NOT replicated through cluster state
- `ConsistentSettingsService` publishes PBKDF2 **hashes** (not values) to cluster state to verify that all nodes have the same values for `Property.Consistent` settings
- But the actual secret values stay on each node's filesystem

**Key file:** `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/ConsistentSettingsService.java`

### 3.3 How Plugins Read Keystore Secrets

Pattern used by repository-s3:
```java
// Declare a secure setting with affix (multi-client support)
static final AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(
    "s3.client.",
    "access_key",
    key -> SecureSetting.secureString(key, null)
);

// Read it from Settings (which internally delegates to SecureSettings)
SecureString accessKey = ACCESS_KEY_SETTING.getConcreteSettingForNamespace(clientName).get(settings);
```

Keystore entry: `s3.client.default.access_key` -> value stored encrypted on disk

The plugin:
1. Declares `SecureSetting` entries in `getSettings()`
2. Reads values from `Settings` at initialization time
3. Values come from keystore transparently (the `Settings` object wraps `SecureSettings`)

**Key files:**
- `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-s3/src/main/java/org/elasticsearch/repositories/s3/S3ClientSettings.java`
- `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-azure/src/main/java/org/elasticsearch/repositories/azure/AzureStorageSettings.java`
- `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-gcs/src/main/java/org/elasticsearch/repositories/gcs/GoogleCloudStorageClientSettings.java`

### 3.4 Can a Datasource Definition Reference Keystore Entries?

**Not today**, but it's technically straightforward:

The datasource definition would store a reference like `"access_key_ref": "esql.datasource.my_s3.access_key"` and at query time, the code would resolve:
```java
SecureString value = ESQL_DATASOURCE_ACCESS_KEY.getConcreteSettingForNamespace(datasourceName).get(settings);
```

The main requirement: ESQL datasource plugins would need to declare `SecureSetting` entries with an affix pattern like `esql.datasource.{name}.access_key`. These would need to be registered in `getSettings()` on the plugin.

**Challenge**: The affix pattern requires knowing the datasource name at plugin registration time, but datasource names are dynamic (created via CRUD API). Solution: Use the same pattern as repository-s3 where affix settings are dynamically namespaced. The `AffixSetting` system already handles this -- it allows any namespace value.

---

## 4. Secret Management Options

### Option A: Keystore References Only

**How it works:**
- Datasource definition in cluster state stores only references: `"credential_ref": "my_s3_creds"`
- User pre-configures secrets in keystore on each node: `esql.datasource.my_s3_creds.access_key`, `esql.datasource.my_s3_creds.secret_key`
- At query time, resolve the reference against node-local keystore

**Example CRUD body:**
```json
PUT /_datasource/my_s3_logs
{
  "type": "s3",
  "uri_pattern": "s3://my-bucket/logs/{date}/*.parquet",
  "config": {
    "region": "us-east-1"
  },
  "credentials": {
    "type": "keystore",
    "name": "my_s3_creds"
  }
}
```

**Pros:**
- Minimal implementation effort (1-2w)
- Reuses battle-tested keystore infrastructure
- Secrets never touch cluster state or network
- Operationally familiar to users who already use keystore for repositories

**Cons:**
- Requires manual keystore setup on every node (operational burden)
- Cannot add new datasource credentials without restarting nodes (keystore is read at startup)
- No API for credential management -- purely CLI (`elasticsearch-keystore add`)
- Doesn't work on managed deployments (Elastic Cloud) where users can't access node filesystem

**Work estimate: ~1.5w H+AI** (define affix settings, wire resolution, test)

### Option B: Inline Encrypted Credentials in Cluster State

**How it works:**
- Datasource definition includes credentials in the PUT body
- Server encrypts sensitive fields before writing to cluster state
- Encryption key derived from keystore-stored master secret (like Watcher's `CryptoService`)
- GET responses mask/omit sensitive values

**Example CRUD body:**
```json
PUT /_datasource/my_s3_logs
{
  "type": "s3",
  "uri_pattern": "s3://my-bucket/logs/{date}/*.parquet",
  "config": {
    "region": "us-east-1"
  },
  "credentials": {
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  }
}
```

Internally stored as:
```json
{
  "credentials": {
    "access_key": "::es_encrypted::BASE64...",
    "secret_key": "::es_encrypted::BASE64..."
  }
}
```

GET response:
```json
{
  "credentials": {
    "access_key": "AKIA***MPLE",
    "secret_key": "****"
  }
}
```

**Pros:**
- API-only workflow -- no node filesystem access needed
- Works on managed deployments (Elastic Cloud)
- Credentials replicated to all nodes automatically via cluster state
- Better UX for dynamic credential management

**Cons:**
- Significantly more implementation effort (3-4w)
- Secrets in cluster state (encrypted, but still a larger blast radius)
- Need a new `EsqlCryptoService` (or reuse Watcher's, which has known weaknesses)
- Encryption key bootstrap problem: needs a keystore-stored master key on all nodes
- Migration path when master key changes

**Work estimate: ~3.5w H+AI** (crypto service, field-level encryption, masking in GET, tests, security review)

### Option C: Hybrid (Recommended)

Support both:
- `"credentials": {"type": "keystore", "name": "my_creds"}` -- reference mode
- `"credentials": {"type": "inline", "access_key": "...", "secret_key": "..."}` -- encrypted mode

Ship keystore references in TP, add inline encrypted in GA.

### Comparison

| Aspect | Keystore Refs (A) | Inline Encrypted (B) |
|--------|-------------------|---------------------|
| Implementation effort | ~1.5w | ~3.5w |
| Operational burden | High (per-node) | Low (API only) |
| Elastic Cloud support | No | Yes |
| Security posture | Excellent | Good (encrypted in CS) |
| Dynamic updates | No (restart required) | Yes |
| Key management | None (existing keystore) | Master key in keystore |

---

## 5. Can You Have CRUD Without Secrets? Can You Have Secrets Without CRUD?

### CRUD Without Secrets: YES (and this is the right MVP)

A datasource definition can store:
- Name, type (s3/azure/gcs/iceberg)
- URI pattern
- Non-sensitive config (region, endpoint, format hints, schema overrides)
- Credential reference (keystore name or "use default credentials")

This is already extremely useful:
```
FROM datasource::my_s3_logs
```
Instead of:
```
EXTERNAL "s3://my-bucket/logs/2026-03-03/*.parquet"
  WITH (region="us-east-1", access_key="...", secret_key="...")
```

### Secrets Without CRUD: NO (not meaningful)

Encrypted secrets need a persistent definition to be associated with. Without CRUD:
- Where would you store the encrypted credentials? (Not in keystore -- that's already Option A)
- A one-off API that just stores encrypted blobs in cluster state with no associated datasource config is useless

---

## 6. Proposed REST API

### Endpoints

```
PUT    /_datasource/{name}          # Create or update
GET    /_datasource/{name}          # Get single
GET    /_datasource                  # List all (with optional type filter)
DELETE /_datasource/{name}          # Delete
```

Following the same pattern as `/_query/view/{name}`.

### PUT Body

```json
{
  "type": "s3",
  "uri_pattern": "s3://my-bucket/logs/**/*.parquet",
  "description": "Production log files in Parquet format",
  "config": {
    "region": "us-east-1",
    "format": "parquet",
    "hive_partitioning": true
  },
  "credentials": {
    "type": "keystore",       // or "inline" in GA
    "name": "my_s3_creds"    // for keystore type
    // "access_key": "...",   // for inline type (GA)
    // "secret_key": "..."    // for inline type (GA)
  },
  "schema_override": {
    "timestamp": "date",
    "level": "keyword"
  }
}
```

### GET Response (Secrets Masked)

```json
{
  "my_s3_logs": {
    "type": "s3",
    "uri_pattern": "s3://my-bucket/logs/**/*.parquet",
    "description": "Production log files in Parquet format",
    "config": {
      "region": "us-east-1",
      "format": "parquet",
      "hive_partitioning": true
    },
    "credentials": {
      "type": "keystore",
      "name": "my_s3_creds",
      "configured": true        // indicates keystore entries exist
    },
    "schema_override": {
      "timestamp": "date",
      "level": "keyword"
    }
  }
}
```

For inline credentials (GA):
```json
{
  "credentials": {
    "type": "inline",
    "access_key": "AKIA***MPLE",
    "secret_key": "****"
  }
}
```

### Validation

On PUT:
- Name must be valid (same rules as view names -- lowercase, no special chars)
- Type must be a recognized datasource type (s3/azure/gcs/iceberg/flight)
- URI pattern must be parseable by `StoragePath.of()`
- If `credentials.type: "keystore"`, optionally warn if keystore entries don't exist on the current node
- If `credentials.type: "inline"`, require encryption to be configured

On DELETE:
- Return 404 if datasource doesn't exist (same as views)

---

## 7. Implementation Sketch

### 7.1 Cluster State Model (following ViewMetadata)

```java
// DatasourceMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>
public final class DatasourceMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>
    implements Metadata.ProjectCustom {
    public static final String TYPE = "esql_datasource";
    private final Map<String, Datasource> datasources;
    // ... writeTo, toXContentChunked, etc.
}

// Datasource record
public record Datasource(
    String name,
    String type,              // "s3", "azure", "gcs", "iceberg"
    String uriPattern,
    String description,
    Map<String, Object> config,
    CredentialRef credentials,
    Map<String, String> schemaOverride
) implements Writeable, ToXContentObject { ... }

// Credential reference (extensible for inline in GA)
public sealed interface CredentialRef permits KeystoreCredentialRef, InlineCredentialRef {
    String type();
}
public record KeystoreCredentialRef(String name) implements CredentialRef {
    public String type() { return "keystore"; }
}
```

### 7.2 Wiring Into Query Resolution

When the parser sees `FROM datasource::my_s3_logs`:
1. Parser creates a new plan node (e.g., `UnresolvedDatasourceRelation`)
2. Analyzer looks up `my_s3_logs` in `DatasourceMetadata` from cluster state
3. Merges stored config with any query-level WITH overrides
4. Resolves credential reference:
   - Keystore: read from `Settings.getSecureSettings()` using the affix pattern
   - Inline (GA): decrypt from stored definition
5. Constructs `Map<String, Object> config` and feeds into existing `ExternalSourceResolver` path
6. From this point, everything is identical to today's `EXTERNAL "..." WITH (...)` flow

### 7.3 Keystore Setting Declaration

```java
// In EsqlPlugin.getSettings()
static final AffixSetting<SecureString> DATASOURCE_ACCESS_KEY = Setting.affixKeySetting(
    "esql.datasource.",
    "access_key",
    key -> SecureSetting.secureString(key, null)
);
// ... similar for secret_key, sas_token, key, credentials, connection_string
```

User runs:
```bash
bin/elasticsearch-keystore add esql.datasource.my_s3_creds.access_key
bin/elasticsearch-keystore add esql.datasource.my_s3_creds.secret_key
```

---

## 8. Dependency Analysis

```
                    CRUD API (non-sensitive config)
                    |
                    |  depends on
                    v
        FROM datasource::name syntax  <--- can ship together
                    |
                    |  enhances (not blocks)
                    v
            Secret Management
            /              \
    Keystore Refs        Inline Encrypted
    (TP, 1.5w)           (GA, 3.5w)
```

**The CRUD API does NOT depend on secret management.** You can store datasource definitions
with `"credentials": {"type": "default"}` (use cloud provider default credentials --
IAM roles, managed identity, workload identity federation) and it's immediately useful.

**Secret management depends on CRUD** -- you need a persistent definition to attach secrets to.

**Recommended plan:**
1. CRUD API + `FROM datasource::name` syntax + default credentials (TP) -- 2w
2. Add keystore reference support (TP) -- 1w additional
3. Add inline encrypted credentials (GA) -- 3.5w additional

---

## 9. Open Questions

1. **Should datasources be ProjectCustom or ClusterCustom?** Views use `ProjectCustom`, which seems right for multi-tenancy. Datasources should follow the same pattern.

2. **URI pattern vs exact URI?** Should the definition store a glob pattern (`s3://bucket/logs/**/*.parquet`) or just the bucket prefix? Glob patterns are more flexible but the schema inference needs at least one file.

3. **Schema caching in the definition?** Could the CRUD API also store the discovered schema to avoid re-reading on every query? This overlaps with the schema caching item in the roadmap.

4. **How to handle credential rotation?** With keystore refs, user updates keystore and restarts. With inline encrypted, user PUTs an updated definition. Should there be a separate `POST /_datasource/{name}/_rotate_credentials` endpoint?

5. **Authorization model?** Who can create/read/delete datasources? This maps to the cluster privilege model -- probably a new `manage_esql_datasource` privilege similar to `manage_transform`.

6. **Should GET mask or omit secrets?** Masking (`AKIA***MPLE`) leaks partial information. Omitting entirely is safer. Watcher omits. Consider: `"credentials": {"type": "inline", "configured": true}` with no value at all.
