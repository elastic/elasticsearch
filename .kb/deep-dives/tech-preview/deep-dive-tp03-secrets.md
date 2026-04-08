# Deep Dive: Secret Management (Keystore References)

## 1. KeyStoreWrapper

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/KeyStoreWrapper.java`

### How it works

`KeyStoreWrapper` implements the `SecureSettings` interface (line 76) and provides disk-based, AES-256-GCM encrypted storage for sensitive settings.

**Storage format** (line 147-154):
- Version 7 (current, ES 9.0): 256-bit cipher key, PBKDF2WithHmacSHA512 with 210,000 iterations
- File: `elasticsearch.keystore` in the config directory (line 114)
- Format: Lucene codec header + hasPassword byte + encrypted data block (salt + IV + ciphertext) + Lucene footer

**Key lifecycle**:
1. **Creation**: `KeyStoreWrapper.create()` (line 196-201) -- creates empty keystore with bootstrap seed
2. **Loading**: `KeyStoreWrapper.load(Path configDir)` (line 246-293) -- reads raw encrypted bytes from disk, does NOT decrypt
3. **Decryption**: `decrypt(char[] password)` (line 353-411) -- derives AES key from password via PBKDF2, decrypts entries into `Map<String, Entry>`
4. **Bootstrap**: `bootstrap(Path, Supplier)` (line 217-238) -- loads + decrypts + upgrades, creating new keystore if none exists

**Entry storage** (line 81-100):
- Each entry is an `Entry` with raw `byte[]` and a precomputed `sha256Digest`
- Entries stored in `SetOnce<Map<String, Entry>>` -- can only be set once

**Retrieval**:
- `getString(String setting)` (line 561-567): decodes UTF-8 bytes to `SecureString`
- `getFile(String setting)` (line 570-574): returns `ByteArrayInputStream` over raw bytes
- `getSettingNames()` (line 554-557): returns key set (works even after close)
- `getSHA256Digest(String setting)` (line 581-585): returns digest for change detection without exposing value

**CLI tool**: `elasticsearch-keystore` (documented in `docs/reference/elasticsearch/command-line-tools/elasticsearch-keystore.md`)
- `elasticsearch-keystore add <setting>` -- add string setting
- `elasticsearch-keystore add-file <setting> <path>` -- add file setting
- `elasticsearch-keystore list` -- list setting names
- `elasticsearch-keystore remove <setting>` -- remove setting

---

## 2. SecureSetting

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/SecureSetting.java`

### How secure settings are defined

`SecureSetting<T>` extends `Setting<T>` (line 27) and provides read access to the Elasticsearch keystore. Key design constraints:

- **Always NodeScope**: Fixed property `Property.NodeScope` is appended (line 39)
- **No raw string access**: `getDefaultRaw()`, `getDefault()`, `innerGetRaw()` all throw `UnsupportedOperationException` (lines 57-69)
- **Key validation**: Constructor calls `KeyStoreWrapper.validateSettingName(key)` (line 44) -- enforces pattern `[A-Za-z0-9_\-.]+`

### Factory methods

1. **`secureString(String name, Setting<SecureString> fallback, Property... properties)`** (line 134-136):
   - Creates a `SecureStringSetting` that reads a string from the keystore
   - `getSecret()` calls `secureSettings.getString(getKey())` (line 167)
   - Falls back to `fallback.get(settings)` if not in keystore, or empty `SecureString` if no fallback (line 171-176)

2. **`secureFile(String name, Setting<InputStream> fallback, Property... properties)`** (line 153-155):
   - Creates a `SecureFileSetting` that reads a file/binary blob from the keystore
   - `getSecret()` calls `secureSettings.getFile(getKey())` (line 205)

3. **`insecureString(String name)`** (line 144-146):
   - Deprecated (line 143). Only used by S3 repository module for legacy plaintext credentials
   - Throws `IllegalArgumentException` at read time unless `-Des.allow_insecure_settings=true` (line 189-191)

### Runtime read flow

`SecureSetting.get(Settings settings)` (line 84-96):
1. Gets `SecureSettings` from `settings.getSecureSettings()` (line 86)
2. Checks if the key exists in `secureSettings.getSettingNames()` (line 88)
3. If exists: calls `getSecret(secureSettings)` which delegates to `getString()` or `getFile()`
4. If not exists: calls `getFallback(settings)` which returns fallback setting or empty value

The `Settings` object carries its `SecureSettings` reference internally (line 139-141 of `Settings.java`), set via `Settings.Builder.setSecureSettings()` (line 953 of `Settings.java`). During node bootstrap, the decrypted `KeyStoreWrapper` is attached to the node's `Settings` object.

---

## 3. S3 Credential Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-s3/src/main/java/org/elasticsearch/repositories/s3/S3ClientSettings.java`

### Secure settings definition (lines 57-75)

```java
// Prefix: "s3.client."
private static final String PREFIX = "s3.client.";

static final Setting.AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(
    PREFIX, "access_key",
    key -> SecureSetting.secureString(key, null)   // line 60
);

static final Setting.AffixSetting<SecureString> SECRET_KEY_SETTING = Setting.affixKeySetting(
    PREFIX, "secret_key",
    key -> SecureSetting.secureString(key, null)   // line 67
);

static final Setting.AffixSetting<SecureString> SESSION_TOKEN_SETTING = Setting.affixKeySetting(
    PREFIX, "session_token",
    key -> SecureSetting.secureString(key, null)   // line 74
);
```

This creates settings with keys like:
- `s3.client.default.access_key`
- `s3.client.my-client.access_key`
- `s3.client.default.secret_key`
- etc.

### How credentials are loaded

`S3ClientSettings.loadCredentials(Settings settings, String clientName)` (lines 466-492):
1. Calls `getConfigValue(settings, clientName, ACCESS_KEY_SETTING)` which resolves the affix to the concrete key
2. `getConfigValue` (line 575-578) calls `clientSetting.getConcreteSettingForNamespace(clientName).get(settings)`
3. This triggers `SecureSetting.get()` which reads from the keystore
4. If both access_key and secret_key present: creates `AwsBasicCredentials` or `AwsSessionCredentials`
5. If neither present: returns `null` (falls back to AWS default credential chain)

### Per-client settings pattern

`S3ClientSettings.load(Settings settings)` (lines 417-429):
1. Discovers all client names via `settings.getGroups(PREFIX).keySet()` (line 418)
2. For each client name, calls `getClientSettings(settings, clientName)` (line 421)
3. Always ensures a "default" client exists (line 423-426)

**User setup**: `elasticsearch-keystore add s3.client.my-backup.access_key`

Also has proxy credentials as secure settings (lines 115-126):
- `s3.client.{name}.proxy.username` -- `SecureSetting.secureString`
- `s3.client.{name}.proxy.password` -- `SecureSetting.secureString`

---

## 4. GCS Credential Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-gcs/src/main/java/org/elasticsearch/repositories/gcs/GoogleCloudStorageClientSettings.java`

### Service account file as secure file (line 46-50)

```java
private static final String PREFIX = "gcs.client.";

static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING = Setting.affixKeySetting(
    PREFIX, "credentials_file",
    key -> SecureSetting.secureFile(key, null)    // line 49
);
```

Key pattern: `gcs.client.{name}.credentials_file`

### How credentials are loaded

`GoogleCloudStorageClientSettings.loadCredential(Settings, String, Proxy)` (lines 289-309):
1. Checks if `CREDENTIALS_FILE_SETTING` exists for the client namespace (line 292)
2. If exists: opens `InputStream` from keystore via `credentialsFileSetting.get(settings)` (line 297)
3. Parses the stream as `ServiceAccountCredentials.fromStream(credStream)` (line 300)
4. If scoped credentials required, creates scoped version (lines 301-302)
5. If not exists: returns `null` (defaults to Application Default Credentials later)

**User setup**: `elasticsearch-keystore add-file gcs.client.default.credentials_file /path/to/service-account.json`

**Key difference from S3**: GCS uses `SecureSetting.secureFile()` (binary file blob) rather than `SecureSetting.secureString()`. The entire service account JSON is stored as a binary file in the keystore.

---

## 5. Azure Credential Pattern (Repository Plugin)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/modules/repository-azure/src/main/java/org/elasticsearch/repositories/azure/AzureStorageSettings.java`

### Secure settings definition (lines 41-62)

```java
private static final String AZURE_CLIENT_PREFIX_KEY = "azure.client.";

public static final AffixSetting<SecureString> ACCOUNT_SETTING = Setting.affixKeySetting(
    AZURE_CLIENT_PREFIX_KEY, "account",
    key -> SecureSetting.secureString(key, null)   // line 47
);

public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(
    AZURE_CLIENT_PREFIX_KEY, "key",
    key -> SecureSetting.secureString(key, null)   // line 54
);

public static final AffixSetting<SecureString> SAS_TOKEN_SETTING = Setting.affixKeySetting(
    AZURE_CLIENT_PREFIX_KEY, "sas_token",
    key -> SecureSetting.secureString(key, null)   // line 61
);
```

Key patterns:
- `azure.client.{name}.account`
- `azure.client.{name}.key`
- `azure.client.{name}.sas_token`

Auth modes: account+key (SharedKey), SAS token, or connection string (all three via keystore).

---

## 6. Current External Source (ESQL Datasource) Credential Flow

### The critical finding: ESQL datasource plugins do NOT use the keystore at all.

**S3 ESQL Datasource Plugin**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3DataSourcePlugin.java`
- `storageProviders()` (line 31-53): Creates `S3StorageProvider` from `Map<String, Object> config` -- plain string values
- Config keys: `access_key`, `secret_key`, `endpoint`, `region` (line 43-47)
- **No `SecureSetting` usage, no keystore integration**

**S3 Configuration**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3Configuration.java`
- Plain record with `String accessKey, secretKey, endpoint, region` (lines 21-24)
- `fromFields(String, String, String, String)` (line 50-55) -- plain strings, no secure handling
- `fromParams(Map<String, Expression>)` (line 33-48) -- extracts from query literal expressions

**S3 Storage Provider**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3StorageProvider.java`
- `buildS3Client()` (lines 50-81): Uses `StaticCredentialsProvider` with plaintext strings or `DefaultCredentialsProvider`
- No integration with repository-s3's `S3ClientSettings` or its keystore-backed `SecureSetting`s

**GCS ESQL Datasource Plugin**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-gcs/src/main/java/org/elasticsearch/xpack/esql/datasource/gcs/GcsDataSourcePlugin.java`
- Config keys: `credentials` (service account JSON as string), `project_id`, `endpoint`, `token_uri` (lines 50-54)
- **No `SecureSetting.secureFile()` usage** -- service account JSON passed as plain string in query

**GCS Configuration**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-gcs/src/main/java/org/elasticsearch/xpack/esql/datasource/gcs/GcsConfiguration.java`
- Record: `String serviceAccountCredentials, String projectId, String endpoint, String tokenUri` (line 25)

**Azure ESQL Datasource Plugin**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureDataSourcePlugin.java`
- Config keys: `connection_string`, `account`, `key`, `sas_token`, `endpoint` (lines 52-56)
- **No `SecureSetting` usage** -- all values from `Map<String, Object> config`

**Azure Configuration**:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureConfiguration.java`
- Record: `String connectionString, String account, String key, String sasToken, String endpoint` (line 28)
- Auth modes: connection_string, account+key (SharedKey), account+sas_token, or DefaultAzureCredential (lines 66-70)

### How config flows today

The complete credential flow for ESQL external sources:

1. **Parser** (`LogicalPlanBuilder.visitExternalCommand`, line 781):
   ```
   EXTERNAL "s3://bucket/file.parquet" WITH (access_key="AKIA...", secret_key="...")
   ```
   Creates `UnresolvedExternalRelation(tablePath, params)` where `params` is `Map<String, Expression>` of literal values.

2. **PreAnalyzer/Session**: Extracts external paths and params, passes to `ExternalSourceResolver.resolve()`

3. **ExternalSourceResolver** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`):
   - `paramsToConfigMap()` (lines 286-305): Converts `Map<String, Expression>` to `Map<String, Object>` (extracting literal values as plain strings)
   - Passes config map to `factory.resolveMetadata(path, config)` and `registry.createProvider(scheme, settings, config)`
   - Wraps result in `ExternalSourceMetadata` which carries `config()` map through to execution

4. **StorageProviderFactory.create(Settings, Map<String, Object>)** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/StorageProviderFactory.java`, line 24):
   - Each plugin interprets the `Map<String, Object>` config to create credentials
   - S3: creates `S3Configuration.fromFields(accessKey, secretKey, endpoint, region)`
   - GCS: creates `GcsConfiguration.fromFields(credentials, projectId, endpoint, tokenUri)`
   - Azure: creates `AzureConfiguration.fromFields(connectionString, account, key, sasToken, endpoint)`

5. **Execution**: Config map stored in `SourceMetadata.config()`, passed through `ExternalRelation` -> `ExternalSourceExec` -> operator factory

**Summary**: Credentials travel as plaintext strings from query text through the entire pipeline. No keystore, no `SecureSetting`, no encryption at rest.

---

## 7. affixKeySetting Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/settings/Setting.java`

### How it works (lines 2269-2296)

```java
public static <T> AffixSetting<T> affixKeySetting(
    String prefix,    // e.g. "s3.client."
    String suffix,    // e.g. "access_key"
    Function<String, Setting<T>> delegateFactory,  // e.g. key -> SecureSetting.secureString(key, null)
    AffixSettingDependency... dependencies
)
```

**AffixKey** (line 2397-2441):
- Builds regex pattern: `(prefix)([-\w]+)\.suffix` to match keys like `s3.client.default.access_key`
- `getNamespace(key)` (line 2484-2490): Extracts the namespace portion (e.g., "default" from "s3.client.default.access_key")
- `toConcreteKey(namespace)` (line 2492-2503): Builds full key from prefix + namespace + "." + suffix

**getConcreteSettingForNamespace(String namespace)** (line 1101-1104):
```java
public Setting<T> getConcreteSettingForNamespace(String namespace) {
    String fullKey = key.toConcreteKey(namespace);
    return getConcreteSetting(namespace, fullKey);
}
```
Returns a concrete `Setting<T>` with the namespace filled in. For example:
- `ACCESS_KEY_SETTING.getConcreteSettingForNamespace("my-backup")` returns a setting with key `s3.client.my-backup.access_key`
- Calling `.get(settings)` on this concrete setting reads from the keystore if it's a `SecureSetting`

### Concrete examples

| Plugin | Prefix | Suffix | Full key pattern | Type |
|--------|--------|--------|-----------------|------|
| S3 repo | `s3.client.` | `access_key` | `s3.client.{name}.access_key` | `SecureSetting.secureString` |
| S3 repo | `s3.client.` | `secret_key` | `s3.client.{name}.secret_key` | `SecureSetting.secureString` |
| S3 repo | `s3.client.` | `session_token` | `s3.client.{name}.session_token` | `SecureSetting.secureString` |
| S3 repo | `s3.client.` | `proxy.username` | `s3.client.{name}.proxy.username` | `SecureSetting.secureString` |
| S3 repo | `s3.client.` | `proxy.password` | `s3.client.{name}.proxy.password` | `SecureSetting.secureString` |
| GCS repo | `gcs.client.` | `credentials_file` | `gcs.client.{name}.credentials_file` | `SecureSetting.secureFile` |
| Azure repo | `azure.client.` | `account` | `azure.client.{name}.account` | `SecureSetting.secureString` |
| Azure repo | `azure.client.` | `key` | `azure.client.{name}.key` | `SecureSetting.secureString` |
| Azure repo | `azure.client.` | `sas_token` | `azure.client.{name}.sas_token` | `SecureSetting.secureString` |

---

## 8. What Needs to Change for Keystore-Backed Datasource CRUD API

### Current state

The ESQL datasource plugins exist in a completely separate world from the repository plugins:

| Aspect | Repository plugins (S3/GCS/Azure) | ESQL datasource plugins |
|--------|----------------------------------|------------------------|
| Credentials | `SecureSetting` + keystore | Plain `Map<String, Object>` from query text |
| Per-instance | `affixKeySetting` with named clients | None -- credentials per-query only |
| At rest | AES-256-GCM encrypted | Not stored (query ephemeral) |
| CLI | `elasticsearch-keystore add` | N/A |
| Node scope | Yes (set once, all queries use) | No (each query provides its own) |

### Existing integration points

1. **`Settings` object already flows to plugins**: `DataSourcePlugin.storageProviders(Settings settings)` receives node settings which contain `SecureSettings`. Every plugin already has access to the keystore.

2. **`StorageProviderFactory.create(Settings settings, Map<String, Object> config)`**: The two-argument version already exists (line 24 of `StorageProviderFactory.java`). The `Settings` parameter carries the keystore, but no plugin reads from it today.

3. **`ExternalSourceResolver` has `Settings`**: Constructor takes `Settings` (line 71 of `ExternalSourceResolver.java`), passed to `registry.createProvider(scheme, settings, config)` (line 155).

4. **Repository plugins already define all the affix settings**: `S3ClientSettings`, `GoogleCloudStorageClientSettings`, `AzureStorageSettings` each define complete keystore-backed credential patterns that could be reused or mirrored.

### The gap

For a datasource CRUD API where users create named datasource configurations with keystore-backed credentials, these changes are needed:

**A. Credential storage layer**:
- Define new `SecureSetting` affix keys for ESQL datasources, e.g.:
  - `esql.datasource.{name}.s3.access_key` -- `SecureSetting.secureString`
  - `esql.datasource.{name}.s3.secret_key` -- `SecureSetting.secureString`
  - `esql.datasource.{name}.gcs.credentials_file` -- `SecureSetting.secureFile`
  - `esql.datasource.{name}.azure.key` -- `SecureSetting.secureString`
  - `esql.datasource.{name}.azure.sas_token` -- `SecureSetting.secureString`
- OR reuse existing repository plugin settings (e.g., `s3.client.{name}.*`) by sharing client configuration
- **Problem**: `SecureSetting` keys must be known at node startup (statically defined). A CRUD API that creates datasources at runtime cannot add new keystore entries without node restart.

**B. The fundamental tension: static keystore vs. dynamic CRUD**:
- The keystore is a static, node-local, file-based store. Entries are added via CLI and require node restart to take effect.
- A CRUD API expects dynamic creation/deletion of datasources without restart.
- The repository plugin solves this by having a fixed set of named clients (e.g., `s3.client.default`, `s3.client.my-backup`) that are configured at startup, then referenced by name in repository creation.
- **Option 1**: Follow repository pattern -- users pre-configure named credentials in keystore, then reference by name in CRUD API. This means: `PUT _esql/datasource/my-s3 { "type": "s3", "client": "my-backup" }` which looks up `s3.client.my-backup.access_key` from keystore.
- **Option 2**: Store credentials in cluster state (encrypted). This is what Kibana Fleet/Connectors do. Requires a new secrets store abstraction beyond `KeyStoreWrapper`.
- **Option 3**: Store datasource configs in cluster state with credential references to keystore entries. Non-secret fields (endpoint, region) in cluster state, secret references point to pre-configured keystore entries.

**C. Plugin wiring changes needed**:
1. `DataSourcePlugin` implementations need to accept a credential reference (e.g., client name) in addition to inline config
2. `StorageProviderFactory.create(Settings settings, Map<String, Object> config)` already has `Settings` -- it just needs to look up keystore values from it
3. `ExternalSourceResolver.paramsToConfigMap()` (line 286-305) needs to handle credential references: e.g., if config has `"credentials_ref": "my-s3-creds"`, resolve from keystore instead of expecting inline values
4. A CRUD API transport action (e.g., `TransportPutDataSourceAction`) would store the datasource definition in cluster state metadata
5. The query `FROM datasource::my-s3/path/file.parquet` would resolve the datasource name, look up its config from cluster state + keystore, and pass to the factory

**D. The `StorageProviderFactory` change is minimal**:
```java
// Current:
StorageProvider create(Settings settings, Map<String, Object> config);

// The Settings already carries SecureSettings (keystore). A plugin could do:
public StorageProvider create(Settings settings, Map<String, Object> config) {
    String clientName = (String) config.get("client");
    if (clientName != null) {
        // Read from keystore using existing affix settings
        SecureString accessKey = S3ClientSettings.ACCESS_KEY_SETTING
            .getConcreteSettingForNamespace(clientName).get(settings);
        // ... build provider with keystore-backed credentials
    }
}
```

**E. Security considerations**:
- Today credentials in `EXTERNAL ... WITH (secret_key="...")` are visible in query text, audit logs, slow logs
- Keystore-backed references would eliminate this exposure
- `SecureSetting.diff()` is a no-op (line 127 of `SecureSetting.java`) -- secure settings never appear in diffs
- `SecureString` implements `Closeable` and zeros memory on close

### Summary of gaps

| Gap | Severity | Notes |
|-----|----------|-------|
| No keystore integration in ESQL datasource plugins | High | Credentials travel as plaintext through entire pipeline |
| No named datasource persistence (CRUD API) | High | No cluster state storage for datasource definitions |
| Static keystore vs dynamic CRUD tension | Design | Requires architecture decision on credential storage model |
| ESQL plugins don't reuse repository plugin client configs | Medium | Duplicated S3/GCS/Azure client creation code |
| No credential reference resolution in config flow | Medium | `paramsToConfigMap()` only handles inline literals |
| Query text contains secrets in audit/slow logs | High | Security exposure from inline credentials |
