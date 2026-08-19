use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock, Mutex};

use lru::LruCache;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::client::ClientOptions;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::limit::LimitStore;
use object_store::local::LocalFileSystem;
use url::Url;

/// Storage connection parameters deserialized from the JSON config map
/// passed across JNI from the ESQL WITH clause.
/// New keys can be added here without changing any JNI signatures.
#[derive(Clone, Default)]
pub struct StorageConfig {
    map: HashMap<String, String>,
}

impl StorageConfig {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let map: HashMap<String, String> = serde_json::from_str(json)?;
        Ok(Self { map })
    }

    pub fn empty() -> Self {
        Self { map: HashMap::new() }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(|s| s.as_str())
    }

    /// Stable string encoding connection-identity fields used as a store cache key.
    /// Fields that affect which remote endpoint/auth are included; per-request options are not.
    pub fn connection_key(&self) -> String {
        const IDENTITY_FIELDS: &[&str] = &[
            "access_key", "account", "account_key", "account_name", "auth",
            "credentials", "endpoint", "key", "region", "sas_token",
        ];
        let mut parts: Vec<String> = IDENTITY_FIELDS
            .iter()
            .filter_map(|k| self.map.get(*k).map(|v| format!("{}={}", k, v)))
            .collect();
        parts.sort();
        parts.join(";")
    }
}

/// Returns true if the URI targets Azure Blob Storage, which does not support
/// suffix range requests (`bytes=-N`). Callers should pre-fetch the file size
/// via HEAD and pass it to `ParquetObjectReader::with_file_size` so that the
/// reader emits bounded range requests instead.
pub fn needs_file_size_hint(uri: &str) -> bool {
    uri.starts_with("az://") || uri.starts_with("abfss://") || uri.starts_with("wasbs://")
}

/// Maximum number of concurrent object store operations (head, get_range, get_ranges, etc.)
/// across all queries sharing the same bucket. Applies globally per store instance via
/// `LimitStore`. 64 concurrent range reads saturates typical S3 bandwidth without
/// triggering stream timeouts. Hadoop S3A defaults to 96; AWS SDK defaults to 50.
const S3_MAX_CONCURRENT_REQUESTS: usize = 64;

/// Maximum number of `ObjectStore` instances retained process-wide. Each entry holds
/// a `reqwest::Client` with its own connection + TLS session pool, so unbounded growth
/// over a long-running ES (months of distinct buckets / roles / endpoints) leaks
/// memory and sockets. 256 covers the typical case (one per bucket/config) with
/// headroom for many partitioned tables; matches the footer-metadata cache.
const MAX_STORES: usize = 256;

/// Process-wide LRU cache of `ObjectStore` instances keyed by bucket+config identity.
///
/// Building a new `AmazonS3` (or equivalent) creates a fresh `reqwest::Client` with
/// an empty connection pool. Sharing one store instance across all splits of a query —
/// and across queries — means the connection pool (including warm TLS sessions) is
/// reused rather than rebuilt for every split, eliminating repeated TLS handshakes to
/// the same S3 endpoint.
///
/// Backed by `lru::LruCache` (intrusive doubly-linked list + hash map): `get` promotes
/// to MRU in O(1); `push` evicts the LRU entry in O(1) when at capacity. Eviction is
/// safe: the evicted `Arc<dyn ObjectStore>` is dropped, but any in-flight workers still
/// holding `Arc::clone`s keep the underlying `reqwest::Client` (and its connection pool)
/// alive until they finish; only when the last reference drops do idle TCP/TLS sessions
/// close.
static STORE_CACHE: LazyLock<Mutex<LruCache<String, Arc<dyn ObjectStore>>>> =
    LazyLock::new(|| {
        Mutex::new(LruCache::new(
            NonZeroUsize::new(MAX_STORES).expect("MAX_STORES > 0"),
        ))
    });

/// Returns the cache key for a remote store: `scheme://bucket-or-host[/container]|connection_key`.
///
/// The path component (object key) is excluded so that all objects in the same bucket
/// share one store entry. For Azure the container name is included because
/// `MicrosoftAzureBuilder` is per-container; the key is normalized so that path-style
/// and Hadoop-style Azure URLs targeting the same account+container collapse to the
/// same entry.
fn store_cache_key(uri: &str, config: &StorageConfig) -> String {
    let prefix = if uri.starts_with("az://") || uri.starts_with("abfss://") || uri.starts_with("wasbs://") {
        match parse_azure_url(uri) {
            Ok((account, container, _)) => {
                let scheme_end = uri.find("://").map(|i| i + 3).unwrap_or(0);
                let scheme_with_sep = &uri[..scheme_end];
                let account = account.unwrap_or_default();
                format!("{}{}/{}", scheme_with_sep, account, container)
            }
            Err(_) => uri.to_string(),
        }
    } else {
        // S3, GCS, HTTP: host (bucket) only.
        let after_scheme = uri.find("://").map(|i| i + 3).unwrap_or(0);
        let rest = &uri[after_scheme..];
        let prefix_len = rest.find('/').unwrap_or(rest.len());
        uri[..after_scheme + prefix_len].to_string()
    };

    format!("{}|{}", prefix, config.connection_key())
}

/// Returns a cached or newly built store for remote URIs; local paths always get a fresh store.
///
/// Two-phase lookup-then-insert: the (potentially slow) builder runs OUTSIDE the cache
/// lock so concurrent store lookups for unrelated buckets aren't serialized behind it.
/// On the rare race where two threads miss the same key and both build, the recheck-
/// under-lock path returns the first writer's store and drops the loser's — the cache
/// always holds at most one entry per key, just like the previous `entry().or_insert(_)`
/// behavior.
fn get_or_build_store(
    uri: &str,
    config: &StorageConfig,
    build: impl FnOnce() -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>> {
    let path = object_path_from_uri(uri)?;
    let key = store_cache_key(uri, config);

    {
        // `LruCache::get` promotes to MRU and so requires `&mut self`.
        let mut guard = STORE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(store) = guard.get(&key) {
            return Ok((Arc::clone(store), path));
        }
    }

    let store = build()?;
    let mut guard = STORE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = guard.get(&key) {
        // Race: a peer inserted the same key while we were building. Use theirs;
        // our freshly built store will be dropped at end of scope.
        return Ok((Arc::clone(existing), path));
    }
    // `push` returns the evicted (key, value) only when we're at capacity AND the
    // key was new to the cache (always true here, because the recheck above passed).
    if let Some((evicted_key, _evicted_store)) = guard.push(key, Arc::clone(&store)) {
        log::debug!(
            target: "esql_parquet_rs::store_cache",
            "evicted LRU store entry [{}] (cache_len={}, cap={})",
            evicted_key, guard.len(), MAX_STORES
        );
    }
    Ok((store, path))
}

/// Extracts the object path portion from a URI (everything after scheme://host[/container]/).
fn object_path_from_uri(uri: &str) -> Result<object_store::path::Path, Box<dyn std::error::Error + Send + Sync>> {
    if uri.starts_with("s3://") || uri.starts_with("gs://") {
        let url = Url::parse(uri)?;
        let key = url.path().trim_start_matches('/');
        return Ok(object_store::path::Path::from(key));
    }
    if uri.starts_with("az://") || uri.starts_with("abfss://") || uri.starts_with("wasbs://") {
        let (_, _, key) = parse_azure_url(uri)?;
        return Ok(object_store::path::Path::from(key));
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
        let url = Url::parse(uri)?;
        let key = url.path().trim_start_matches('/');
        return Ok(object_store::path::Path::from(key));
    }
    // Local
    let normalized = uri.strip_prefix('/').unwrap_or(uri);
    Ok(object_store::path::Path::from(normalized))
}

/// Parses an Azure URL in either path-style or Hadoop-style form.
///
/// Path-style (account in host, container as first path segment):
///   `wasbs://ACCOUNT.blob.core.windows.net/CONTAINER/key/...`
///
/// Hadoop-style (container in userinfo, account in host):
///   `wasbs://CONTAINER@ACCOUNT.blob.core.windows.net/key/...`
///
/// Returns `(account_from_url, container, key)`. The account is `None` only when the
/// host doesn't contain a leading label; callers may then fall back to config values.
fn parse_azure_url(
    uri: &str,
) -> Result<(Option<String>, String, String), Box<dyn std::error::Error + Send + Sync>> {
    let url = Url::parse(uri)?;
    let host = url.host_str().ok_or("missing host in Azure URL")?;
    let account = host
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let path = url.path().trim_start_matches('/');
    let username = url.username();

    if !username.is_empty() {
        Ok((account, username.to_string(), path.to_string()))
    } else {
        let (container, key) = path
            .split_once('/')
            .ok_or("missing container/key in Azure URL path")?;
        Ok((account, container.to_string(), key.to_string()))
    }
}

/// Resolves a URI into an ObjectStore + object path based on the scheme.
/// Remote stores (S3, GCS, Azure, HTTP) are cached process-wide to share connection pools.
pub fn resolve_store(
    uri: &str,
    config: &StorageConfig,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    if uri.starts_with("s3://") {
        get_or_build_store(uri, config, || build_s3(uri, config))
    } else if uri.starts_with("az://") || uri.starts_with("abfss://") || uri.starts_with("wasbs://") {
        get_or_build_store(uri, config, || build_azure(uri, config))
    } else if uri.starts_with("gs://") {
        get_or_build_store(uri, config, || build_gcs(uri, config))
    } else if uri.starts_with("http://") || uri.starts_with("https://") {
        get_or_build_store(uri, config, || build_http(uri))
    } else {
        resolve_local(uri)
    }
}

fn build_s3(
    uri: &str,
    config: &StorageConfig,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let bucket = url.host_str().ok_or("missing bucket in S3 URL")?;

    // HTTP/2 multiplexes all concurrent worker requests over a single persistent TCP
    // connection, eliminating per-connection DNS lookups and TLS handshakes that would
    // otherwise be needed when multiple row-group workers issue concurrent GET requests.
    let client_options = ClientOptions::new().with_allow_http2();

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_client_options(client_options);
    if let Some(v) = config.get("endpoint") {
        builder = builder.with_endpoint(v).with_allow_http(true);
    }

    // `auth = "none"` (case-insensitive) selects anonymous access for publicly readable
    // buckets and matches the Java S3StorageProvider's AnonymousCredentialsProvider path.
    // It must short-circuit the default AWS credential chain (env -> IMDS -> ...) which
    // would otherwise attempt PUT http://169.254.169.254/latest/api/token off-EC2 and
    // fail with "Generic S3 error" after exhausting retries.
    let anonymous = config.get("auth").is_some_and(|v| v.eq_ignore_ascii_case("none"));
    if anonymous {
        builder = builder.with_skip_signature(true);
    } else {
        if let Some(v) = config.get("access_key") {
            builder = builder.with_access_key_id(v);
        }
        if let Some(v) = config.get("secret_key") {
            builder = builder.with_secret_access_key(v);
        }
    }

    if let Some(v) = config.get("region") {
        builder = builder.with_region(v);
    } else if config.get("endpoint").is_some() {
        builder = builder.with_region("us-east-1");
    }

    Ok(Arc::new(LimitStore::new(builder.build()?, S3_MAX_CONCURRENT_REQUESTS)))
}

/// Parses `wasbs://` / `abfss://` / `az://` Azure URLs in both path-style and
/// Hadoop-style forms. See `parse_azure_url` for the supported grammars.
fn build_azure(
    uri: &str,
    config: &StorageConfig,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>>
{
    let (account_from_url, container, _key) = parse_azure_url(uri)?;

    let mut builder = MicrosoftAzureBuilder::new().with_container_name(&container);

    if let Some(v) = config.get("endpoint") {
        builder = builder.with_endpoint(v.to_string()).with_allow_http(true);
    }

    let account = config.get("account")
        .map(str::to_string)
        .or_else(|| config.get("account_name").map(str::to_string))
        .or(account_from_url);
    if let Some(v) = account {
        builder = builder.with_account(v);
    }

    if let Some(v) = config.get("key").or_else(|| config.get("account_key")) {
        builder = builder.with_access_key(v);
    }
    if let Some(v) = config.get("sas_token") {
        let pairs: Vec<(String, String)> = url::form_urlencoded::parse(v.as_bytes())
            .map(|(k, val)| (k.into_owned(), val.into_owned()))
            .collect();
        builder = builder.with_sas_authorization(pairs);
    }

    Ok(Arc::new(LimitStore::new(builder.build()?, S3_MAX_CONCURRENT_REQUESTS)))
}

fn build_gcs(
    uri: &str,
    config: &StorageConfig,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let bucket = url.host_str().ok_or("missing bucket in GCS URL")?;

    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    if let Some(v) = config.get("endpoint") {
        builder = builder.with_base_url(v);
    }
    if let Some(v) = config.get("credentials") {
        builder = builder.with_service_account_key(v);
    }

    Ok(Arc::new(LimitStore::new(builder.build()?, S3_MAX_CONCURRENT_REQUESTS)))
}

fn build_http(
    uri: &str,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let host = url.host_str().ok_or("missing host in HTTP URL")?;
    let base = match url.port() {
        Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
        None => format!("{}://{}", url.scheme(), host),
    };
    let options = object_store::ClientOptions::new().with_allow_http(true);
    let store = HttpBuilder::new()
        .with_url(&base)
        .with_client_options(options)
        .build()?;
    Ok(Arc::new(LimitStore::new(store, S3_MAX_CONCURRENT_REQUESTS)))
}

fn resolve_local(
    path: &str,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    let local_fs = LocalFileSystem::new_with_prefix("/")?;
    let normalized = path.strip_prefix('/').unwrap_or(path);
    Ok((Arc::new(local_fs), object_store::path::Path::from(normalized)))
}
