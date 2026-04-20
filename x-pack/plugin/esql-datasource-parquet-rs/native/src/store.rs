use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
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
}

/// Returns true if the URI targets Azure Blob Storage, which does not support
/// suffix range requests (`bytes=-N`). Callers should pre-fetch the file size
/// via HEAD and pass it to `ParquetObjectReader::with_file_size` so that the
/// reader emits bounded range requests instead.
pub fn needs_file_size_hint(uri: &str) -> bool {
    uri.starts_with("az://") || uri.starts_with("abfss://") || uri.starts_with("wasbs://")
}

/// Resolves a URI into an ObjectStore + object path based on the scheme.
pub fn resolve_store(
    uri: &str,
    config: &StorageConfig,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    if uri.starts_with("s3://") {
        resolve_s3(uri, config)
    } else if uri.starts_with("az://") || uri.starts_with("abfss://") || uri.starts_with("wasbs://") {
        resolve_azure(uri, config)
    } else if uri.starts_with("gs://") {
        resolve_gcs(uri, config)
    } else if uri.starts_with("http://") || uri.starts_with("https://") {
        resolve_http(uri)
    } else {
        resolve_local(uri)
    }
}

fn resolve_s3(
    uri: &str,
    config: &StorageConfig,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let bucket = url.host_str().ok_or("missing bucket in S3 URL")?;
    let key = url.path().trim_start_matches('/');

    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
    if let Some(v) = config.get("endpoint") {
        builder = builder.with_endpoint(v).with_allow_http(true);
    }
    if let Some(v) = config.get("access_key") {
        builder = builder.with_access_key_id(v);
    }
    if let Some(v) = config.get("secret_key") {
        builder = builder.with_secret_access_key(v);
    }
    if let Some(v) = config.get("region") {
        builder = builder.with_region(v);
    } else if config.get("endpoint").is_some() {
        builder = builder.with_region("us-east-1");
    }

    Ok((Arc::new(builder.build()?), object_store::path::Path::from(key)))
}

/// Parses `wasbs://` / `abfss://` / `az://` Azure URLs.
/// Format: `wasbs://ACCOUNT.blob.core.windows.net/CONTAINER/path/to/object`
fn resolve_azure(
    uri: &str,
    config: &StorageConfig,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let host = url.host_str().ok_or("missing host in Azure URL")?;

    let account_from_url = host.split('.').next().filter(|s| s.is_empty() == false);

    let path = url.path().trim_start_matches('/');
    let (container, key) = path.split_once('/')
        .ok_or("missing container/key in Azure URL path")?;

    let mut builder = MicrosoftAzureBuilder::new().with_container_name(container);

    if let Some(v) = config.get("endpoint") {
        builder = builder.with_endpoint(v.to_string()).with_allow_http(true);
    }

    let account = config.get("account")
        .or_else(|| config.get("account_name"))
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

    Ok((Arc::new(builder.build()?), object_store::path::Path::from(key)))
}

fn resolve_gcs(
    uri: &str,
    config: &StorageConfig,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let bucket = url.host_str().ok_or("missing bucket in GCS URL")?;
    let key = url.path().trim_start_matches('/');

    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    if let Some(v) = config.get("endpoint") {
        builder = builder.with_base_url(v);
    }
    if let Some(v) = config.get("credentials") {
        builder = builder.with_service_account_key(v);
    }

    Ok((Arc::new(builder.build()?), object_store::path::Path::from(key)))
}

fn resolve_http(
    uri: &str,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    let url = Url::parse(uri)?;
    let host = url.host_str().ok_or("missing host in HTTP URL")?;
    let base = match url.port() {
        Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
        None => format!("{}://{}", url.scheme(), host),
    };
    let key = url.path().trim_start_matches('/');
    let options = object_store::ClientOptions::new().with_allow_http(true);
    let store = HttpBuilder::new()
        .with_url(&base)
        .with_client_options(options)
        .build()?;
    Ok((Arc::new(store), object_store::path::Path::from(key)))
}

fn resolve_local(
    path: &str,
) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), Box<dyn std::error::Error + Send + Sync>>
{
    let local_fs = LocalFileSystem::new_with_prefix("/")?;
    let normalized = path.strip_prefix('/').unwrap_or(path);
    Ok((Arc::new(local_fs), object_store::path::Path::from(normalized)))
}
