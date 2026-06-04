/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * StorageProvider implementation for Azure Blob Storage.
 * <p>
 * Supports the {@code wasbs://} and {@code wasb://} URI schemes in two equivalent forms:
 * <ul>
 *   <li><b>Path-style</b>: {@code wasbs://<account>.blob.core.windows.net/<container>/<blob>}
 *       — host carries the account; the first path segment is the container.</li>
 *   <li><b>Hadoop/Spark form</b>: {@code wasbs://<container>@<account>.blob.core.windows.net/<blob>}
 *       — userInfo carries the container; the path is the blob name. This is the canonical
 *       form documented at https://hadoop.apache.org/docs/stable/hadoop-azure/index.html
 *       and emitted by Azure Open Datasets.</li>
 * </ul>
 * In both forms the account name is extracted as the first dot-segment of the host.
 * <p>
 * Maintains both a sync {@link BlobServiceClient} and an async {@link BlobServiceAsyncClient},
 * built from the same {@link BlobServiceClientBuilder} (same pattern as {@code repository-azure}'s
 * {@code AzureClientProvider}). Both clients share credentials, endpoint, and HTTP pipeline config.
 * <ul>
 *   <li><b>Sync client</b> — used for streaming reads ({@code openInputStream} returns a live
 *       {@code InputStream}), metadata ({@code getProperties}), existence checks, and listing.
 *       These are inherently blocking or return streaming results.</li>
 *   <li><b>Async client</b> — used for {@code readBytesAsync} range reads in
 *       {@link AzureStorageObject} via {@code BlobAsyncClient.downloadWithResponse}. Reactor
 *       Netty handles concurrent range reads without blocking a thread per request.</li>
 * </ul>
 * <p>
 * The async dependencies ({@code azure-core-http-netty}, Reactor Netty, Netty) are already
 * bundled in this plugin's classloader. Versions are aligned with {@code repository-azure}.
 * <p>
 * Authentication via connection string, account+key, SAS token, {@code auth=none} for public
 * containers, or {@code auth=workload_identity}. The workload-identity chain checks AKS Workload
 * Identity first (federated token file at the entitled symlink under {@code ${ES_PATH_CONF}}) and
 * falls back to {@code ManagedIdentityCredential} via Azure IMDS.
 * {@code DefaultAzureCredential} is excluded: it bundles file-reading and process-spawning
 * credential sources blocked by entitlements.
 */
public final class AzureStorageProvider implements StorageProvider {

    private static final Logger LOGGER = LogManager.getLogger(AzureStorageProvider.class);

    /** Operator-managed AKS Workload Identity token symlink, relative to {@code ${ES_PATH_CONF}}. */
    public static final String AKS_FEDERATED_TOKEN_FILE_LOCATION = "esql-datasource-azure/azure-federated-token";

    /**
     * Test-only system property that disables Microsoft Entra instance discovery on the Azure SDK
     * credential builders. Without this, the SDK probes the real Microsoft Entra discovery
     * endpoint at startup to validate the configured authority/tenant, which hangs in offline
     * test environments where {@code AZURE_AUTHORITY_HOST} is redirected to a local fixture.
     * Mirrors the same property name used by {@code repository-azure}'s {@code AzureClientProvider}
     * so a single test-cluster system property toggles both surfaces consistently.
     */
    private static final boolean DISABLE_INSTANCE_DISCOVERY = Booleans.parseBoolean(
        System.getProperty("tests.azure.credentials.disable_instance_discovery", "false")
    );

    private record Clients(BlobServiceClient sync, BlobServiceAsyncClient async) {}

    private volatile Clients clients;
    private final AzureConfiguration config;
    private final Environment environment;

    /**
     * Test-friendly constructor: equivalent to production behavior on a node where AKS Workload
     * Identity is not configured (no {@link Environment} means we cannot resolve the entitled
     * federated-token path, so the workload-identity chain falls back to {@code ManagedIdentity}
     * only).
     */
    public AzureStorageProvider(AzureConfiguration config) {
        this(config, null);
    }

    public AzureStorageProvider(AzureConfiguration config, Environment environment) {
        this.config = config;
        this.environment = environment;
        if (config != null && (config.hasCredentials() || config.isAnonymous())) {
            BlobServiceClientBuilder builder = configureBlobServiceClientBuilder(config, null);
            this.clients = new Clients(builder.buildClient(), builder.buildAsyncClient());
        }
    }

    /**
     * Constructor for testing with a pre-built BlobServiceClient.
     */
    public AzureStorageProvider(BlobServiceClient blobServiceClient) {
        this.config = null;
        this.environment = null;
        this.clients = new Clients(blobServiceClient, null);
    }

    private Clients clients(String accountFromPath) {
        Clients c = clients;
        if (c != null) {
            return c;
        }
        synchronized (this) {
            if (clients == null) {
                BlobServiceClientBuilder builder = configureBlobServiceClientBuilder(config, accountFromPath);
                clients = new Clients(builder.buildClient(), builder.buildAsyncClient());
            }
        }
        return clients;
    }

    /**
     * Builds an AKS Workload Identity credential when all three preconditions hold:
     * <ol>
     *   <li>{@link Environment} is available (i.e. the plugin's {@code createComponents} ran),</li>
     *   <li>the AKS env triple ({@code AZURE_FEDERATED_TOKEN_FILE}, {@code AZURE_CLIENT_ID},
     *       {@code AZURE_TENANT_ID}) is present, and</li>
     *   <li>the entitled federated-token symlink at
     *       {@code ${ES_PATH_CONF}/esql-datasource-azure/azure-federated-token} exists and is
     *       readable.</li>
     * </ol>
     * Returns {@code null} otherwise so the caller can fall back to {@code ManagedIdentity}-only.
     *
     * <p>The K8s-injected path in {@code AZURE_FEDERATED_TOKEN_FILE} is ignored on purpose: it
     * lives outside the entitlement-allowlisted area and would be blocked at runtime. Operators
     * are expected to symlink the K8s-managed token to the entitled location.
     */
    private TokenCredential maybeBuildAksWorkloadIdentityCredential() {
        return maybeBuildAksWorkloadIdentityCredential(System::getenv);
    }

    /**
     * Test seam: env-var lookups are routed through {@code envLookup} so unit tests can inject a
     * stub map without manipulating real {@code System.getenv} state.
     */
    TokenCredential maybeBuildAksWorkloadIdentityCredential(Function<String, String> envLookup) {
        if (environment == null) {
            return null;
        }
        String federatedTokenEnvVar = envLookup.apply("AZURE_FEDERATED_TOKEN_FILE");
        String clientId = envLookup.apply("AZURE_CLIENT_ID");
        String tenantId = envLookup.apply("AZURE_TENANT_ID");
        if (Strings.hasText(federatedTokenEnvVar) == false || Strings.hasText(clientId) == false || Strings.hasText(tenantId) == false) {
            return null;
        }
        Path tokenPath = environment.configDir().resolve(AKS_FEDERATED_TOKEN_FILE_LOCATION);
        if (Files.exists(tokenPath) == false) {
            LOGGER.warn(
                "AKS Workload Identity env triple is set (AZURE_FEDERATED_TOKEN_FILE=[{}]) but the entitled symlink [{}] is missing; "
                    + "falling back to ManagedIdentityCredential",
                federatedTokenEnvVar,
                tokenPath
            );
            return null;
        }
        if (Files.isReadable(tokenPath) == false) {
            LOGGER.warn(
                "AKS Workload Identity entitled symlink [{}] exists but is not readable; falling back to ManagedIdentityCredential",
                tokenPath
            );
            return null;
        }
        WorkloadIdentityCredentialBuilder workloadIdentityBuilder = new WorkloadIdentityCredentialBuilder().clientId(clientId)
            .tenantId(tenantId)
            .tokenFilePath(tokenPath.toString());
        if (DISABLE_INSTANCE_DISCOVERY) {
            workloadIdentityBuilder.disableInstanceDiscovery();
        }
        return workloadIdentityBuilder.build();
    }

    private BlobServiceClientBuilder configureBlobServiceClientBuilder(AzureConfiguration config, String accountFromPath) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

        if (config != null && config.isAnonymous()) {
            String account = accountFromPath;
            if (account == null && config.account() != null) {
                account = config.account();
            }
            if (config.endpoint() != null && config.endpoint().isEmpty() == false) {
                builder.endpoint(config.endpoint());
            } else if (account != null) {
                builder.endpoint("https://" + account + ".blob.core.windows.net");
            } else {
                throw new IllegalStateException(
                    "Anonymous Azure access requires an endpoint or account from the path "
                        + "(wasbs://account.blob.core.windows.net/...) or WITH (endpoint = '...')"
                );
            }
        } else if (config != null && config.hasCredentials()) {
            if (Strings.hasText(config.connectionString())) {
                builder.connectionString(config.connectionString());
                if (config.endpoint() != null && config.endpoint().isEmpty() == false) {
                    builder.endpoint(config.endpoint());
                }
            } else if (Strings.hasText(config.account()) && Strings.hasText(config.key())) {
                StorageSharedKeyCredential credential = new StorageSharedKeyCredential(config.account(), config.key());
                String endpoint = config.endpoint();
                if (endpoint == null || endpoint.isEmpty()) {
                    endpoint = "https://" + config.account() + ".blob.core.windows.net";
                }
                builder.endpoint(endpoint).credential(credential);
            } else if (Strings.hasText(config.sasToken()) && Strings.hasText(config.account())) {
                String endpoint = config.endpoint();
                if (endpoint == null || endpoint.isEmpty()) {
                    endpoint = "https://" + config.account() + ".blob.core.windows.net";
                }
                builder.endpoint(endpoint).sasToken(config.sasToken());
            } else {
                throw new IllegalStateException("Azure credentials require connection_string, (account + key), or (account + sas_token)");
            }
        } else if (config != null && config.isWorkloadIdentity()) {
            // Workload-identity selection (NOT a chain: only one of these makes sense at a time).
            //
            // 1. If the AKS Workload Identity env triple is present AND the entitled
            // federated-token symlink is readable, use WorkloadIdentityCredential pinned to
            // the entitled symlink. We deliberately ignore AZURE_FEDERATED_TOKEN_FILE so the
            // K8s-injected path stays out of the entitlement allowlist.
            //
            // Crucially we do NOT also add ManagedIdentityCredential here:
            // ManagedIdentityCredentialBuilder auto-detects AZURE_FEDERATED_TOKEN_FILE itself
            // and would re-enter the K8s path through its IdentityClient, hitting the
            // entitlement and surfacing as a misleading "Managed Identity authentication is
            // not available" error. WorkloadIdentityCredential already covers the AKS case.
            //
            // 2. Otherwise (no AKS env triple, no entitled symlink), fall back to
            // ManagedIdentityCredential — covers Azure IMDS, the v1 surface.
            //
            // EnvironmentCredential is intentionally excluded: it reads AZURE_CLIENT_* env vars,
            // which are a dev/CI convention and open a JVM-global-state override on production
            // nodes. DefaultAzureCredential is also excluded — it bundles file-reading and
            // process-spawning sources blocked by entitlements.
            TokenCredential workloadIdentity = maybeBuildAksWorkloadIdentityCredential();
            TokenCredential credential = workloadIdentity != null ? workloadIdentity : new ManagedIdentityCredentialBuilder().build();
            String endpoint = Strings.hasText(config.endpoint())
                ? config.endpoint()
                : (accountFromPath != null ? "https://" + accountFromPath + ".blob.core.windows.net" : null);
            if (endpoint == null && Strings.hasText(config.account())) {
                endpoint = "https://" + config.account() + ".blob.core.windows.net";
            }
            if (endpoint == null) {
                throw new IllegalStateException(
                    "auth=workload_identity requires an account from the path (wasbs://account.blob.core.windows.net/...) "
                        + "or WITH (endpoint = '...')"
                );
            }
            builder.endpoint(endpoint).credential(credential);
        } else {
            throw new IllegalArgumentException(
                "Azure data source requires credentials: provide WITH (connection_string = '...'), "
                    + "WITH (account = '...', key = '...'), WITH (account = '...', sas_token = '...'), "
                    + "WITH (auth = 'none') for public containers, "
                    + "or WITH (auth = 'workload_identity') to use the node's managed identity (requires cluster setting)"
            );
        }

        return builder;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        Clients c = clients(account);
        BlobClient blobClient = c.sync().getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        BlobAsyncClient blobAsyncClient = resolveAsyncClient(c, parsed);
        return new AzureStorageObject(blobClient, blobAsyncClient, parsed.container, parsed.blobName, path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        Clients c = clients(account);
        BlobClient blobClient = c.sync().getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        BlobAsyncClient blobAsyncClient = resolveAsyncClient(c, parsed);
        return new AzureStorageObject(blobClient, blobAsyncClient, parsed.container, parsed.blobName, path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        Clients c = clients(account);
        BlobClient blobClient = c.sync().getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        BlobAsyncClient blobAsyncClient = resolveAsyncClient(c, parsed);
        return new AzureStorageObject(blobClient, blobAsyncClient, parsed.container, parsed.blobName, path, length, lastModified);
    }

    private static BlobAsyncClient resolveAsyncClient(Clients c, ParsedPath parsed) {
        BlobServiceAsyncClient async = c.async();
        if (async == null) {
            return null;
        }
        return async.getBlobContainerAsyncClient(parsed.container).getBlobAsyncClient(parsed.blobName);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        validateAzureScheme(prefix);
        ParsedPath parsed = parsePathForListing(prefix);
        String account = extractAccountFromHost(parsed.host);
        BlobContainerClient containerClient = clients(account).sync().getBlobContainerClient(parsed.container);
        ListBlobsOptions options = new ListBlobsOptions().setPrefix(parsed.blobName);
        Iterable<BlobItem> blobItems = recursive
            ? containerClient.listBlobs(options, null)
            : containerClient.listBlobsByHierarchy("/", options, null);
        return new AzureStorageIterator(blobItems, prefix, parsed.container);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateAzureScheme(path);
        ParsedPath parsed = parsePath(path);
        String account = extractAccountFromHost(parsed.host);
        BlobClient blobClient = clients(account).sync().getBlobContainerClient(parsed.container).getBlobClient(parsed.blobName);
        try {
            return blobClient.exists();
        } catch (Exception e) {
            if (e instanceof BlobStorageException bse && bse.getStatusCode() == 403) {
                return existsViaRangeGet(blobClient, path);
            }
            throw new IOException("Failed to check existence of " + path + credentialHint(), e);
        }
    }

    private boolean existsViaRangeGet(BlobClient blobClient, StoragePath path) throws IOException {
        try (var stream = blobClient.openInputStream(new BlobRange(0, 1L), null)) {
            return true;
        } catch (Exception e) {
            if (e instanceof BlobStorageException bse && bse.getStatusCode() == 404) {
                return false;
            }
            throw new IOException("Failed to check existence of " + path + " (properties denied, range GET also failed)", e);
        }
    }

    private String credentialHint() {
        if (config == null || (config.isAnonymous() == false && config.hasCredentials() == false)) {
            return ". If accessing a public container, use WITH (auth = 'none'). "
                + "Otherwise, provide credentials via WITH (account = '...', key = '...') or set Azure environment variables";
        }
        return "";
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("wasbs", "wasb");
    }

    @Override
    public void close() throws IOException {
        Clients c = clients;
        clients = null;
        if (c != null) {
            try {
                closeHttpClient(c.sync().getHttpPipeline().getHttpClient());
            } catch (Exception e) {
                throw new IOException("Failed to close Azure BlobServiceClient", e);
            } finally {
                if (c.async() != null) {
                    try {
                        closeHttpClient(c.async().getHttpPipeline().getHttpClient());
                    } catch (Exception e) {
                        throw new IOException("Failed to close Azure BlobServiceAsyncClient", e);
                    }
                }
            }
        }
    }

    /**
     * Close the underlying HttpClient if it implements AutoCloseable.
     * BlobServiceClient does not implement Closeable; the default NettyAsyncHttpClient
     * does not either, so this is typically a no-op. We attempt close when possible so
     * that custom HttpClient implementations or future SDK versions that support close
     * will be properly cleaned up. For full resource cleanup (like repository-azure),
     * the client would need to be built with a custom ConnectionProvider.
     */
    private static void closeHttpClient(com.azure.core.http.HttpClient httpClient) throws Exception {
        if (httpClient instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private static void validateAzureScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("wasbs") == false && scheme.equals("wasb") == false) {
            throw new IllegalArgumentException("AzureStorageProvider only supports wasbs:// and wasb:// schemes, got: " + scheme);
        }
    }

    private static String extractAccountFromHost(String host) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Invalid Azure host: " + host);
        }
        int dot = host.indexOf('.');
        return dot > 0 ? host.substring(0, dot) : host;
    }

    /**
     * Parse path for object access. Accepts two equivalent forms:
     * <ul>
     *   <li>Path-style {@code wasbs://account.blob.core.windows.net/container/blob} — host is
     *       {@code account.blob.core.windows.net}, container is the first path segment.</li>
     *   <li>Hadoop form {@code wasbs://container@account.blob.core.windows.net/blob} — userInfo
     *       carries the container; the entire path is the blob name.</li>
     * </ul>
     */
    static ParsedPath parsePath(StoragePath path) {
        String host = path.host();
        String pathStr = path.path();
        if (pathStr.startsWith(StoragePath.PATH_SEPARATOR)) {
            pathStr = pathStr.substring(1);
        }
        String userInfo = path.userInfo();
        if (userInfo != null) {
            // Hadoop form: container is in userInfo, blob name is the entire path (may be empty for root listing).
            return new ParsedPath(host, userInfo, pathStr);
        }
        if (pathStr.isEmpty()) {
            throw new IllegalArgumentException("Invalid Azure path: container and blob name required: " + path);
        }
        int firstSlash = pathStr.indexOf(StoragePath.PATH_SEPARATOR);
        String container;
        String blobName;
        if (firstSlash < 0) {
            container = pathStr;
            blobName = "";
        } else {
            container = pathStr.substring(0, firstSlash);
            blobName = pathStr.substring(firstSlash + 1);
        }
        if (container.isEmpty()) {
            throw new IllegalArgumentException("Invalid Azure path: container is required: " + path);
        }
        return new ParsedPath(host, container, blobName);
    }

    /**
     * Parse path for listing: prefix may end with / or a glob pattern.
     */
    private static ParsedPath parsePathForListing(StoragePath path) {
        ParsedPath parsed = parsePath(path);
        String prefix = parsed.blobName;
        if (prefix.isEmpty() == false && prefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            prefix += StoragePath.PATH_SEPARATOR;
        }
        return new ParsedPath(parsed.host, parsed.container, prefix);
    }

    record ParsedPath(String host, String container, String blobName) {}

    // Package-private (rather than private) so AzureStorageProviderTests can construct one
    // directly from a synthetic Iterable<BlobItem> and verify URI-form preservation without
    // standing up a real BlobServiceClient.
    static final class AzureStorageIterator implements StorageIterator {
        private final Iterable<BlobItem> blobItems;
        private final StoragePath basePath;
        private final String container;
        private final String scheme;
        private final String userInfo;

        private Iterator<BlobItem> iterator;
        private BlobItem current;

        AzureStorageIterator(Iterable<BlobItem> blobItems, StoragePath basePath, String container) {
            this.blobItems = blobItems;
            this.basePath = basePath;
            this.container = container;
            this.scheme = basePath.scheme();
            this.userInfo = basePath.userInfo();
        }

        @Override
        public boolean hasNext() {
            try {
                if (iterator == null) {
                    iterator = blobItems.iterator();
                }
                if (current != null) {
                    return true;
                }
                while (iterator.hasNext()) {
                    BlobItem item = iterator.next();
                    if (Boolean.TRUE.equals(item.isPrefix())) {
                        continue;
                    }
                    String name = item.getName();
                    if (name != null && name.endsWith(StoragePath.PATH_SEPARATOR) == false) {
                        current = item;
                        return true;
                    }
                }
                return false;
            } catch (Exception e) {
                String msg = (e instanceof BlobStorageException bse && bse.getStatusCode() == 403)
                    ? "Access denied listing blobs in container ["
                        + container
                        + "]. "
                        + "Verify that the configured credentials have listing permission on this container, "
                        + "or use exact file paths instead of glob patterns."
                    : "Failed to list blobs in container [" + container + "]";
                throw new RuntimeException(msg, e);
            }
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            BlobItem item = current;
            current = null;
            // Preserve the input URI form: Hadoop form (container@host) emits
            // scheme://container@host/blob; path-style emits scheme://host/container/blob.
            String name = item.getName();
            StringBuilder fullPath = new StringBuilder().append(scheme).append(StoragePath.SCHEME_SEPARATOR);
            if (userInfo != null) {
                fullPath.append(userInfo).append('@').append(basePath.host()).append(StoragePath.PATH_SEPARATOR);
            } else {
                fullPath.append(basePath.host()).append(StoragePath.PATH_SEPARATOR).append(container).append(StoragePath.PATH_SEPARATOR);
            }
            fullPath.append(name);
            StoragePath objectPath = StoragePath.of(fullPath.toString());
            Instant lastModified = item.getProperties() != null && item.getProperties().getLastModified() != null
                ? item.getProperties().getLastModified().toInstant()
                : null;
            long size = item.getProperties() != null && item.getProperties().getContentLength() != null
                ? item.getProperties().getContentLength()
                : 0L;
            return new StorageEntry(objectPath, size, lastModified);
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }
    }
}
