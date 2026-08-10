/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import reactor.netty.resources.ConnectionProvider;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientAssertionCredentialBuilder;
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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.env.Environment;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceConfiguration;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
 * Authentication (selected by the {@code auth} mode; {@code auto} infers it from the fields present):
 * {@code auth=static_credentials} (connection string, account+key, or account+SAS token),
 * {@code auth=federated_identity} ({@code tenant_id} + {@code client_id} + {@code jwt_audience}) which mints a JWT
 * via the node's workload-identity issuer and exchanges it through Azure AD as a client assertion,
 * {@code auth=anonymous} for public containers, or {@code auth=managed_identity} (AKS Workload Identity via the
 * entitled federated-token symlink under {@code ${ES_PATH_CONF}} when configured, falling back to
 * {@code ManagedIdentityCredential} via Azure IMDS).
 * {@code DefaultAzureCredential} is excluded entirely: it bundles file-reading and process-spawning
 * credential sources blocked by entitlements.
 */
public final class AzureStorageProvider implements StorageProvider {

    /** Operator-managed AKS Workload Identity token symlink, relative to {@code ${ES_PATH_CONF}}. */
    public static final String AKS_FEDERATED_TOKEN_FILE_LOCATION = "esql-datasource-azure/azure-federated-token";
    private static final String DEFAULT_JWT_AUDIENCE = "api://AzureADTokenExchange";

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

    /**
     * Generous acquisition timeout so brief pool contention QUEUES rather than fails a read, mirroring the S3
     * client's {@code connectionAcquisitionTimeout}.
     */
    private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);

    private record Clients(BlobServiceClient sync, BlobServiceAsyncClient async) {}

    private volatile Clients clients;

    /**
     * The reactor-netty connection pool backing both clients' HTTP transport, sized by {@link #maxConnections}.
     * Owned by this provider and disposed in {@link #close()}. Null on the pre-built-client test constructor and
     * until the (eager or lazy) client build runs.
     */
    private volatile ConnectionProvider connectionProvider;
    private final AzureConfiguration config;
    private final Environment environment;

    /**
     * Data-source pool used by the federated-auth (keyless) credential (see {@link #buildClientAssertionCredential}).
     * Non-null only on federated code paths.
     */
    private final ExecutorService executor;

    /**
     * Sizes the reactor-netty connection pool shared by the sync and async clients (see {@link #buildSizedHttpClient}).
     * The value of the {@code esql.external.max_concurrent_requests} node setting.
     */
    private final int maxConnections;

    /**
     * Convenience constructor that sizes the connection pool at the {@code esql.external.max_concurrent_requests}
     * default. Used by tests; production uses the four-argument form with the node-setting value.
     */
    public AzureStorageProvider(AzureConfiguration config, Environment environment, ExecutorService executor) {
        this(config, environment, executor, ExternalSourceSettings.blobStoreConcurrency(Settings.EMPTY));
    }

    /**
     * Production constructor. {@code maxConnections} sizes the reactor-netty connection pool and is the value of the
     * {@code esql.external.max_concurrent_requests} node setting, read at the plugin's construction path (which holds
     * node {@link Settings}).
     */
    public AzureStorageProvider(AzureConfiguration config, Environment environment, ExecutorService executor, int maxConnections) {
        this.config = config;
        this.environment = environment;
        this.executor = executor;
        this.maxConnections = maxConnections;
        // Build the client eagerly so misconfigurations are caught early — except auth=managed_identity, whose
        // endpoint can be derived from the per-query wasbs://<account>... path (unavailable at construction), so it
        // is deferred to first use like on the pre-refactor path. With no configuration (config is null), also defer.
        if (config != null && config.resolveAuthMode() != FileDataSourceConfiguration.AuthMode.MANAGED_IDENTITY) {
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
        this.executor = null;
        this.maxConnections = ExternalSourceSettings.blobStoreConcurrency(Settings.EMPTY);
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
     * Returns {@code null} when the {@link Environment} or the AKS env triple is absent, so the
     * caller can fall back to {@code ManagedIdentity}-only (the plain Azure VM / IMDS case).
     * Throws {@link IllegalStateException} when the AKS env triple is present but the entitled
     * symlink is missing or unreadable: the env triple means the operator deliberately enabled AKS
     * Workload Identity, so a misconfigured token fails loudly rather than silently degrading to a
     * different identity (mirrors the esql-datasource-s3 IRSA provider).
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
        // The AKS env triple is the AKS Workload Identity webhook's signature: its presence means the
        // operator deliberately wired up workload identity. A missing or unreadable entitled symlink
        // is therefore a misconfiguration. Fail loudly rather than returning null and silently
        // degrading to ManagedIdentityCredential, which auto-detects AZURE_FEDERATED_TOKEN_FILE and
        // re-enters the entitlement-blocked K8s path, surfacing as a misleading "Managed Identity
        // not available" error. (Mirrors the esql-datasource-s3 IRSA provider's hard-fail.)
        Path tokenPath = environment.configDir().resolve(AKS_FEDERATED_TOKEN_FILE_LOCATION);
        if (Files.exists(tokenPath) == false) {
            throw new IllegalStateException(
                Strings.format(
                    "Cannot use AKS Workload Identity: the AKS env triple is set (AZURE_FEDERATED_TOKEN_FILE=[%s]) but the entitled "
                        + "symlink [%s] is missing. Create it to point at the projected service-account token.",
                    federatedTokenEnvVar,
                    tokenPath
                )
            );
        }
        if (Files.isReadable(tokenPath) == false) {
            throw new IllegalStateException(
                Strings.format(
                    "Cannot use AKS Workload Identity: the AKS env triple is set (AZURE_FEDERATED_TOKEN_FILE=[%s]) but the entitled "
                        + "symlink [%s] exists and is not readable.",
                    federatedTokenEnvVar,
                    tokenPath
                )
            );
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
        if (config == null) {
            // The datasource layer always builds a provider from a validated configuration.
            throw new IllegalArgumentException("Azure data source requires a configuration");
        }
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder().httpClient(buildSizedHttpClient());

        // Field inference happens only inside resolveAuthMode()'s auto branch; every explicit mode maps straight to
        // its case here. Within STATIC_CREDENTIALS the specific static form (connection_string / account+key /
        // account+sas) is assembled from whichever fields are present — that is form selection, not mode inference.
        // A switch expression yielding the configured builder keeps this exhaustive: a new AuthMode fails to compile.
        return switch (config.resolveAuthMode()) {
            case ANONYMOUS -> {
                String account = accountFromPath;
                if (account == null && config.account() != null) {
                    account = config.account();
                }
                if (config.endpoint() != null && config.endpoint().isEmpty() == false) {
                    builder.endpoint(config.endpoint());
                } else if (account != null) {
                    builder.endpoint(blobEndpoint(account));
                } else {
                    throw new IllegalStateException(
                        "Anonymous Azure access requires an endpoint or account from the path "
                            + "(wasbs://account.blob.core.windows.net/...) or the endpoint setting"
                    );
                }
                yield builder;
            }
            case STATIC_CREDENTIALS -> {
                if (Strings.hasText(config.connectionString())) {
                    builder.connectionString(config.connectionString());
                    if (config.endpoint() != null && config.endpoint().isEmpty() == false) {
                        builder.endpoint(config.endpoint());
                    }
                } else if (Strings.hasText(config.account()) && Strings.hasText(config.key())) {
                    StorageSharedKeyCredential credential = new StorageSharedKeyCredential(config.account(), config.key());
                    String endpoint = config.endpoint();
                    if (endpoint == null || endpoint.isEmpty()) {
                        endpoint = blobEndpoint(config.account());
                    }
                    builder.endpoint(endpoint).credential(credential);
                } else if (Strings.hasText(config.sasToken()) && Strings.hasText(config.account())) {
                    String endpoint = config.endpoint();
                    if (endpoint == null || endpoint.isEmpty()) {
                        endpoint = blobEndpoint(config.account());
                    }
                    builder.endpoint(endpoint).sasToken(config.sasToken());
                } else {
                    throw new IllegalStateException(
                        "Azure credentials require connection_string, (account + key), or (account + sas_token)"
                    );
                }
                yield builder;
            }
            case FEDERATED_IDENTITY -> {
                String account = accountFromPath;
                if (account == null && config.account() != null) {
                    account = config.account();
                }
                String endpoint = config.endpoint();
                if (endpoint == null || endpoint.isEmpty()) {
                    if (account == null) {
                        throw new IllegalStateException(
                            "Azure federated authentication requires an account from the path "
                                + "(wasbs://account.blob.core.windows.net/...) or the account setting"
                        );
                    }
                    endpoint = blobEndpoint(account);
                }
                builder.endpoint(endpoint).credential(buildClientAssertionCredential(config, executor));
                yield builder;
            }
            case MANAGED_IDENTITY -> {
                // Workload-identity selection (NOT a chain: only one of these makes sense at a time).
                //
                // 1. If the AKS Workload Identity env triple is present, use WorkloadIdentityCredential
                // pinned to the entitled symlink (maybeBuildAksWorkloadIdentityCredential hard-fails if
                // the env triple is set but the symlink is missing/unreadable). We deliberately ignore
                // AZURE_FEDERATED_TOKEN_FILE so the K8s-injected path stays out of the entitlement
                // allowlist.
                //
                // Crucially we do NOT also add ManagedIdentityCredential here:
                // ManagedIdentityCredentialBuilder auto-detects AZURE_FEDERATED_TOKEN_FILE itself
                // and would re-enter the K8s path through its IdentityClient, hitting the
                // entitlement and surfacing as a misleading "Managed Identity authentication is
                // not available" error. WorkloadIdentityCredential already covers the AKS case.
                //
                // 2. Otherwise (no AKS env triple at all) fall back to ManagedIdentityCredential —
                // covers Azure IMDS, the v1 surface.
                //
                // EnvironmentCredential is intentionally excluded: it reads AZURE_CLIENT_* env vars,
                // which are a dev/CI convention and open a JVM-global-state override on production
                // nodes. DefaultAzureCredential is also excluded — it bundles file-reading and
                // process-spawning sources blocked by entitlements.
                TokenCredential workloadIdentity = maybeBuildAksWorkloadIdentityCredential();
                TokenCredential credential = workloadIdentity != null ? workloadIdentity : new ManagedIdentityCredentialBuilder().build();
                String endpoint = Strings.hasText(config.endpoint())
                    ? config.endpoint()
                    : (accountFromPath != null ? blobEndpoint(accountFromPath) : null);
                if (endpoint == null && Strings.hasText(config.account())) {
                    endpoint = blobEndpoint(config.account());
                }
                if (endpoint == null) {
                    throw new IllegalStateException(
                        "auth=managed_identity requires an account from the path (wasbs://account.blob.core.windows.net/...) "
                            + "or the endpoint setting"
                    );
                }
                builder.endpoint(endpoint).credential(credential);
                yield builder;
            }
        };
    }

    /**
     * Builds the HTTP client shared by the sync and async Blob clients, backed by a reactor-netty connection pool
     * sized to {@link #maxConnections}. Records the pool in {@link #connectionProvider} so {@link #close()} disposes
     * it. The single {@code esql.external.max_concurrent_requests} knob sizes this pool; the circuit breaker bounds memory
     * and reactive 503 backoff handles throttling. The Azure SDK's reactor-netty default of 50 connections would cap
     * read parallelism far below what Azure Blob serves, so we size it explicitly here.
     */
    private com.azure.core.http.HttpClient buildSizedHttpClient() {
        ConnectionProvider provider = ConnectionProvider.builder("esql-datasource-azure")
            .maxConnections(maxConnections)
            .pendingAcquireMaxCount(-1)
            .pendingAcquireTimeout(CONNECTION_ACQUISITION_TIMEOUT)
            .build();
        this.connectionProvider = provider;
        return new NettyAsyncHttpClientBuilder(reactor.netty.http.client.HttpClient.create(provider)).build();
    }

    /**
     * Builds a {@link FederatedAssertionCredential} for federated (keyless) authentication: it presents a workload-identity JWT,
     * minted by the node's {@link WorkloadIdentityIssuerClient}, as the client assertion in the Azure AD
     * {@code client_credentials} grant. See {@link FederatedAssertionCredential} for how the asynchronous assertion
     * is bridged to the credential's synchronous supplier.
     *
     * <p>{@code executor} is pinned as MSAL's {@code executorService} so the token exchange completes on the
     * data-source pool rather than {@code ForkJoinPool.commonPool}; it is required.
     */
    static TokenCredential buildClientAssertionCredential(AzureConfiguration config, ExecutorService executor) {
        WorkloadIdentityIssuerClient issuerClient = WorkloadIdentityRegistry.getSharedIssuerClient();
        if (issuerClient.isEnabled() == false) {
            throw new IllegalStateException(
                "Azure federated authentication requires the workload-identity feature to be enabled on this node"
            );
        }
        if (executor == null) {
            // The federated (keyless) path always runs with the injected data-source executor; a null pool would let MSAL fall
            // back to ForkJoinPool.commonPool, which we deliberately keep token acquisition off of.
            throw new IllegalStateException("Azure federated authentication requires a non-null executor for token acquisition");
        }
        String jwtAudience = Strings.hasText(config.jwtAudience()) ? config.jwtAudience() : DEFAULT_JWT_AUDIENCE;
        // The synchronous clientAssertion supplier the delegate reads is wired by FederatedAssertionCredential itself;
        // we only configure the identity and the MSAL executor here.
        ClientAssertionCredentialBuilder delegateBuilder = new ClientAssertionCredentialBuilder().tenantId(config.tenantId())
            .clientId(config.clientId())
            .executorService(executor);
        return new FederatedAssertionCredential(delegateBuilder, () -> issueAssertionAsync(issuerClient, jwtAudience));
    }

    /**
     * Bridges the asynchronous {@link WorkloadIdentityIssuerClient#issueToken} listener API to the
     * {@link CompletableFuture} that {@link FederatedAssertionCredential} resolves.
     */
    static CompletableFuture<String> issueAssertionAsync(WorkloadIdentityIssuerClient issuerClient, String jwtAudience) {
        CompletableFuture<String> assertion = new CompletableFuture<>();
        issuerClient.issueToken(
            new WorkloadIdentityIssuerClient.IssueTokenRequest(jwtAudience),
            ActionListener.wrap(response -> assertion.complete(response.token()), assertion::completeExceptionally)
        );
        return assertion;
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
            throw new IOException(
                "Failed to check existence of " + path + " (properties denied, range GET also failed)" + credentialHint(),
                e
            );
        }
    }

    private String credentialHint() {
        if (config == null || config.resolveAuthModeOrNull() == null) {
            return ". If accessing a public container, set auth=anonymous. "
                + "Otherwise, provide credentials via account and key, "
                + "or configure federated authentication with tenant_id and client_id";
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
        ConnectionProvider cp = connectionProvider;
        connectionProvider = null;
        IOException primaryException = null;
        if (c != null) {
            try {
                closeHttpClient(c.sync().getHttpPipeline().getHttpClient());
            } catch (Exception e) {
                primaryException = new IOException("Failed to close Azure BlobServiceClient", e);
            }
            if (c.async() != null) {
                try {
                    closeHttpClient(c.async().getHttpPipeline().getHttpClient());
                } catch (Exception e) {
                    IOException asyncException = new IOException("Failed to close Azure BlobServiceAsyncClient", e);
                    if (primaryException != null) {
                        primaryException.addSuppressed(asyncException);
                    } else {
                        primaryException = asyncException;
                    }
                }
            }
        }
        // Dispose the connection pool last, after both clients have released their borrowed connections.
        if (cp != null) {
            try {
                cp.disposeLater().block(Duration.ofSeconds(5));
            } catch (Exception e) {
                IOException disposeException = new IOException("Failed to dispose Azure connection pool", e);
                if (primaryException != null) {
                    primaryException.addSuppressed(disposeException);
                } else {
                    primaryException = disposeException;
                }
            }
        }
        if (primaryException != null) {
            throw primaryException;
        }
    }

    /**
     * Close the underlying HttpClient if it implements AutoCloseable. The {@code NettyAsyncHttpClient} the SDK
     * builds is not {@code Closeable}, so this is typically a no-op — the pool it borrows from is the
     * {@link #connectionProvider}, which {@link #close()} disposes explicitly. We still attempt close so a custom
     * or future {@code AutoCloseable} HttpClient is cleaned up.
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

    /** Builds the default Blob service endpoint for an account: {@code https://<account>.blob.core.windows.net}. */
    private static String blobEndpoint(String account) {
        return "https://" + account + ".blob.core.windows.net";
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
            var props = item.getProperties();
            Instant lastModified = props != null && props.getLastModified() != null ? props.getLastModified().toInstant() : null;
            long size = props != null && props.getContentLength() != null ? props.getContentLength() : 0L;
            return new StorageEntry(objectPath, size, lastModified);
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }
    }
}
