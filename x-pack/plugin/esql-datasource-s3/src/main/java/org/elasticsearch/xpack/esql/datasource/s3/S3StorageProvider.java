/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import io.netty.channel.ChannelOption;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.StsAsyncClientBuilder;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.workload.identity.aws.AsyncWebIdentityCredentialsProvider;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.elasticsearch.xpack.esql.datasource.nettycommons.PooledRecvByteBufAllocator;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * StorageProvider implementation for S3 using AWS SDK v2.
 * <p>
 * Maintains both a sync {@link S3Client} (Apache HTTP) and an async {@link S3AsyncClient}
 * (Netty NIO, via {@code netty-nio-client} at {@code ${versions.awsv2sdk}}). The two clients
 * serve different purposes:
 * <ul>
 *   <li><b>Sync client</b> — used for streaming reads ({@code newStream} returns a live
 *       {@code ResponseInputStream} that reads on demand), metadata ({@code headObject}),
 *       existence checks, and object listing. These operations are inherently blocking or
 *       return streaming results that cannot be efficiently expressed as futures.</li>
 *   <li><b>Async client</b> — used exclusively for {@code readBytesAsync} range reads in
 *       {@link S3StorageObject}. When multiple concurrent range reads are dispatched, the Netty
 *       event loop handles them without blocking a thread per request, reducing thread-pool
 *       pressure under load. The pool is sized by {@code esql.external.max_connections}.</li>
 * </ul>
 * <p>
 * Both clients share the same credentials, region, and endpoint configuration. The Netty
 * jars are bundled with this plugin (classloader-isolated from the server and other plugins)
 * at {@code ${versions.netty}}, matching the pattern used by the inference plugin.
 */
public class S3StorageProvider implements StorageProvider {
    private static final Logger LOGGER = LogManager.getLogger(S3StorageProvider.class);
    private static final String DEFAULT_ROLE_SESSION_NAME = "elasticsearch-esql-datasource";

    private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final S3Configuration config;
    // Owned only on the keyless workload-identity path; null otherwise. Closed by close().
    private final StsAsyncClient stsAsyncClient;
    private final CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider;
    /**
     * Workload-identity credentials providers that this instance creates in
     * {@link #workloadIdentityProviders()} and therefore owns: the {@link ContainerCredentialsProvider}
     * and {@link InstanceProfileCredentialsProvider}, each of which opens a background credential-refresh
     * resource. Closed by {@link #close()}. The IRSA provider is excluded — it is a node-level singleton
     * owned by {@code S3DataSourcePlugin}.
     */
    private final List<SdkAutoCloseable> ownedWorkloadIdentityProviders = new ArrayList<>();

    /**
     * Test-friendly constructor: no IRSA web-identity provider available, async pool sized at the
     * {@code esql.external.max_connections} default. Equivalent to production behavior on a node where
     * {@code AWS_WEB_IDENTITY_TOKEN_FILE} is unset.
     */
    public S3StorageProvider(S3Configuration config) {
        this(config, null, ExternalSourceSettings.MAX_CONNECTIONS.get(Settings.EMPTY));
    }

    /**
     * Production constructor. {@code maxConnections} sizes the async client's Netty connection pool and is the
     * value of the {@code esql.external.max_connections} node setting, read at the plugin's construction path
     * (which holds node {@link Settings}).
     */
    @SuppressWarnings("this-escape")
    public S3StorageProvider(
        S3Configuration config,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider,
        int maxConnections
    ) {
        this.config = config;
        // Set first so that workloadIdentityProviders() (called via credentialsProvider() ->
        // buildWorkloadIdentityCredentialsProvider() on the auth=workload_identity path) can read it.
        this.webIdentityTokenCredentialsProvider = webIdentityTokenCredentialsProvider;
        final IdentityProvider<? extends AwsCredentialsIdentity> credentials;
        StsAsyncClient sts = null;
        S3Client s3 = null;
        boolean success = false;
        try {
            if (config != null && config.hasKeylessAuth()) {
                // Resolve the issuer client (and assert it is enabled) before allocating the STS client, so a
                // disabled-feature misconfiguration fails fast without leaking the STS client's Netty resources.
                WorkloadIdentityIssuerClient issuerClient = enabledWorkloadIdentityIssuerClient();
                // One STS async client and one credentials provider, shared by both S3 clients so a single token
                // cache and a single single-flight refresh back every request.
                sts = buildStsAsyncClient(config);
                credentials = buildWorkloadIdentityCredentialsProvider(config, issuerClient, sts);
            } else {
                // auth=none / auth=workload_identity (IMDS / IRSA / Pod Identity / EC2) / static creds all flow
                // through credentialsProvider(); its return type AwsCredentialsProvider is a subtype of
                // IdentityProvider<AwsCredentialsIdentity>.
                credentials = credentialsProvider(config);
            }
            s3 = buildS3Client(config, credentials);
            this.stsAsyncClient = sts;
            this.s3Client = s3;
            this.s3AsyncClient = buildS3AsyncClient(config, credentials, maxConnections);
            success = true;
        } finally {
            if (success == false) {
                List<Closeable> closeables = new ArrayList<>(2 + ownedWorkloadIdentityProviders.size());
                closeables.add(asCloseable(sts));
                closeables.add(asCloseable(s3));
                for (SdkAutoCloseable provider : ownedWorkloadIdentityProviders) {
                    closeables.add(asCloseable(provider));
                }
                IOUtils.closeWhileHandlingException(closeables);
            }
        }
    }

    /**
     * Adapts a (possibly {@code null}) AWS SDK {@link SdkAutoCloseable} client to a {@link Closeable} so it can be
     * handed to {@link IOUtils}.
     */
    private static Closeable asCloseable(SdkAutoCloseable closeable) {
        return closeable == null ? null : closeable::close;
    }

    /**
     * Test-only constructor that accepts pre-built clients plus an IRSA provider.
     * <p>
     * Single 3-arg form on purpose: a 2-arg test constructor with two nullable reference args
     * would be ambiguous against the 2-arg production constructor at {@code null, null} call
     * sites. Tests without IRSA pass {@code null} for the third arg, or use the
     * {@link #forTesting(S3Client, S3AsyncClient)} sugar.
     */
    S3StorageProvider(
        S3Client s3Client,
        S3AsyncClient s3AsyncClient,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider
    ) {
        this.config = null;
        this.stsAsyncClient = null;
        this.webIdentityTokenCredentialsProvider = webIdentityTokenCredentialsProvider;
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
    }

    /** Test-only sugar: a 2-arg form with no IRSA provider. */
    static S3StorageProvider forTesting(S3Client s3Client, S3AsyncClient s3AsyncClient) {
        return new S3StorageProvider(s3Client, s3AsyncClient, null);
    }

    private static S3Client buildS3Client(S3Configuration config, IdentityProvider<? extends AwsCredentialsIdentity> credentials) {
        return configureCommon(S3Client.builder(), config, credentials).build();
    }

    private static S3AsyncClient buildS3AsyncClient(
        S3Configuration config,
        IdentityProvider<? extends AwsCredentialsIdentity> credentials,
        int maxConnections
    ) {
        // Install a pooled receive-buffer allocator so that socket reads on Netty channels reuse
        // pooled memory instead of allocating a fresh zero-filled byte[] per read. The AWS SDK's
        // Netty client unconditionally overrides ChannelOption.ALLOCATOR to UnpooledByteBufAllocator
        // for HTTPS channels using the JDK SSL provider, so configuring ALLOCATOR directly has no
        // effect; RCVBUF_ALLOCATOR is the closest knob the SDK leaves untouched as of
        // netty-nio-client 2.31.x. Re-verify this assumption when bumping the AWS SDK version. See
        // PooledRecvByteBufAllocator for the full rationale.
        //
        // Pass the builder (not a pre-built client) so the SDK takes ownership of the Netty client
        // and closes it when S3AsyncClient.close() is called. A pre-built client passed via
        // .httpClient() is wrapped in NonManagedSdkAsyncHttpClient whose close() is a no-op.
        // Size the connection pool from the single esql.external.max_connections setting; the circuit breaker
        // bounds memory and reactive 503 backoff handles throttling. The SDK's default maxConcurrency is 50,
        // which caps a single query's read parallelism well below what S3 serves happily — S3 throttles per
        // key-prefix request rate, not per per-machine connection count, and pushes back with 503/backoff when it
        // actually needs to. connectionAcquisitionTimeout is generous so brief pool contention queues rather than
        // failing the read.
        return configureCommon(S3AsyncClient.builder(), config, credentials).httpClientBuilder(
            NettyNioAsyncHttpClient.builder()
                .putChannelOption(ChannelOption.RCVBUF_ALLOCATOR, PooledRecvByteBufAllocator.DEFAULT)
                .maxConcurrency(maxConnections)
                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
        ).build();
    }

    /**
     * Applies credentials, region, endpoint, and profile settings common to both the sync and async S3 clients.
     */
    private static <B extends S3BaseClientBuilder<B, ?>> B configureCommon(
        B builder,
        S3Configuration config,
        IdentityProvider<? extends AwsCredentialsIdentity> credentials
    ) {
        // Disable profile file loading to prevent the AWS SDK from reading ~/.aws/config
        // or the path set via AWS_CONFIG_FILE, which would be blocked by the entitlement system.
        ProfileFile emptyProfileFile = ProfileFile.aggregator().build();
        builder.overrideConfiguration(c -> {
            c.defaultProfileFile(emptyProfileFile);
            c.defaultProfileFileSupplier(() -> emptyProfileFile);
            // Pin the SDK retry strategy to Standard (deterministic: 3 attempts, jittered exponential backoff,
            // a retry-quota token bucket) instead of leaving it to resolve from the environment (which defaults
            // to Legacy / 4 attempts, or whatever AWS_RETRY_MODE/AWS_MAX_ATTEMPTS happen to be). This is the
            // per-backend, connection-aware retry layer beneath our provider-agnostic RetryPolicy.
            c.retryStrategy(AwsRetryStrategy.standardRetryStrategy());
        });

        // Disable optional response checksum validation. The SDK default (WHEN_SUPPORTED) wraps
        // every GetObject response in a checksum-validating stream, adding ~6-7% CPU overhead.
        // TLS already provides in-transit integrity; this matches what other engines do.
        builder.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);

        builder.credentialsProvider(credentials);

        if (config != null && config.region() != null) {
            builder.region(Region.of(config.region()));
        } else {
            builder.region(Region.US_EAST_1);
        }

        if (config != null && config.endpoint() != null) {
            builder.endpointOverride(URI.create(config.endpoint()));
            builder.forcePathStyle(true);
        }

        return builder;
    }

    /**
     * Returns the node-wide workload-identity issuer client, asserting the feature is enabled on this node.
     */
    private static WorkloadIdentityIssuerClient enabledWorkloadIdentityIssuerClient() {
        WorkloadIdentityIssuerClient issuerClient = WorkloadIdentityRegistry.getSharedIssuerClient();
        if (issuerClient.isEnabled() == false) {
            throw new IllegalStateException("S3 keyless authentication requires the workload-identity feature to be enabled on this node");
        }
        return issuerClient;
    }

    /**
     * Builds the AWS credentials provider for the given configuration:
     * <ul>
     *   <li>{@code auth=none} — anonymous (unsigned) requests</li>
     *   <li>{@code auth=workload_identity} — chain in order: EKS IRSA via the entitled
     *       web-identity token symlink (when the node provides
     *       {@link CustomWebIdentityTokenCredentialsProvider}), then ECS task role / EKS Pod
     *       Identity via {@link ContainerCredentialsProvider} (with the auth-token file path
     *       redirected to {@code ${ES_PATH_CONF}/esql-datasource-s3/eks-pod-identity-token} via
     *       JVM sysprop in {@code S3DataSourcePlugin}), then EC2 instance profile. Env-var and
     *       system-property providers are excluded (dev/CI convention, not the unattended-server
     *       posture).</li>
     *   <li>access_key + secret_key + session_token — STS temporary credentials</li>
     *   <li>access_key + secret_key — static credentials</li>
     * </ul>
     */
    AwsCredentialsProvider credentialsProvider(S3Configuration config) {
        if (config != null && config.isAnonymous()) {
            return AnonymousCredentialsProvider.create();
        }
        if (config != null && config.isWorkloadIdentity()) {
            return buildWorkloadIdentityCredentialsProvider();
        }
        if (config != null && config.hasCredentials()) {
            if (Strings.hasText(config.sessionToken())) {
                return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(config.accessKey(), config.secretKey(), config.sessionToken())
                );
            }
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey(), config.secretKey()));
        }
        if (config != null && Strings.hasText(config.sessionToken())) {
            // A session token alone cannot authenticate without its access key and secret key.
            throw new IllegalArgumentException("S3 session_token requires access_key and secret_key");
        }
        throw new IllegalArgumentException(
            "S3 data source requires credentials: provide WITH {\"access_key\": \"...\", \"secret_key\": \"...\"}, "
                + "optionally WITH {\"session_token\": \"...\"} for STS temporary credentials, "
                + "WITH {\"auth\": \"none\"} for public buckets, "
                + "WITH {\"auth\": \"workload_identity\"} to use the node's instance role "
                + "(requires the esql.datasource.workload_identity.enabled cluster setting), "
                + "or configure keyless authentication settings (role_arn, jwt_audience)"
        );
    }

    /**
     * Builds the credentials provider for {@code auth=workload_identity}. Default chain order:
     * <ol>
     *   <li>EKS IRSA via {@link CustomWebIdentityTokenCredentialsProvider}, if the node-level
     *       singleton exists and {@link CustomWebIdentityTokenCredentialsProvider#isActive()}.
     *       Wrapped in {@link ErrorLoggingCredentialsProvider} so STS unreachability surfaces in
     *       logs before the chain falls through.</li>
     *   <li>{@link ContainerCredentialsProvider} — covers ECS task roles and EKS Pod Identity
     *       (the latter requires the JVM sysprop {@code aws.containerAuthorizationTokenFile} to
     *       be redirected at the entitled symlink, done in {@code S3DataSourcePlugin}).</li>
     *   <li>{@link InstanceProfileCredentialsProvider} — EC2 metadata fallback.</li>
     * </ol>
     * Env-var and system-property providers are excluded — they are a dev/CI convention and open
     * a JVM-global-state override on servers. Profile-file loading is excluded (file read, blocked
     * by entitlements).
     *
     * <p>Tests may subclass and override to inject a {@code StaticCredentialsProvider} backed by
     * a local fixture — the same seam pattern used by {@code GcsStorageProvider}.
     */
    protected AwsCredentialsProvider buildWorkloadIdentityCredentialsProvider() {
        return AwsCredentialsProviderChain.builder()
            // Re-resolve through every link on each request instead of pinning the chain to the
            // first provider that ever succeeded. This matches repository-s3 and is needed so that
            // (a) the IRSA provider's short-lived STS credentials are refreshed/re-read, and (b) a
            // transient failure that caused fallback to a lower-priority provider does not become
            // permanent once the preferred provider recovers.
            .reuseLastProviderEnabled(false)
            .credentialsProviders(workloadIdentityProviders())
            .build();
    }

    /**
     * The ordered providers that {@link #buildWorkloadIdentityCredentialsProvider()} wraps in an
     * {@link AwsCredentialsProviderChain}. Exposed package-private so unit tests can assert on
     * chain composition by inspecting the list directly rather than parsing
     * {@link AwsCredentialsProviderChain#toString()}.
     */
    List<AwsCredentialsProvider> workloadIdentityProviders() {
        List<AwsCredentialsProvider> providers = new ArrayList<>(3);
        if (webIdentityTokenCredentialsProvider != null && webIdentityTokenCredentialsProvider.isActive()) {
            // Node-level singleton owned by S3DataSourcePlugin; do NOT close it from this instance.
            providers.add(new ErrorLoggingCredentialsProvider(webIdentityTokenCredentialsProvider, LOGGER));
        }
        // Created per S3StorageProvider, so this instance owns them and must close them in close().
        ContainerCredentialsProvider containerCredentialsProvider = ContainerCredentialsProvider.create();
        InstanceProfileCredentialsProvider instanceProfileCredentialsProvider = InstanceProfileCredentialsProvider.create();
        ownedWorkloadIdentityProviders.add(containerCredentialsProvider);
        ownedWorkloadIdentityProviders.add(instanceProfileCredentialsProvider);
        providers.add(containerCredentialsProvider);
        providers.add(instanceProfileCredentialsProvider);
        return providers;
    }

    /**
     * Builds an {@link AsyncWebIdentityCredentialsProvider} that mints an OIDC token through the node's
     * workload-identity issuer and exchanges it for temporary credentials via STS {@code AssumeRoleWithWebIdentity}.
     * Visible for testing so a stub issuer client and {@link StsAsyncClient} can be injected.
     */
    static AsyncWebIdentityCredentialsProvider buildWorkloadIdentityCredentialsProvider(
        S3Configuration config,
        WorkloadIdentityIssuerClient issuerClient,
        StsAsyncClient stsAsyncClient
    ) {
        String roleSessionName = Strings.hasText(config.roleSessionName()) ? config.roleSessionName() : DEFAULT_ROLE_SESSION_NAME;
        return AsyncWebIdentityCredentialsProvider.builder()
            .roleArn(config.roleArn())
            .roleSessionName(roleSessionName)
            .tokenSupplier(new S3WorkloadIdentityTokenSupplier(issuerClient, config.jwtAudience()))
            .stsAsyncClient(stsAsyncClient)
            .build();
    }

    /**
     * Builds the async STS client used for {@code AssumeRoleWithWebIdentity}. The exchange itself is unauthenticated
     * (the web-identity token is the credential), so anonymous credentials are configured.
     * <p>
     * The region is resolved independently of the bucket region: an explicit {@code sts_region} wins, otherwise the
     * bucket {@code region} is used, otherwise {@code us-east-1}. STS uses regional endpoints
     * ({@code sts.<region>.amazonaws.com}); inheriting the bucket region by default also keeps STS in the bucket's
     * AWS partition (commercial vs. GovCloud/China), while {@code sts_region}/{@code sts_endpoint} allow overriding it.
     */
    private static StsAsyncClient buildStsAsyncClient(S3Configuration config) {
        // Disable profile file loading to prevent the AWS SDK from reading ~/.aws/config, which the
        // entitlement system would block (matching configureCommon).
        ProfileFile emptyProfileFile = ProfileFile.aggregator().build();
        StsAsyncClientBuilder builder = StsAsyncClient.builder()
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .httpClient(NettyNioAsyncHttpClient.builder().build())
            .overrideConfiguration(c -> {
                c.defaultProfileFile(emptyProfileFile);
                c.defaultProfileFileSupplier(() -> emptyProfileFile);
                c.retryStrategy(AwsRetryStrategy.standardRetryStrategy());
            });
        String region = config != null ? (Strings.hasText(config.stsRegion()) ? config.stsRegion() : config.region()) : null;
        builder.region(Strings.hasText(region) ? Region.of(region) : Region.US_EAST_1);
        if (config != null && Strings.hasText(config.stsEndpoint())) {
            builder.endpointOverride(URI.create(config.stsEndpoint()));
        }
        return builder.build();
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, s3AsyncClient, bucket, key, path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, s3AsyncClient, bucket, key, path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        return new S3StorageObject(s3Client, s3AsyncClient, bucket, key, path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        validateS3Scheme(prefix);
        String bucket = prefix.host();
        String keyPrefix = extractKey(prefix);

        if (keyPrefix.isEmpty() == false && keyPrefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            keyPrefix += StoragePath.PATH_SEPARATOR;
        }

        // S3 is a flat namespace — ListObjectsV2 is inherently prefix-based and recursive.
        // The recursive flag is effectively ignored.
        return new S3StorageIterator(s3Client, bucket, keyPrefix, prefix);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);

        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            if (e instanceof S3Exception s3e && s3e.statusCode() == 403) {
                return existsViaRangeGet(bucket, key, path);
            }
            throw new IOException("Failed to check existence of " + path + credentialHint(), e);
        }
    }

    private boolean existsViaRangeGet(String bucket, String key, StoragePath path) throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=0-0").build();
            try (var response = s3Client.getObject(request)) {
                return true;
            }
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            throw new IOException("Failed to check existence of " + path + " (HEAD denied, range GET also failed)" + credentialHint(), e);
        }
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("s3", "s3a", "s3n");
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = new ArrayList<>(3 + ownedWorkloadIdentityProviders.size());
        closeables.add(asCloseable(s3Client));
        closeables.add(asCloseable(s3AsyncClient));
        closeables.add(asCloseable(stsAsyncClient));
        for (SdkAutoCloseable provider : ownedWorkloadIdentityProviders) {
            closeables.add(asCloseable(provider));
        }
        IOUtils.close(closeables);
    }

    private String credentialHint() {
        if (config == null
            || (config.isAnonymous() == false
                && config.hasCredentials() == false
                && config.hasKeylessAuth() == false
                && config.isWorkloadIdentity() == false)) {
            return ". If accessing a public bucket, use WITH {\"auth\": \"none\"}. "
                + "Otherwise, provide credentials via WITH {\"access_key\": \"...\", \"secret_key\": \"...\"} "
                + "or configure keyless authentication settings (role_arn, jwt_audience)";
        }
        return "";
    }

    private static void validateS3Scheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("s3") == false && scheme.equals("s3a") == false && scheme.equals("s3n") == false) {
            throw new IllegalArgumentException("S3StorageProvider only supports s3://, s3a://, and s3n:// schemes, got: " + scheme);
        }
    }

    private String extractKey(StoragePath path) {
        String key = path.path();
        if (key.startsWith(StoragePath.PATH_SEPARATOR)) {
            key = key.substring(1);
        }
        return key;
    }

    public S3Client s3Client() {
        return s3Client;
    }

    public S3Configuration config() {
        return config;
    }

    @Override
    public String toString() {
        return "S3StorageProvider{config=" + config + "}";
    }

    /**
     * Iterator for S3 object listing with pagination support.
     */
    private static final class S3StorageIterator implements StorageIterator {
        private final S3Client s3Client;
        private final String bucket;
        private final String prefix;
        private final StoragePath baseDirectory;

        private Iterator<S3Object> currentBatch;
        private String continuationToken;
        private boolean hasMorePages;

        S3StorageIterator(S3Client s3Client, String bucket, String prefix, StoragePath baseDirectory) {
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.prefix = prefix;
            this.baseDirectory = baseDirectory;
            this.hasMorePages = true;
        }

        @Override
        public boolean hasNext() {
            if (currentBatch == null) {
                fetchNextBatch();
            }

            if (currentBatch != null && currentBatch.hasNext()) {
                return true;
            }

            if (hasMorePages) {
                fetchNextBatch();
                return currentBatch != null && currentBatch.hasNext();
            }

            return false;
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }

            S3Object s3Object = currentBatch.next();
            String fullPath = baseDirectory.scheme() + StoragePath.SCHEME_SEPARATOR + bucket + StoragePath.PATH_SEPARATOR + s3Object.key();
            StoragePath objectPath = StoragePath.of(fullPath);

            Instant lastModified = s3Object.lastModified();
            if (lastModified == null) {
                lastModified = Instant.EPOCH;
            }
            return new StorageEntry(objectPath, s3Object.size(), lastModified);
        }

        @Override
        public void close() throws IOException {
            // No resources to close
        }

        private void fetchNextBatch() {
            try {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);

                if (continuationToken != null) {
                    requestBuilder.continuationToken(continuationToken);
                }

                ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

                currentBatch = response.contents().iterator();
                continuationToken = response.nextContinuationToken();
                hasMorePages = response.isTruncated();
            } catch (Exception e) {
                String msg = (e instanceof S3Exception s3e && s3e.statusCode() == 403)
                    ? "Access denied listing objects in bucket ["
                        + bucket
                        + "] with prefix ["
                        + prefix
                        + "]. "
                        + "Verify that the configured credentials have s3:ListBucket permission on this bucket, "
                        + "or use exact file paths instead of glob patterns."
                    : "Failed to list objects in bucket [" + bucket + "] with prefix [" + prefix + "]";
                throw new RuntimeException(msg, e);
            }
        }
    }
}
