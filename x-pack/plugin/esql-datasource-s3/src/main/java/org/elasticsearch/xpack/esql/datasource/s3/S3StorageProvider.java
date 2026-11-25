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
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.workload.identity.aws.AsyncWebIdentityCredentialsProvider;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityRegistry;
import org.elasticsearch.xpack.esql.datasource.nettycommons.PooledRecvByteBufAllocator;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalPlanningIo;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.TestConnectionNotSupportedException;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
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
 *       pressure under load. The pool is sized by {@code esql.external.max_concurrent_requests}.</li>
 * </ul>
 * <p>
 * Both clients share the same credentials, region, and endpoint configuration. The Netty
 * jars are bundled with this plugin (classloader-isolated from the server and other plugins)
 * at {@code ${versions.netty}}, matching the pattern used by the inference plugin.
 * <p>
 * The credential mode is selected by the {@code auth} setting (see {@link S3Configuration}):
 * {@code static_credentials}, {@code federated_identity}, {@code anonymous}, or {@code managed_identity};
 * {@code auto} (the default) infers it from the fields present.
 */
public class S3StorageProvider implements StorageProvider {
    private static final Logger LOGGER = LogManager.getLogger(S3StorageProvider.class);
    private static final String DEFAULT_ROLE_SESSION_NAME = "elasticsearch-esql-datasource";
    private static final String DEFAULT_JWT_AUDIENCE = "sts.amazonaws.com";

    private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);
    /**
     * Idle read timeout on the async {@code readBytesAsync} path (Parquet ranges). Streaming
     * {@code newStream} uses the Apache client, whose abort drops the socket.
     */
    private static final Duration ASYNC_READ_TIMEOUT = Duration.ofSeconds(30);

    private static NettyNioAsyncHttpClient.Builder nettyHttpClientBuilder(int maxConnections) {
        return NettyNioAsyncHttpClient.builder()
            .putChannelOption(ChannelOption.RCVBUF_ALLOCATOR, PooledRecvByteBufAllocator.DEFAULT)
            .maxConcurrency(maxConnections)
            .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
            .readTimeout(ASYNC_READ_TIMEOUT);
    }

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    /**
     * Drives retries for {@code S3StorageObject#readBytesAsync} (AWS Standard semantics). One shared
     * instance per provider so the retry-quota token bucket spans all async reads through this
     * provider, mirroring the client-wide scope the strategy had when it lived inside the SDK client.
     * SDK-level retries are disabled on {@link #s3AsyncClient} — see {@link #buildS3AsyncClient}.
     */
    private final RetryStrategy asyncReadRetryStrategy = standardRetryStrategy();
    private final S3Configuration config;
    private final StorageIdentity storageIdentity;
    // Non-null only in the production constructor; null in the test-only constructor (forTesting).
    // Used by buildRetryClient() to rebuild the S3 client at a discovered region.
    @Nullable
    private final IdentityProvider<? extends AwsCredentialsIdentity> credentials;
    // The max-connections value for the async client, stored so pair-for-region can rebuild it.
    private final int maxConnections;
    /**
     * Test-only endpoint applied after the no-endpoint configureCommon branch so unit tests can
     * hit an in-process fake without taking the custom-endpoint path. Null in production.
     */
    @Nullable
    private final URI testEndpoint;
    /**
     * Per-bucket region cache and per-region client pairs. Null in {@link #forTesting} constructors
     * that inject pre-built clients and have no configuration.
     */
    @Nullable
    private final BucketRegionResolver resolver;

    // Owned only on the federated (keyless) workload-identity path; null otherwise. Closed by close().
    private final StsAsyncClient stsAsyncClient;
    private final CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider;
    private final EsqlContainerCredentialsProvider containerCredentialsProvider;
    /**
     * Managed-identity credentials providers that this instance creates in
     * {@link #managedIdentityProviders()} and therefore owns: the stock {@link ContainerCredentialsProvider}
     * (ECS path only) and {@link InstanceProfileCredentialsProvider}, each of which opens a background
     * credential-refresh resource. Closed by {@link #close()}. The IRSA and Pod Identity providers are
     * excluded — they are node-level singletons owned by {@code S3DataSourcePlugin}.
     */
    private final List<SdkAutoCloseable> ownedManagedIdentityProviders = new ArrayList<>();

    /**
     * Test-friendly constructor: no IRSA / Pod Identity providers available, async pool sized at the
     * {@code esql.external.max_concurrent_requests} default. Equivalent to production behavior on a node where
     * the EKS workload-identity environment variables are unset.
     */
    public S3StorageProvider(S3Configuration config) {
        this(config, null, null, ExternalSourceSettings.blobStoreConcurrency(Settings.EMPTY));
    }

    /**
     * Production constructor. {@code maxConnections} sizes both HTTP pools: Apache for blocking
     * {@code newStream} GETs and Netty for {@code readBytesAsync}
     * ({@link ExternalSourceSettings#sdkConnectionPoolSize(Settings)} from {@code esql.external.max_concurrent_requests}).
     */
    @SuppressWarnings("this-escape")
    public S3StorageProvider(
        S3Configuration config,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider,
        EsqlContainerCredentialsProvider containerCredentialsProvider,
        int maxConnections
    ) {
        this(config, webIdentityTokenCredentialsProvider, containerCredentialsProvider, maxConnections, null);
    }

    S3StorageProvider(
        S3Configuration config,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider,
        EsqlContainerCredentialsProvider containerCredentialsProvider,
        int maxConnections,
        @Nullable URI testEndpoint
    ) {
        this.config = config;
        this.storageIdentity = identityOf(config);
        this.maxConnections = maxConnections;
        this.testEndpoint = testEndpoint;
        // Set first so that managedIdentityProviders() (called from buildManagedIdentityCredentialsProvider() on
        // the MANAGED_IDENTITY path) can read them.
        this.webIdentityTokenCredentialsProvider = webIdentityTokenCredentialsProvider;
        this.containerCredentialsProvider = containerCredentialsProvider;
        StsAsyncClient sts = null;
        S3Client s3 = null;
        boolean success = false;
        try {
            if (config == null) {
                // The datasource layer always builds a provider from a validated configuration.
                throw new IllegalArgumentException("S3 data source requires a configuration");
            }
            // Select the credential mechanism from the resolved auth mode. Field inference happens only inside
            // resolveAuthMode()'s auto branch; every explicit mode maps straight to its case here.
            final IdentityProvider<? extends AwsCredentialsIdentity> resolvedCredentials = switch (config.resolveAuthMode()) {
                case ANONYMOUS -> AnonymousCredentialsProvider.create();
                case STATIC_CREDENTIALS -> buildStaticCredentials(config);
                case FEDERATED_IDENTITY -> {
                    // Resolve the issuer client (and assert it is enabled) before allocating the STS client, so a
                    // disabled-feature misconfiguration fails fast without leaking the STS client's Netty resources.
                    WorkloadIdentityIssuerClient issuerClient = enabledWorkloadIdentityIssuerClient();
                    // One STS async client and one credentials provider, shared by both S3 clients so a single token
                    // cache and a single single-flight refresh back every request.
                    sts = buildStsAsyncClient(config);
                    yield buildWorkloadIdentityCredentialsProvider(config, issuerClient, sts);
                }
                // IMDS / IRSA / Pod Identity / EC2 chain; populates ownedManagedIdentityProviders (closed in the finally).
                case MANAGED_IDENTITY -> buildManagedIdentityCredentialsProvider();
            };
            this.credentials = resolvedCredentials;
            s3 = buildS3Client(config, resolvedCredentials, maxConnections, testEndpoint);
            this.stsAsyncClient = sts;
            this.s3Client = s3;
            this.s3AsyncClient = buildS3AsyncClient(config, resolvedCredentials, maxConnections, testEndpoint);
            String seedRegion = config.region() != null ? config.region() : Region.US_EAST_1.id();
            this.resolver = new BucketRegionResolver(
                seedRegion,
                config.region(),
                this.s3Client,
                this.s3AsyncClient,
                CONNECTION_ACQUISITION_TIMEOUT.toNanos(),
                this::pairForRegion
            );
            success = true;
        } finally {
            if (success == false) {
                List<Closeable> closeables = new ArrayList<>(2 + ownedManagedIdentityProviders.size());
                closeables.add(asCloseable(sts));
                closeables.add(asCloseable(s3));
                for (SdkAutoCloseable provider : ownedManagedIdentityProviders) {
                    closeables.add(asCloseable(provider));
                }
                IOUtils.closeWhileHandlingException(closeables);
            }
        }
    }

    private static StorageIdentity identityOf(S3Configuration config) {
        return config == null ? StorageIdentity.unique() : S3CredentialIdentity.of(config);
    }

    /**
     * Adapts a (possibly {@code null}) AWS SDK {@link SdkAutoCloseable} client to a {@link Closeable} so it can be
     * handed to {@link IOUtils}.
     */
    private static Closeable asCloseable(SdkAutoCloseable closeable) {
        return closeable == null ? null : closeable::close;
    }

    /**
     * Test-only constructor that accepts pre-built clients plus workload-identity providers.
     * <p>
     * Tests without IRSA / Pod Identity pass {@code null} for those args, or use the
     * {@link #forTesting(S3Client, S3AsyncClient)} sugar.
     */
    S3StorageProvider(
        S3Client s3Client,
        S3AsyncClient s3AsyncClient,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider
    ) {
        this(s3Client, s3AsyncClient, webIdentityTokenCredentialsProvider, null);
    }

    S3StorageProvider(
        S3Client s3Client,
        S3AsyncClient s3AsyncClient,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider,
        EsqlContainerCredentialsProvider containerCredentialsProvider
    ) {
        this.config = null;
        this.storageIdentity = StorageIdentity.unique();
        this.credentials = null;
        this.stsAsyncClient = null;
        this.webIdentityTokenCredentialsProvider = webIdentityTokenCredentialsProvider;
        this.containerCredentialsProvider = containerCredentialsProvider;
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.maxConnections = ExternalSourceSettings.blobStoreConcurrency(Settings.EMPTY);
        this.testEndpoint = null;
        this.resolver = null;
    }

    /** Test-only sugar: a 2-arg form with no IRSA provider. */
    static S3StorageProvider forTesting(S3Client s3Client, S3AsyncClient s3AsyncClient) {
        return new S3StorageProvider(s3Client, s3AsyncClient, null);
    }

    /**
     * Test-only: production no-endpoint/no-region branch with an endpoint override applied after
     * branch selection, so an in-process fake can exercise the AWS path.
     */
    static S3StorageProvider forTestEndpoint(S3Configuration config, URI testEndpoint) {
        return new S3StorageProvider(config, null, null, ExternalSourceSettings.blobStoreConcurrency(Settings.EMPTY), testEndpoint);
    }

    /**
     * Test-only: accepts a configuration and pre-built (or null) S3 client.
     * Pass {@code null} for the client when the test expects testConnection() to short-circuit
     * before any client call (e.g. {@code auth=anonymous}).
     */
    S3StorageProvider(S3Configuration config, S3Client s3Client) {
        this.config = config;
        this.storageIdentity = identityOf(config);
        this.credentials = null;
        this.stsAsyncClient = null;
        this.webIdentityTokenCredentialsProvider = null;
        this.containerCredentialsProvider = null;
        this.s3Client = s3Client;
        this.s3AsyncClient = null;
        this.maxConnections = ExternalSourceSettings.blobStoreConcurrency(Settings.EMPTY);
        this.testEndpoint = null;
        this.resolver = null;
    }

    private BucketRegionResolver.ClientPair pairForRegion(String region) {
        S3Client sync = buildRetryClient(region);
        try {
            return new BucketRegionResolver.ClientPair(sync, buildRetryAsyncClient(region));
        } catch (RuntimeException e) {
            IOUtils.closeWhileHandlingException(asCloseable(sync));
            throw e;
        }
    }

    /**
     * Builds a new sync {@link S3Client} signed for {@code region}, reusing all other settings
     * (endpoint, credentials, profile suppression, retry strategy) from this provider's config.
     */
    S3Client buildRetryClient(String region) {
        S3ClientBuilder builder = S3Client.builder();
        configureCommon(builder, config, credentials, standardRetryStrategy(), region, List.of(), testEndpoint);
        return builder.httpClientBuilder(apacheHttpClientBuilder(maxConnections)).build();
    }

    /**
     * Builds a new async {@link S3AsyncClient} signed for {@code region}, reusing all other settings
     * from this provider's config. Mirrors {@link #buildRetryClient(String)} for the async path.
     */
    S3AsyncClient buildRetryAsyncClient(String region) {
        return configureCommon(S3AsyncClient.builder(), config, credentials, AwsRetryStrategy.doNotRetry(), region, List.of(), testEndpoint)
            .httpClientBuilder(nettyHttpClientBuilder(maxConnections))
            .build();
    }

    BucketRegionResolver.ClientPair clientsFor(String bucket) throws IOException {
        if (resolver == null) {
            return new BucketRegionResolver.ClientPair(s3Client, s3AsyncClient);
        }
        return resolver.clientsFor(bucket);
    }

    void invalidateBucketRegion(String bucket, @Nullable String observedRegion) {
        if (resolver != null) {
            resolver.invalidate(bucket, observedRegion);
        }
    }

    void rememberBucketRegion(String bucket, String region) {
        if (resolver != null) {
            resolver.rememberRegion(bucket, region);
        }
    }

    boolean discoversRegion() {
        return resolver != null && config != null && config.region() == null;
    }

    boolean hasResolvedRegion(String bucket) {
        return resolver != null && resolver.hasResolvedRegion(bucket);
    }

    boolean healWrongRegion(String bucket, Throwable cause) {
        if (discoversRegion() == false || BucketRegionResolver.isWrongRegion(cause) == false) {
            return false;
        }
        String observed = BucketRegionResolver.observedRegion(cause);
        invalidateBucketRegion(bucket, observed);
        if (observed != null && observed.isEmpty() == false) {
            rememberBucketRegion(bucket, observed);
        }
        return true;
    }

    /**
     * Returns true when {@code e} is an S3 {@code AuthorizationHeaderMalformed} (HTTP 400,
     * error code {@code AuthorizationHeaderMalformed}). This is the signal custom-endpoint
     * stores use to indicate a wrong signing region — as opposed to {@code SignatureDoesNotMatch},
     * which indicates wrong credentials.
     */
    static boolean isAuthorizationHeaderMalformed(Exception e) {
        return e instanceof S3Exception s3e
            && s3e.awsErrorDetails() != null
            && "AuthorizationHeaderMalformed".equals(s3e.awsErrorDetails().errorCode());
    }

    /**
     * AWS Standard retries plus {@code InvalidAccessKeyId}. IAM is not strongly consistent, so a
     * freshly minted key can 403 until it propagates; this is the S3 SDK seam that waits that out.
     * Azure/GCS would add an SDK-native predicate at their own builders if they hit the same class
     * of failure.
     */
    private static RetryStrategy standardRetryStrategy() {
        return AwsRetryStrategy.standardRetryStrategy()
            .toBuilder()
            .retryOnException(S3FailureDetail::isInvalidAccessKeyIdException)
            .build();
    }

    private static AbortRegisteringHttpClient.Builder apacheHttpClientBuilder(int maxConnections) {
        return new AbortRegisteringHttpClient.Builder(maxConnections, CONNECTION_ACQUISITION_TIMEOUT);
    }

    private static S3Client buildS3Client(
        S3Configuration config,
        IdentityProvider<? extends AwsCredentialsIdentity> credentials,
        int maxConnections,
        @Nullable URI testEndpoint
    ) {
        S3ClientBuilder builder = S3Client.builder();
        configureCommon(builder, config, credentials, standardRetryStrategy(), null, List.of(), testEndpoint);
        return builder.httpClientBuilder(apacheHttpClientBuilder(maxConnections)).build();
    }

    private static S3AsyncClient buildS3AsyncClient(
        S3Configuration config,
        IdentityProvider<? extends AwsCredentialsIdentity> credentials,
        int maxConnections,
        @Nullable URI testEndpoint
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
        // Size the connection pool from esql.external.max_concurrent_requests via
        // sdkConnectionPoolSize (N+1, or the default when unlimited). The SDK's default
        // maxConcurrency is 50, which caps a single query's read parallelism well below what S3
        // serves happily — S3 throttles per key-prefix request rate, not per per-machine connection
        // count, and pushes back with 503/backoff when it actually needs to.
        // connectionAcquisitionTimeout is generous so brief pool contention queues rather than
        // failing the read.
        //
        // SDK-level retries are DISABLED on the async client: it exists solely for
        // S3StorageObject#readBytesAsync, which drives Standard-strategy retries itself so that each
        // attempt gets a fresh CrossRegionAwareResponseTransformer (wrapping a fresh
        // KnownLengthAsyncResponseTransformer). The SDK reuses one transformer across its internal
        // retries, and a stale exceptionOccurred from a finished attempt cannot be attributed to an
        // attempt — it could spuriously fail a healthy retry and free its buffer.
        // See KnownLengthAsyncResponseTransformer's javadoc; do not re-enable retries here without
        // removing the single-use contract there.
        return configureCommon(S3AsyncClient.builder(), config, credentials, AwsRetryStrategy.doNotRetry(), null, List.of(), testEndpoint)
            .httpClientBuilder(nettyHttpClientBuilder(maxConnections))
            .build();
    }

    /**
     * Applies credentials, region, endpoint, and profile settings common to both the sync and async S3 clients.
     * The retry strategy is caller-supplied: Standard for the sync client, doNotRetry for the async client
     * (whose retries are owned by {@code S3StorageObject#readBytesAsync} — see {@link #buildS3AsyncClient}).
     * When {@code overrideRegion} is non-null it is used directly (the retry path after HeadBucket region
     * discovery); when null the region is resolved from {@code config} as usual.
     */
    /**
     * Applies credentials, region, endpoint, and profile settings common to both the sync and async S3 clients.
     * Pass an empty list for {@code interceptors} in production.
     */
    static <B extends S3BaseClientBuilder<B, ?>> B configureCommon(
        B builder,
        S3Configuration config,
        IdentityProvider<? extends AwsCredentialsIdentity> credentials,
        List<ExecutionInterceptor> interceptors
    ) {
        return configureCommon(builder, config, credentials, standardRetryStrategy(), null, interceptors, null);
    }

    private static <B extends S3BaseClientBuilder<B, ?>> B configureCommon(
        B builder,
        S3Configuration config,
        IdentityProvider<? extends AwsCredentialsIdentity> credentials,
        RetryStrategy retryStrategy,
        @Nullable String overrideRegion,
        List<ExecutionInterceptor> interceptors,
        @Nullable URI testEndpoint
    ) {
        // Disable profile file loading to prevent the AWS SDK from reading ~/.aws/config
        // or the path set via AWS_CONFIG_FILE, which would be blocked by the entitlement system.
        ProfileFile emptyProfileFile = ProfileFile.aggregator().build();
        builder.overrideConfiguration(c -> {
            c.defaultProfileFile(emptyProfileFile);
            c.defaultProfileFileSupplier(() -> emptyProfileFile);
            // Pin the SDK retry strategy explicitly (Standard is deterministic: 3 attempts, jittered exponential
            // backoff, a retry-quota token bucket) instead of leaving it to resolve from the environment (which
            // defaults to Legacy / 4 attempts, or whatever AWS_RETRY_MODE/AWS_MAX_ATTEMPTS happen to be). This is
            // the per-backend, connection-aware retry layer beneath our provider-agnostic RetryPolicy.
            c.retryStrategy(retryStrategy);
            interceptors.forEach(c::addExecutionInterceptor);
        });

        // Disable optional response checksum validation. The SDK default (WHEN_SUPPORTED) wraps
        // every GetObject response in a checksum-validating stream, adding ~6-7% CPU overhead.
        // TLS already provides in-transit integrity; this matches what other engines do.
        builder.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);

        builder.credentialsProvider(credentials);

        // Diverges from repository-s3 deliberately: datasets name individual buckets, and region
        // is now a dataset-level key rather than a data-source-level one.
        // overrideRegion is non-null when building a pair for a discovered region.
        String region = overrideRegion != null ? overrideRegion : (config == null ? null : config.region());

        if (region != null) {
            // Explicit region wins; a wrong value stays an error (no silent cross-region redirect).
            builder.region(Region.of(region));
        } else {
            // Seed us-east-1. Bucket region is resolved by BucketRegionResolver, not the SDK decorator.
            builder.region(Region.US_EAST_1);
        }

        if (config != null && Strings.hasText(config.endpoint())) {
            builder.endpointOverride(URI.create(config.endpoint()));
        }

        S3Configuration.AddressingStyleMode addressingStyle = config != null
            ? config.resolveAddressingStyle()
            : S3Configuration.AddressingStyleMode.AUTO;
        switch (addressingStyle) {
            case PATH -> builder.forcePathStyle(true);
            case VIRTUAL_HOSTED -> builder.forcePathStyle(false);
            case AUTO -> {
                // path-style when an endpoint override is set, SDK default otherwise. Same hasText test as
                // the endpointOverride guard above: a blank endpoint sets no override, so it must not
                // force path-style either.
                if (config != null && Strings.hasText(config.endpoint())) {
                    builder.forcePathStyle(true);
                }
            }
        }

        // Test seam: applied after branch selection so a fake can hit the no-endpoint AWS path.
        if (testEndpoint != null) {
            builder.endpointOverride(testEndpoint);
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
            // Operator misconfiguration, not a broken invariant: the data source asked for federated
            // identity on a node where the feature is off.
            throw new IllegalArgumentException(
                "S3 federated authentication requires the workload-identity feature to be enabled on this node"
            );
        }
        return issuerClient;
    }

    /**
     * Builds the static-credential provider for the {@code STATIC_CREDENTIALS} mode: access_key + secret_key, plus a
     * session_token for STS temporary credentials when present. Validation guarantees access_key and secret_key are
     * both set for this mode, so this is only reached with a complete static credential.
     */
    static AwsCredentialsProvider buildStaticCredentials(S3Configuration config) {
        if (Strings.hasText(config.sessionToken())) {
            return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(config.accessKey(), config.secretKey(), config.sessionToken())
            );
        }
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey(), config.secretKey()));
    }

    /**
     * Builds the credentials provider for {@code auth=managed_identity}. Default chain order:
     * <ol>
     *   <li>EKS IRSA via {@link CustomWebIdentityTokenCredentialsProvider}, if the node-level
     *       singleton exists and {@link CustomWebIdentityTokenCredentialsProvider#isActive()}.
     *       Wrapped in {@link ErrorLoggingCredentialsProvider} so STS unreachability surfaces in
     *       logs before the chain falls through.</li>
     *   <li>EKS Pod Identity via {@link EsqlContainerCredentialsProvider} when that singleton is
     *       active; otherwise the stock {@link ContainerCredentialsProvider} for ECS task roles.
     *       When the Pod Identity env vars are set but the entitled symlink is missing or
     *       unreadable: if no earlier provider is already in the chain, fails loudly naming the
     *       file rather than falling through to the stock provider; if IRSA is already present,
     *       skips the container link. Unlike IRSA (missing symlink → inactive / soft skip), Pod
     *       Identity treats a present env + missing symlink as a hard misconfiguration for the
     *       container link — matching the Azure AKS pattern — so we never open the
     *       entitlement-blocked Kubernetes token path.</li>
     *   <li>{@link InstanceProfileCredentialsProvider} — EC2 metadata fallback, only when neither
     *       IRSA nor Pod Identity is active. On EKS those providers are the intended auth path and
     *       IMDS is typically blocked; appending instance-profile there would turn a failing
     *       workload-identity call into a ~15s timeout rather than a fast failure.</li>
     * </ol>
     * Env-var and system-property providers are excluded — they are a dev/CI convention and open
     * a JVM-global-state override on servers. Profile-file loading is excluded (file read, blocked
     * by entitlements).
     *
     * <p>Tests may subclass and override to inject a {@code StaticCredentialsProvider} backed by
     * a local fixture — the same seam pattern used by {@code GcsStorageProvider}.
     */
    protected AwsCredentialsProvider buildManagedIdentityCredentialsProvider() {
        return AwsCredentialsProviderChain.builder()
            // Re-resolve through every link on each request instead of pinning the chain to the
            // first provider that ever succeeded. This matches repository-s3 and is needed so that
            // (a) the IRSA provider's short-lived STS credentials are refreshed/re-read, and (b) a
            // transient failure that caused fallback to a lower-priority provider does not become
            // permanent once the preferred provider recovers.
            .reuseLastProviderEnabled(false)
            .credentialsProviders(managedIdentityProviders())
            .build();
    }

    /**
     * The ordered providers that {@link #buildManagedIdentityCredentialsProvider()} wraps in an
     * {@link AwsCredentialsProviderChain}. Exposed package-private so unit tests can assert on
     * chain composition by inspecting the list directly rather than parsing
     * {@link AwsCredentialsProviderChain#toString()}.
     */
    List<AwsCredentialsProvider> managedIdentityProviders() {
        List<AwsCredentialsProvider> providers = new ArrayList<>(3);
        boolean irsaActive = webIdentityTokenCredentialsProvider != null && webIdentityTokenCredentialsProvider.isActive();
        boolean podIdentityActive = containerCredentialsProvider != null && containerCredentialsProvider.isActive();
        if (irsaActive) {
            // Node-level singleton owned by S3DataSourcePlugin; do NOT close it from this instance.
            providers.add(new ErrorLoggingCredentialsProvider(webIdentityTokenCredentialsProvider, LOGGER));
        }
        if (podIdentityActive) {
            // Node-level singleton owned by S3DataSourcePlugin; do NOT close it from this instance.
            providers.add(new ErrorLoggingCredentialsProvider(containerCredentialsProvider, LOGGER));
        } else if (containerCredentialsProvider != null && containerCredentialsProvider.isMisconfigured()) {
            // Pod Identity env is present but the entitled symlink is missing/unreadable (or the
            // node Environment was unavailable). Do not fall through to
            // ContainerCredentialsProvider.create() (entitlement-blocked K8s path). If IRSA is
            // already in the chain, skip the container link and continue.
            if (providers.isEmpty()) {
                throw new IllegalStateException(containerCredentialsProvider.misconfigurationMessage());
            }
            LOGGER.warn(
                "Skipping EKS Pod Identity for S3 data sources: {}; continuing with earlier managed-identity providers",
                containerCredentialsProvider.misconfigurationMessage()
            );
        } else if (containerCredentialsProvider != null && containerCredentialsProvider.isClosedAfterConfiguration()) {
            // Was successfully configured then closed (plugin shutdown). Do not fall through to
            // the ECS task-role endpoint, which is unreachable on EKS.
            if (providers.isEmpty()) {
                throw new IllegalStateException("EKS Pod Identity credentials provider has been closed");
            }
            LOGGER.warn("Skipping closed EKS Pod Identity provider; continuing with earlier managed-identity providers");
        } else {
            // ECS task-role (and any other container-credentials shape that does not use a token
            // file). Created per S3StorageProvider, so this instance owns it.
            ContainerCredentialsProvider stockContainerCredentialsProvider = ContainerCredentialsProvider.create();
            ownedManagedIdentityProviders.add(stockContainerCredentialsProvider);
            providers.add(stockContainerCredentialsProvider);
        }
        // Skip IMDS when an EKS workload-identity provider is active: IMDS is usually blocked in
        // Kubernetes, and with reuseLastProviderEnabled(false) a failing IRSA/Pod Identity call
        // would otherwise burn ~15s on IMDS retries before surfacing the real error.
        if (irsaActive == false && podIdentityActive == false) {
            InstanceProfileCredentialsProvider instanceProfileCredentialsProvider = InstanceProfileCredentialsProvider.create();
            ownedManagedIdentityProviders.add(instanceProfileCredentialsProvider);
            providers.add(instanceProfileCredentialsProvider);
        }
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
        String jwtAudience = Strings.hasText(config.jwtAudience()) ? config.jwtAudience() : DEFAULT_JWT_AUDIENCE;
        return AsyncWebIdentityCredentialsProvider.builder()
            .roleArn(config.roleArn())
            .roleSessionName(roleSessionName)
            .tokenSupplier(new S3WorkloadIdentityTokenSupplier(issuerClient, jwtAudience))
            .stsAsyncClient(stsAsyncClient)
            .build();
    }

    /**
     * Builds the async STS client used for {@code AssumeRoleWithWebIdentity}. The exchange itself is unauthenticated
     * (the web-identity token is the credential), so anonymous credentials are configured.
     * <p>
     * The region is resolved independently of the dataset region: an explicit {@code sts_region} wins, otherwise the
     * dataset {@code region} is used (arriving at the top level of the flat config after
     * {@code DatasetRewriter.mergeSettings} strips it from the parent {@code _datasource} contribution), otherwise
     * {@code us-east-1}. STS uses regional endpoints ({@code sts.<region>.amazonaws.com}); inheriting the dataset
     * region by default also keeps STS in the bucket's AWS partition (commercial vs. GovCloud/China), while
     * {@code sts_region}/{@code sts_endpoint} allow overriding it.
     * <p>
     * Backward-compat note: a federated data source that previously set {@code region} on the data source, and whose
     * datasets omit it, will now resolve STS at {@code us-east-1}; set {@code sts_region} on the data source to fix.
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
    public StorageEntry objectMetadata(StoragePath path) throws IOException {
        Target t = resolve(path);
        BucketRegionResolver.ClientPair pair = clientsFor(t.bucket);
        return owned(new S3StorageObject(pair.sync(), pair.async(), asyncReadRetryStrategy, storageIdentity, t.bucket, t.key, path))
            .headObjectMetadata();
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        Target t = resolve(path);
        return owned(new S3StorageObject(t.sync, t.async, asyncReadRetryStrategy, storageIdentity, t.bucket, t.key, path));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        Target t = resolve(path);
        return owned(new S3StorageObject(t.sync, t.async, asyncReadRetryStrategy, storageIdentity, t.bucket, t.key, path, length));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        Target t = resolve(path);
        return owned(
            new S3StorageObject(t.sync, t.async, asyncReadRetryStrategy, storageIdentity, t.bucket, t.key, path, length, lastModified)
        );
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified, String version) {
        Target t = resolve(path);
        S3StorageObject object = owned(
            new S3StorageObject(t.sync, t.async, asyncReadRetryStrategy, storageIdentity, t.bucket, t.key, path, length, lastModified)
        );
        object.seedVersion(version);
        return object;
    }

    private S3StorageObject owned(S3StorageObject object) {
        object.ownedBy(this);
        return object;
    }

    /** The bucket, the key and the seed clients. Region-pinned clients are resolved on first throwing I/O. */
    private record Target(S3Client sync, S3AsyncClient async, String bucket, String key) {}

    /**
     * Checks the scheme and splits the path. Does not HeadBucket: {@code newObject} must not throw.
     */
    private Target resolve(StoragePath path) {
        validateS3Scheme(path);
        return new Target(s3Client, s3AsyncClient, path.host(), extractKey(path));
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        return listObjects(prefix, "", recursive);
    }

    /**
     * S3 lists by key prefix natively, so {@code namePrefix} is appended to the directory's key and the store does
     * the narrowing. The separator closing that directory's key is added either way; what differs is that with a
     * name prefix the key asked for then deliberately stops mid-name.
     */
    @Override
    public StorageIterator listObjects(StoragePath prefix, String namePrefix, boolean recursive) throws IOException {
        validateS3Scheme(prefix);
        String bucket = prefix.host();
        String keyPrefix = extractKey(prefix);

        if (keyPrefix.isEmpty() == false && keyPrefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            keyPrefix += StoragePath.PATH_SEPARATOR;
        }
        keyPrefix += namePrefix;

        S3Client listingClient = clientsFor(bucket).sync();
        // S3 is a flat namespace — ListObjectsV2 is inherently prefix-based and recursive.
        // The recursive flag is effectively ignored.
        return new S3StorageIterator(this, listingClient, bucket, keyPrefix, prefix, regionHint());
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) throws IOException {
        return listChildren(prefix, limit, true);
    }

    private StorageChildren listChildren(StoragePath prefix, int limit, boolean allowHeal) throws IOException {
        validateS3Scheme(prefix);
        String bucket = prefix.host();
        String keyPrefix = extractKey(prefix);
        if (keyPrefix.isEmpty() == false && keyPrefix.endsWith(StoragePath.PATH_SEPARATOR) == false) {
            keyPrefix += StoragePath.PATH_SEPARATOR;
        }

        List<StorageEntry> files = new ArrayList<>();
        List<StoragePath> directories = new ArrayList<>();
        String pathPrefix = bucketPathPrefix(prefix.scheme(), bucket);
        String continuationToken = null;
        try {
            S3Client listingClient = clientsFor(bucket).sync();
            do {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(keyPrefix)
                    .delimiter(StoragePath.PATH_SEPARATOR);
                if (continuationToken != null) {
                    requestBuilder.continuationToken(continuationToken);
                }
                ListObjectsV2Response response = listingClient.listObjectsV2(requestBuilder.build());
                ExternalPlanningIo.addMetadataGet(0);
                for (S3Object s3Object : response.contents()) {
                    if (s3Object.key().endsWith(StoragePath.PATH_SEPARATOR)) {
                        continue; // directory placeholder key (console "folder" object)
                    }
                    files.add(toStorageEntry(s3Object, pathPrefix));
                }
                for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                    String dirKey = commonPrefix.prefix();
                    if (dirKey.endsWith(StoragePath.PATH_SEPARATOR)) {
                        dirKey = dirKey.substring(0, dirKey.length() - 1);
                    }
                    directories.add(StoragePath.of(pathPrefix + dirKey));
                }
                if (files.size() + directories.size() > limit) {
                    return null; // too wide to buffer; the caller falls back to listObjects, which pages lazily
                }
                if (truncatedWithoutToken(response)) {
                    throw new TruncatedWithoutTokenException();
                }
                continuationToken = continuationTokenOf(response);
            } while (continuationToken != null);
        } catch (TruncatedWithoutTokenException e) {
            throw new IOException("Failed to list children in the configured path: " + TRUNCATED_WITHOUT_TOKEN_MESSAGE);
        } catch (Exception e) {
            ExternalPlanningIo.addMetadataGet(0);
            if (allowHeal && healWrongRegion(bucket, e)) {
                return listChildren(prefix, limit, false);
            }
            if (e instanceof ExternalException runtime) {
                throw runtime;
            }
            // Same typing as the other list sites: a 503/429 must surface as ExternalUnavailableException so the
            // retry layer re-attempts it and the adaptive backoff hears about it.
            ExternalUnavailableException unavailable = mapResolveFailure(prefix, e);
            if (unavailable != null) {
                throw unavailable;
            }
            throw new IOException("Failed to list children in the configured path: " + S3FailureDetail.of(e), e);
        }
        return new StorageChildren(files, directories);
    }

    /** The {@code scheme://bucket/} prefix full object paths are built from, shared with {@link S3StorageIterator}. */
    private static String bucketPathPrefix(String scheme, String bucket) {
        return scheme + StoragePath.SCHEME_SEPARATOR + bucket + StoragePath.PATH_SEPARATOR;
    }

    /** One conversion from an SDK listing entry to a {@link StorageEntry}, shared by both listing shapes. */
    private static StorageEntry toStorageEntry(S3Object s3Object, String pathPrefix) {
        Instant lastModified = s3Object.lastModified() != null ? s3Object.lastModified() : Instant.EPOCH;
        return new StorageEntry(
            StoragePath.of(pathPrefix + s3Object.key()),
            s3Object.size(),
            lastModified,
            S3StorageObject.strongEtag(s3Object.eTag())
        );
    }

    @Override
    public boolean listsInKeyOrder() {
        return true;
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateS3Scheme(path);
        String bucket = path.host();
        String key = extractKey(path);
        try {
            return existsWithClient(clientsFor(bucket).sync(), bucket, key, path);
        } catch (Exception e) {
            if (healWrongRegion(bucket, e)) {
                return existsWithClient(clientsFor(bucket).sync(), bucket, key, path);
            }
            if (e instanceof IOException ioe) {
                throw ioe;
            }
            if (e instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException(
                "Failed to check existence of external object: " + S3FailureDetail.of(e) + credentialHint() + regionHint(),
                e
            );
        }
    }

    private boolean existsWithClient(S3Client client, String bucket, String key, StoragePath path) throws IOException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            ExternalCredentialsExpiredException expired = S3FailureDetail.expired(e, "checking object existence");
            if (expired != null) {
                throw expired;
            }
            if (e instanceof S3Exception s3e && s3e.statusCode() == 403) {
                return existsViaRangeGet(client, bucket, key, path);
            }
            ExternalUnavailableException unavailable = mapResolveFailure(path, e);
            if (unavailable != null) {
                throw unavailable;
            }
            throw new IOException(
                "Failed to check existence of external object: " + S3FailureDetail.of(e) + credentialHint() + regionHint(),
                e
            );
        }
    }

    private boolean existsViaRangeGet(S3Client client, String bucket, String key, StoragePath path) throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=0-0").build();
            try (var response = client.getObject(request)) {
                return true;
            }
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            ExternalCredentialsExpiredException expired = S3FailureDetail.expired(e, "checking object existence");
            if (expired != null) {
                throw expired;
            }
            ExternalUnavailableException unavailable = mapResolveFailure(path, e);
            if (unavailable != null) {
                throw unavailable;
            }
            throw new IOException(
                "Failed to check existence of external object (HEAD denied, range GET also failed): "
                    + S3FailureDetail.of(e)
                    + credentialHint()
                    + regionHint(),
                e
            );
        }
    }

    /**
     * Types retryable S3 statuses and SDK transport failures (no HTTP response) before resolution
     * code can erase them inside an {@link IOException}. The returned exception is both the HTTP 503
     * surfaced to the caller and the marker consumed by the storage retry policy.
     */
    static ExternalUnavailableException mapResolveFailure(StoragePath path, Exception cause) {
        if (cause instanceof S3Exception s3 && ExternalUnavailableException.isRetryableStatus(s3.statusCode())) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(s3.statusCode());
            long retryAfterMs = 0L;
            if (throttling && s3.awsErrorDetails() != null && s3.awsErrorDetails().sdkHttpResponse() != null) {
                retryAfterMs = ExternalUnavailableException.parseRetryAfterMs(
                    s3.awsErrorDetails().sdkHttpResponse().firstMatchingHeader("Retry-After").orElse(null)
                );
            }
            Condition condition = throttling ? Condition.STORE_THROTTLED : Condition.STORE_UNAVAILABLE;
            return new ExternalUnavailableException(condition, path, "HTTP " + s3.statusCode(), "", throttling, retryAfterMs, cause);
        }
        if (S3StorageObject.isSdkClientTransportFailure(cause)) {
            return new ExternalUnavailableException(Condition.STORE_UNAVAILABLE, path, S3FailureDetail.of(cause), "", false, 0L, cause);
        }
        return null;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("s3", "s3a", "s3n");
    }

    @Override
    public void close() throws IOException {
        if (resolver != null) {
            resolver.close();
        }
        List<Closeable> closeables = new ArrayList<>(3 + ownedManagedIdentityProviders.size());
        closeables.add(asCloseable(s3Client));
        closeables.add(asCloseable(s3AsyncClient));
        closeables.add(asCloseable(stsAsyncClient));
        for (SdkAutoCloseable provider : ownedManagedIdentityProviders) {
            closeables.add(asCloseable(provider));
        }
        IOUtils.close(closeables);
    }

    private String credentialHint() {
        if (config == null || config.resolveAuthModeOrNull() == null) {
            return ". If accessing a public bucket, set auth=anonymous. "
                + "Otherwise, provide credentials via access_key and secret_key, "
                + "or configure federated authentication with role_arn";
        }
        return "";
    }

    private String regionHint() {
        if (config == null || config.region() == null) {
            return ". If the bucket is not in us-east-1, set [region] on the dataset";
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

    /**
     * Tests connectivity by attempting {@code ListBuckets}.
     * <p>
     * A {@code 403 AccessDenied} response means the credentials are valid but bucket-scoped — they
     * signed and delivered the request, so authentication works; the IAM policy just does not grant
     * {@code s3:ListAllMyBuckets}. Under the false-negative avoidance principle, this returns
     * {@link TestConnectionNotSupportedException} (untestable) rather than success or failure:
     * the same as GCS and Azure when facing bucket/container-scoped credentials. The user is
     * directed to create a dataset to verify access at the bucket level.
     *
     * <p>An {@code AuthorizationHeaderMalformed} error (HTTP 400) means the request was signed for
     * the wrong region. Queries recover via {@link BucketRegionResolver}, but {@code ListBuckets}
     * has no bucket to discover the region from, so this probe cannot perform the same recovery.
     * Because the data source works fine for queries, this is also reported as {@code untestable}
     * rather than {@code failure}.
     *
     * <p>Invalid credentials ({@code InvalidClientTokenId}, {@code SignatureDoesNotMatch}) are re-thrown as failures.
     * Called from the factory's {@code testConnection} on a GENERIC thread — blocking I/O is expected.
     */
    public void testConnection() {
        if (config != null && config.isAnonymous()) {
            throw new TestConnectionNotSupportedException(
                "S3 anonymous access cannot be verified at the data source level",
                "Anonymous access targets public buckets; create a dataset to validate read access."
            );
        }
        try {
            s3Client.listBuckets();
        } catch (S3Exception e) {
            if (e.statusCode() == 403 && e.awsErrorDetails() != null && "AccessDenied".equals(e.awsErrorDetails().errorCode())) {
                // Credentials are valid but bucket-scoped; cannot verify at the data-source level.
                throw new TestConnectionNotSupportedException(
                    "S3 returned 403 AccessDenied on ListBuckets; credentials may be bucket-scoped",
                    "Bucket-scoped credentials cannot be verified at the data source level; create a dataset to validate access."
                );
            }
            if (isAuthorizationHeaderMalformed(e)) {
                // Custom endpoint with no region: ListBuckets fails with AuthorizationHeaderMalformed
                // because the request was signed for the wrong region. Region is a dataset-level setting,
                // not a data-source-level one, so the probe cannot discover it here. Queries recover via
                // HeadBucket region-discovery, but that retry needs a bucket name from the dataset URI.
                throw new TestConnectionNotSupportedException(
                    "S3 returned AuthorizationHeaderMalformed on ListBuckets; region cannot be determined at the data source level",
                    "Create a dataset with the region configured to validate access."
                );
            }
            throw e;
        }
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
        private final S3StorageProvider provider;
        private S3Client s3Client;
        private final String bucket;
        private final String prefix;
        private final StoragePath baseDirectory;
        private final String regionHint;
        private boolean healed;

        private Iterator<S3Object> currentBatch;
        private String continuationToken;
        private boolean firstPageFetched;

        S3StorageIterator(
            S3StorageProvider provider,
            S3Client s3Client,
            String bucket,
            String prefix,
            StoragePath baseDirectory,
            String regionHint
        ) {
            this.provider = provider;
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.prefix = prefix;
            this.baseDirectory = baseDirectory;
            this.regionHint = regionHint;
        }

        @Override
        public boolean hasNext() {
            while (currentBatch == null || currentBatch.hasNext() == false) {
                if (firstPageFetched && continuationToken == null) {
                    return false;
                }
                fetchNextBatch();
            }
            return true;
        }

        @Override
        public StorageEntry next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }

            S3Object s3Object = currentBatch.next();
            return toStorageEntry(s3Object, bucketPathPrefix(baseDirectory.scheme(), bucket));
        }

        @Override
        public void close() throws IOException {
            // The retry client (when used) is cached and owned by the enclosing S3StorageProvider; do not close here.
        }

        private void fetchNextBatch() {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response;
            try {
                response = s3Client.listObjectsV2(requestBuilder.build());
                ExternalPlanningIo.addMetadataGet(0);
            } catch (Exception e) {
                if (healed == false && provider.healWrongRegion(bucket, e)) {
                    ExternalPlanningIo.addMetadataGet(0);
                    healed = true;
                    try {
                        s3Client = provider.clientsFor(bucket).sync();
                        response = s3Client.listObjectsV2(requestBuilder.build());
                        ExternalPlanningIo.addMetadataGet(0);
                    } catch (Exception retry) {
                        throw listingFailure(retry);
                    }
                } else {
                    throw listingFailure(e);
                }
            }
            if (truncatedWithoutToken(response)) {
                throw new UncheckedIOException(
                    new IOException("Failed to list objects in the configured path: " + TRUNCATED_WITHOUT_TOKEN_MESSAGE)
                );
            }
            currentBatch = response.contents().iterator();
            continuationToken = continuationTokenOf(response);
            firstPageFetched = true;
        }

        private RuntimeException listingFailure(Exception e) {
            ExternalPlanningIo.addMetadataGet(0);
            if (e instanceof ExternalException runtime) {
                return runtime;
            }
            ExternalCredentialsExpiredException expired = S3FailureDetail.expired(e, "listing objects in the configured path");
            if (expired != null) {
                return expired;
            }
            ExternalUnavailableException unavailable = mapResolveFailure(baseDirectory, e);
            if (unavailable != null) {
                return unavailable;
            }
            String msg = (e instanceof S3Exception s3e && s3e.statusCode() == 403)
                ? "Access denied listing objects in the configured path. "
                    + "Verify that the configured credentials have s3:ListBucket permission on this bucket, "
                    + "or use exact file paths instead of glob patterns."
                : "Failed to list objects in the configured path";
            return new UncheckedIOException(new IOException(msg + ": " + S3FailureDetail.of(e) + regionHint, e));
        }
    }

    private static final class TruncatedWithoutTokenException extends RuntimeException {
        TruncatedWithoutTokenException() {
            super(null, null, false, false);
        }
    }

    private static final String TRUNCATED_WITHOUT_TOKEN_MESSAGE =
        "the store returned a truncated listing page without a continuation token";

    private static boolean truncatedWithoutToken(ListObjectsV2Response response) {
        return Boolean.TRUE.equals(response.isTruncated()) && continuationTokenOf(response) == null;
    }

    private static String continuationTokenOf(ListObjectsV2Response response) {
        String token = response.nextContinuationToken();
        return token == null || token.isEmpty() ? null : token;
    }

}
