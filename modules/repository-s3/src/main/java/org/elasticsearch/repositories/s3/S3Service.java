/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_ARN;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_SESSION_NAME;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE;

class S3Service extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(S3Service.class);

    static final Setting<TimeValue> REPOSITORY_S3_CAS_TTL_SETTING = Setting.timeSetting(
        "repository_s3.compare_and_exchange.time_to_live",
        StoreHeartbeatService.HEARTBEAT_FREQUENCY,
        Setting.Property.NodeScope
    );

    static final Setting<TimeValue> REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING = Setting.timeSetting(
        "repository_s3.compare_and_exchange.anti_contention_delay",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueHours(24),
        Setting.Property.NodeScope
    );

    private final Runnable defaultRegionSetter;
    private volatile Region defaultRegion;

    /**
     * Use a signer that does not require to pre-read (and checksum) the body of PutObject and UploadPart requests since we can rely on
     * TLS for equivalent protection.
     */
    @SuppressWarnings("deprecation")
    private static final Signer signer = AwsS3V4Signer.create();

    final CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider;

    final TimeValue compareAndExchangeTimeToLive;
    final TimeValue compareAndExchangeAntiContentionDelay;
    final boolean isStateless;
    private final S3ClientsManager s3ClientsManager;

    S3Service(
        Environment environment,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ResourceWatcherService resourceWatcherService,
        Supplier<Region> defaultRegionSupplier
    ) {
        final Settings nodeSettings = clusterService.getSettings();
        webIdentityTokenCredentialsProvider = new CustomWebIdentityTokenCredentialsProvider(
            environment,
            System::getenv,
            System::getProperty,
            Clock.systemUTC(),
            resourceWatcherService
        );
        compareAndExchangeTimeToLive = REPOSITORY_S3_CAS_TTL_SETTING.get(nodeSettings);
        compareAndExchangeAntiContentionDelay = REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING.get(nodeSettings);
        isStateless = DiscoveryNode.isStateless(nodeSettings);
        defaultRegionSetter = new RunOnce(() -> defaultRegion = defaultRegionSupplier.get());
        s3ClientsManager = new S3ClientsManager(
            nodeSettings,
            this::buildClientReference,
            clusterService.threadPool().generic(),
            projectResolver.supportsMultipleProjects()
        );
        if (projectResolver.supportsMultipleProjects()) {
            clusterService.addHighPriorityApplier(s3ClientsManager);
        }
    }

    // visible to tests
    S3ClientsManager getS3ClientsManager() {
        return s3ClientsManager;
    }

    /**
     * Refreshes the settings for the AmazonS3 clients and clears the cache of
     * existing clients. New clients will be built using these new settings. Old
     * clients are usable until released. On release, they will be destroyed instead
     * of being returned to the cache.
     */
    public synchronized void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
        s3ClientsManager.refreshAndClearCacheForClusterClients(clientsSettings);
    }

    /**
     * Attempts to retrieve a client by its repository metadata and settings from the cache.
     * If the client does not exist it will be created.
     */
    @FixForMultiProject(description = "can be removed once blobstore is project aware")
    public AmazonS3Reference client(RepositoryMetadata repositoryMetadata) {
        return client(ProjectId.DEFAULT, repositoryMetadata);
    }

    /**
     * Attempts to retrieve either a cluster or project client from the client manager. Throws if project-id or
     * the client name does not exist. The client maybe initialized lazily.
     * @param projectId The project associated with the client, or null if the client is cluster level
     */
    public AmazonS3Reference client(@Nullable ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        return s3ClientsManager.client(effectiveProjectId(projectId), repositoryMetadata);
    }

    /**
     * We use the default project-id for cluster level clients.
     */
    ProjectId effectiveProjectId(@Nullable ProjectId projectId) {
        return projectId == null ? ProjectId.DEFAULT : projectId;
    }

    // TODO: consider moving client building into S3ClientsManager
    private AmazonS3Reference buildClientReference(final S3ClientSettings clientSettings) {
        final SdkHttpClient httpClient = buildHttpClient(clientSettings, getCustomDnsResolver());
        Releasable toRelease = httpClient::close;
        try {
            final AmazonS3Reference clientReference = new AmazonS3Reference(buildClient(clientSettings, httpClient), httpClient);
            clientReference.mustIncRef();
            toRelease = null;
            return clientReference;
        } finally {
            Releasables.close(toRelease);
        }
    }

    @FixForMultiProject(description = "can be removed once blobstore is project aware")
    S3ClientSettings settings(RepositoryMetadata repositoryMetadata) {
        return settings(ProjectId.DEFAULT, repositoryMetadata);
    }

    S3ClientSettings settings(@Nullable ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        return s3ClientsManager.settingsForClient(effectiveProjectId(projectId), repositoryMetadata);
    }

    // proxy for testing
    S3Client buildClient(final S3ClientSettings clientSettings, SdkHttpClient httpClient) {
        final S3ClientBuilder s3clientBuilder = buildClientBuilder(clientSettings, httpClient);
        return s3clientBuilder.build();
    }

    protected S3ClientBuilder buildClientBuilder(S3ClientSettings clientSettings, SdkHttpClient httpClient) {
        var s3clientBuilder = S3Client.builder();
        s3clientBuilder.httpClient(httpClient);
        s3clientBuilder.overrideConfiguration(buildConfiguration(clientSettings, isStateless));
        s3clientBuilder.serviceConfiguration(b -> b.chunkedEncodingEnabled(clientSettings.disableChunkedEncoding == false));

        s3clientBuilder.credentialsProvider(buildCredentials(LOGGER, clientSettings, webIdentityTokenCredentialsProvider));

        if (clientSettings.pathStyleAccess) {
            s3clientBuilder.forcePathStyle(true);
        }

        final var clientRegion = getClientRegion(clientSettings);
        if (clientRegion == null) {
            // If no region or endpoint is specified then (for BwC with SDKv1) default to us-east-1 and enable cross-region access:
            s3clientBuilder.region(Region.US_EAST_1);
            s3clientBuilder.crossRegionAccessEnabled(true);
        } else {
            s3clientBuilder.region(clientRegion);
        }

        if (Strings.hasLength(clientSettings.endpoint)) {
            String endpoint = clientSettings.endpoint;
            if ((endpoint.startsWith("http://") || endpoint.startsWith("https://")) == false) {
                // The SDK does not know how to interpret endpoints without a scheme prefix and will error. Therefore, when the scheme is
                // absent, we'll look at the deprecated .protocol setting
                // See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/client-configuration.html#client-config-other-diffs
                endpoint = switch (clientSettings.protocol) {
                    case HTTP -> "http://" + endpoint;
                    case HTTPS -> "https://" + endpoint;
                };
                LOGGER.warn(
                    """
                        found S3 client with endpoint [{}] that is missing a scheme, guessing it should be [{}]; \
                        to suppress this warning, add a scheme prefix to the [{}] setting on this node""",
                    clientSettings.endpoint,
                    endpoint,
                    S3ClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("CLIENT_NAME").getKey()
                );
            }
            s3clientBuilder.endpointOverride(URI.create(endpoint));
        }

        return s3clientBuilder;
    }

    @Nullable // if the region is wholly unknown (falls back to us-east-1 and enables cross-region access)
    Region getClientRegion(S3ClientSettings clientSettings) {
        if (Strings.hasLength(clientSettings.region)) {
            return Region.of(clientSettings.region);
        }
        final String endpointDescription;
        final var hasEndpoint = Strings.hasLength(clientSettings.endpoint);
        if (hasEndpoint) {
            final var guessedRegion = RegionFromEndpointGuesser.guessRegion(clientSettings.endpoint);
            if (guessedRegion != null) {
                LOGGER.warn(
                    """
                        found S3 client with endpoint [{}] but no configured region, guessing it should use [{}]; \
                        to suppress this warning, configure the [{}] setting on this node""",
                    clientSettings.endpoint,
                    guessedRegion,
                    S3ClientSettings.REGION.getConcreteSettingForNamespace("CLIENT_NAME").getKey()
                );
                return Region.of(guessedRegion);
            }
            endpointDescription = "configured endpoint [" + clientSettings.endpoint + "]";
        } else {
            endpointDescription = "no configured endpoint";
        }
        final var defaultRegion = this.defaultRegion;
        if (defaultRegion != null) {
            LOGGER.debug("""
                found S3 client with no configured region and {}, using region [{}] from SDK""", endpointDescription, defaultRegion);
            return defaultRegion;
        }

        LOGGER.warn(
            """
                found S3 client with no configured region and {}, falling back to [{}]{}; \
                to suppress this warning, configure the [{}] setting on this node""",
            endpointDescription,
            Region.US_EAST_1,
            hasEndpoint ? "" : " and enabling cross-region access",
            S3ClientSettings.REGION.getConcreteSettingForNamespace("CLIENT_NAME").getKey()
        );

        return hasEndpoint ? Region.US_EAST_1 : null;
    }

    @Nullable // in production, but exposed for tests to override
    DnsResolver getCustomDnsResolver() {
        return null;
    }

    /**
     * An override for testing purposes.
     */
    Optional<Duration> getConnectionAcquisitionTimeout() {
        return Optional.empty();
    }

    private SdkHttpClient buildHttpClient(
        S3ClientSettings clientSettings,
        @Nullable /* to use default resolver */ DnsResolver dnsResolver
    ) {
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        var optConnectionAcquisitionTimout = getConnectionAcquisitionTimeout();
        if (optConnectionAcquisitionTimout.isPresent()) {
            // Only tests set this.
            httpClientBuilder.connectionAcquisitionTimeout(optConnectionAcquisitionTimout.get());
        }

        httpClientBuilder.maxConnections(clientSettings.maxConnections);
        httpClientBuilder.socketTimeout(Duration.ofMillis(clientSettings.readTimeoutMillis));

        Optional<ProxyConfiguration> proxyConfiguration = buildProxyConfiguration(clientSettings);
        if (proxyConfiguration.isPresent()) {
            httpClientBuilder.proxyConfiguration(proxyConfiguration.get());
        }

        if (dnsResolver != null) {
            httpClientBuilder.dnsResolver(dnsResolver);
        }

        return httpClientBuilder.build();
    }

    static boolean isInvalidAccessKeyIdException(Throwable e) {
        if (e instanceof AwsServiceException ase) {
            return ase.statusCode() == RestStatus.FORBIDDEN.getStatus() && "InvalidAccessKeyId".equals(ase.awsErrorDetails().errorCode());
        }
        return false;
    }

    static ClientOverrideConfiguration buildConfiguration(S3ClientSettings clientSettings, boolean isStateless) {
        ClientOverrideConfiguration.Builder clientOverrideConfiguration = ClientOverrideConfiguration.builder();
        clientOverrideConfiguration.putAdvancedOption(SdkAdvancedClientOption.SIGNER, signer);
        var retryStrategyBuilder = AwsRetryStrategy.standardRetryStrategy()
            .toBuilder()
            .maxAttempts(clientSettings.maxRetries + 1 /* first attempt is not a retry */);
        if (isStateless) {
            // Create a 403 error retryable policy. In serverless we sometimes get 403s because of delays in propagating updated credentials
            // because IAM is not strongly consistent.
            retryStrategyBuilder.retryOnException(S3Service::isInvalidAccessKeyIdException);
        }
        clientOverrideConfiguration.retryStrategy(retryStrategyBuilder.build());
        return clientOverrideConfiguration.build();
    }

    /**
     * Populates a {@link ProxyConfiguration} with any user specified settings via {@link S3ClientSettings}, if any are set.
     * Otherwise, returns empty Optional.
     */
    // pkg private for tests
    static Optional<ProxyConfiguration> buildProxyConfiguration(S3ClientSettings clientSettings) {
        // If proxy settings are provided
        if (Strings.hasText(clientSettings.proxyHost)) {
            final URIBuilder uriBuilder = new URIBuilder().setScheme(clientSettings.proxyScheme.getSchemeString())
                .setHost(clientSettings.proxyHost)
                .setPort(clientSettings.proxyPort);
            final URI proxyUri;
            try {
                proxyUri = uriBuilder.build();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }

            return Optional.of(
                ProxyConfiguration.builder()
                    .endpoint(proxyUri) // no need to set scheme, ProxyConfiguration populates the scheme from endpoint resolution
                    .username(clientSettings.proxyUsername)
                    .password(clientSettings.proxyPassword)
                    .build()
            );
        }
        return Optional.empty();
    }

    // pkg private for tests
    static AwsCredentialsProvider buildCredentials(
        Logger logger,
        S3ClientSettings clientSettings,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider
    ) {
        final AwsCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            if (webIdentityTokenCredentialsProvider.isActive()) {
                logger.debug("Using a custom provider chain of Web Identity Token and instance profile credentials");
                // Wrap the credential providers in ErrorLoggingCredentialsProvider so that we get log info if/when the STS
                // (in CustomWebIdentityTokenCredentialsProvider) is unavailable to the ES server, before falling back to a standard
                // credential provider.
                return AwsCredentialsProviderChain.builder()
                    // If credentials are refreshed, we want to look around for different forms of credentials again.
                    .reuseLastProviderEnabled(false)
                    .addCredentialsProvider(new ErrorLoggingCredentialsProvider(webIdentityTokenCredentialsProvider, LOGGER))
                    .addCredentialsProvider(new ErrorLoggingCredentialsProvider(DefaultCredentialsProvider.create(), LOGGER))
                    .build();
            } else {
                logger.debug("Using DefaultCredentialsProvider for credentials");
                return DefaultCredentialsProvider.create();
            }
        } else {
            logger.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(credentials);
        }
    }

    @FixForMultiProject(description = "can be removed once blobstore is project aware")
    public void onBlobStoreClose() {
        onBlobStoreClose(ProjectId.DEFAULT);
    }

    /**
     * Release clients for the specified project.
     * @param projectId The project associated with the client, or null if the client is cluster level
     */
    public void onBlobStoreClose(@Nullable ProjectId projectId) {
        s3ClientsManager.releaseCachedClients(effectiveProjectId(projectId));
    }

    @Override
    protected void doStart() {
        defaultRegionSetter.run();
    }

    @Override
    protected void doStop() {}

    @Override
    public void doClose() throws IOException {
        s3ClientsManager.close();
        webIdentityTokenCredentialsProvider.close();
    }

    /**
     * Customizes {@link StsWebIdentityTokenFileCredentialsProvider}.
     *
     * <ul>
     * <li>Reads the location of the web identity token not from AWS_WEB_IDENTITY_TOKEN_FILE, but from a symlink
     * in the S3 plugin directory, so we don't need to create a hardcoded read file permission for the plugin.</li>
     * <li>Supports customization of the STS (Security Token Service) endpoint via a system property, so we can
     * test it against a test fixture.</li>
     * <li>Supports gracefully shutting down the provider and the STS client.</li>
     * </ul>
     */
    static class CustomWebIdentityTokenCredentialsProvider implements AwsCredentialsProvider {

        static final String WEB_IDENTITY_TOKEN_FILE_LOCATION = "repository-s3/aws-web-identity-token-file";

        private StsWebIdentityTokenFileCredentialsProvider credentialsProvider;
        private StsClient securityTokenServiceClient;

        CustomWebIdentityTokenCredentialsProvider(
            Environment environment,
            SystemEnvironment systemEnvironment,
            JvmEnvironment jvmEnvironment,
            Clock clock,
            ResourceWatcherService resourceWatcherService
        ) {
            // Check whether the original environment variable exists. If it doesn't, the system doesn't support AWS web identity tokens
            final var webIdentityTokenFileEnvVar = systemEnvironment.getEnv(AWS_WEB_IDENTITY_TOKEN_FILE.name());
            if (webIdentityTokenFileEnvVar == null) {
                return;
            }

            // The AWS_WEB_IDENTITY_TOKEN_FILE environment variable exists, but in EKS it will point to a file outside the config directory
            // and ES therefore does not have access. Instead as per the docs we require the users to set up a symlink to a fixed location
            // within ${ES_CONF_PATH} which we can access:
            final var webIdentityTokenFileLocation = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
            if (Files.exists(webIdentityTokenFileLocation) == false) {
                LOGGER.warn(
                    """
                        Cannot use AWS Web Identity Tokens: AWS_WEB_IDENTITY_TOKEN_FILE is defined as [{}] but Elasticsearch requires a \
                        symlink to this token file at location [{}] and there is nothing at that location.""",
                    webIdentityTokenFileEnvVar,
                    webIdentityTokenFileLocation
                );
                return;
            }
            if (Files.isReadable(webIdentityTokenFileLocation) == false) {
                throw new IllegalStateException(
                    Strings.format(
                        """
                            Cannot use AWS Web Identity Tokens: AWS_WEB_IDENTITY_TOKEN_FILE is defined as [%s] but Elasticsearch requires \
                            a symlink to this token file at location [{}] and this location is not readable.""",
                        webIdentityTokenFileEnvVar,
                        webIdentityTokenFileLocation
                    )
                );
            }

            final var roleArn = systemEnvironment.getEnv(AWS_ROLE_ARN.name());
            if (roleArn == null) {
                LOGGER.warn(
                    """
                        Cannot use AWS Web Identity Tokens: AWS_WEB_IDENTITY_TOKEN_FILE is defined as [{}] but Elasticsearch requires \
                        the AWS_ROLE_ARN environment variable to be set to the ARN of the role and this variable is not set.""",
                    webIdentityTokenFileEnvVar
                );
                return;
            }

            final var roleSessionName = Objects.requireNonNullElseGet(
                systemEnvironment.getEnv(AWS_ROLE_SESSION_NAME.name()),
                // Mimic the default behaviour of the AWS SDK in case the session name is not set
                // See `com.amazonaws.auth.WebIdentityTokenCredentialsProvider#45`
                () -> "aws-sdk-java-" + clock.millis()
            );

            {
                final var securityTokenServiceClientBuilder = StsClient.builder();
                // allow an endpoint override in tests
                final var endpointOverride = jvmEnvironment.getProperty("org.elasticsearch.repositories.s3.stsEndpointOverride", null);
                if (endpointOverride != null) {
                    securityTokenServiceClientBuilder.endpointOverride(URI.create(endpointOverride));
                }
                securityTokenServiceClientBuilder.credentialsProvider(AnonymousCredentialsProvider.create());
                securityTokenServiceClient = securityTokenServiceClientBuilder.build();
            }

            try {
                credentialsProvider = StsWebIdentityTokenFileCredentialsProvider.builder()
                    .roleArn(roleArn)
                    .roleSessionName(roleSessionName)
                    .webIdentityTokenFile(webIdentityTokenFileLocation)
                    .stsClient(securityTokenServiceClient)
                    .build();

                setupFileWatcherToRefreshCredentials(webIdentityTokenFileLocation, resourceWatcherService);
            } catch (Exception e) {
                securityTokenServiceClient.close();
                throw e;
            }
        }

        @Override
        public String toString() {
            return "CustomWebIdentityTokenCredentialsProvider[" + credentialsProvider + "]";
        }

        /**
         * Sets up a {@link FileWatcher} that runs {@link StsWebIdentityTokenFileCredentialsProvider#resolveCredentials()} whenever the
         * file to which {@code webIdentityTokenFileSymlink} refers gets updated.
         */
        private void setupFileWatcherToRefreshCredentials(Path webIdentityTokenFileSymlink, ResourceWatcherService resourceWatcherService) {
            var watcher = new FileWatcher(webIdentityTokenFileSymlink);
            watcher.addListener(new FileChangesListener() {

                @Override
                public void onFileCreated(Path file) {
                    onFileChanged(file);
                }

                @Override
                public void onFileChanged(Path file) {
                    if (file.equals(webIdentityTokenFileSymlink)) {
                        LOGGER.debug("WS web identity token file [{}] changed, updating credentials", file);
                        credentialsProvider.resolveCredentials();
                    }
                }
            });
            try {
                resourceWatcherService.add(watcher, ResourceWatcherService.Frequency.LOW);
            } catch (IOException e) {
                throw new ElasticsearchException(
                    "failed to start watching AWS web identity token file [{}]",
                    e,
                    webIdentityTokenFileSymlink
                );
            }
        }

        boolean isActive() {
            return credentialsProvider != null;
        }

        public void close() throws IOException {
            Releasables.close(releasableFromSdkCloseable(credentialsProvider), releasableFromSdkCloseable(securityTokenServiceClient));
        }

        private static Releasable releasableFromSdkCloseable(SdkAutoCloseable sdkAutoCloseable) {
            return sdkAutoCloseable == null ? null : sdkAutoCloseable::close;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return credentialsProvider.resolveCredentials();
        }

        @Override
        public Class<AwsCredentialsIdentity> identityType() {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return credentialsProvider.identityType();
        }

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return credentialsProvider.resolveIdentity(request);
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer) {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return credentialsProvider.resolveIdentity(consumer);
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity() {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return credentialsProvider.resolveIdentity();
        }
    }

    /**
     * Wraps a {@link AwsCredentialsProvider} implementation and only adds error logging for any {@link #resolveCredentials()} calls that
     * throw.
     */
    static class ErrorLoggingCredentialsProvider implements AwsCredentialsProvider {

        private final AwsCredentialsProvider delegate;
        private final Logger logger;

        ErrorLoggingCredentialsProvider(AwsCredentialsProvider delegate, Logger logger) {
            this.delegate = Objects.requireNonNull(delegate);
            this.logger = Objects.requireNonNull(logger);
        }

        @Override
        public AwsCredentials resolveCredentials() {
            try {
                return delegate.resolveCredentials();
            } catch (Exception e) {
                logger.error(() -> "Unable to load credentials from " + delegate, e);
                throw e;
            }
        }

        @Override
        public Class<AwsCredentialsIdentity> identityType() {
            return delegate.identityType();
        }

        private <T> T resultHandler(T result, Throwable exception) {
            if (exception != null) {
                logger.error(() -> "Unable to resolve identity from " + delegate, exception);
                if (exception instanceof Error error) {
                    throw error;
                } else if (exception instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                } else {
                    throw new RuntimeException(exception);
                }
            }
            return result;
        }

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
            return delegate.resolveIdentity(request).handle(this::resultHandler);
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer) {
            return delegate.resolveIdentity(consumer).handle(this::resultHandler);
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity() {
            return delegate.resolveIdentity().handle(this::resultHandler);
        }

        @Override
        public String toString() {
            return "ErrorLogging[" + delegate + "]";
        }
    }

    @FunctionalInterface
    interface SystemEnvironment {
        String getEnv(String name);
    }

    @FunctionalInterface
    interface JvmEnvironment {
        String getProperty(String key, String defaultValue);
    }
}
