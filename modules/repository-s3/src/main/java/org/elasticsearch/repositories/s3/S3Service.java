/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.common.ReferenceDocs;

import org.elasticsearch.core.SuppressForbidden;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_ARN;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_SESSION_NAME;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE;

class S3Service implements Closeable {
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
    private volatile Map<S3ClientSettings, AmazonS3Reference> clientsCache = emptyMap();

    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, S3ClientSettings> staticClientSettings = Map.of(
        "default",
        S3ClientSettings.getClientSettings(Settings.EMPTY, "default")
    );

    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
     * in the {@link RepositoryMetadata}.
     */
    private volatile Map<Settings, S3ClientSettings> derivedClientSettings = emptyMap();

    final CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider;

    final TimeValue compareAndExchangeTimeToLive;
    final TimeValue compareAndExchangeAntiContentionDelay;
    final boolean isStateless;

    S3Service(Environment environment, Settings nodeSettings, ResourceWatcherService resourceWatcherService) {
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
    }

    /**
     * Refreshes the settings for the AmazonS3 clients and clears the cache of
     * existing clients. New clients will be built using these new settings. Old
     * clients are usable until released. On release, they will be destroyed instead
     * of being returned to the cache.
     */
    public synchronized void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClients();
        this.staticClientSettings = Maps.ofEntries(clientsSettings.entrySet());
        derivedClientSettings = emptyMap();
        assert this.staticClientSettings.containsKey("default") : "always at least have 'default'";
        /* clients are built lazily by {@link #client} */
    }

    /**
     * Attempts to retrieve a client by its repository metadata and settings from the cache.
     * If the client does not exist it will be created.
     */
    public AmazonS3Reference client(RepositoryMetadata repositoryMetadata) {
        final S3ClientSettings clientSettings = settings(repositoryMetadata);
        {
            final AmazonS3Reference clientReference = clientsCache.get(clientSettings);
            if (clientReference != null && clientReference.tryIncRef()) {
                return clientReference;
            }
        }
        synchronized (this) {
            final AmazonS3Reference existing = clientsCache.get(clientSettings);
            if (existing != null && existing.tryIncRef()) {
                return existing;
            }
            final AmazonS3Reference clientReference = new AmazonS3Reference(buildClient(clientSettings));
            clientReference.mustIncRef();
            clientsCache = Maps.copyMapWithAddedEntry(clientsCache, clientSettings, clientReference);
            return clientReference;
        }
    }

    /**
     * Either fetches {@link S3ClientSettings} for a given {@link RepositoryMetadata} from cached settings or creates them
     * by overriding static client settings from {@link #staticClientSettings} with settings found in the repository metadata.
     * @param repositoryMetadata Repository Metadata
     * @return S3ClientSettings
     */
    S3ClientSettings settings(RepositoryMetadata repositoryMetadata) {
        final Settings settings = repositoryMetadata.settings();
        {
            final S3ClientSettings existing = derivedClientSettings.get(settings);
            if (existing != null) {
                return existing;
            }
        }
        final String clientName = S3Repository.CLIENT_NAME.get(settings);
        final S3ClientSettings staticSettings = staticClientSettings.get(clientName);
        if (staticSettings != null) {
            synchronized (this) {
                final S3ClientSettings existing = derivedClientSettings.get(settings);
                if (existing != null) {
                    return existing;
                }
                final S3ClientSettings newSettings = staticSettings.refine(settings);
                derivedClientSettings = Maps.copyMapWithAddedOrReplacedEntry(derivedClientSettings, settings, newSettings);
                return newSettings;
            }
        }
        throw new IllegalArgumentException(
            "Unknown s3 client name ["
                + clientName
                + "]. Existing client configs: "
                + Strings.collectionToDelimitedString(staticClientSettings.keySet(), ",")
        );
    }

    // proxy for testing
    S3Client buildClient(final S3ClientSettings clientSettings) {
        final S3ClientBuilder s3clientBuilder = buildClientBuilder(clientSettings);
        return SocketAccess.doPrivileged(s3clientBuilder::build);
    }

    protected S3ClientBuilder buildClientBuilder(S3ClientSettings clientSettings) {
        // TODO NOMERGE ensure this has all the same config features as the v1 SDK
        var s3clientBuilder = S3Client.builder();
        s3clientBuilder.httpClient(buildHttpClient(clientSettings).build());
        s3clientBuilder.overrideConfiguration(buildConfiguration(clientSettings, isStateless));
        s3clientBuilder.serviceConfiguration(b -> b.chunkedEncodingEnabled(clientSettings.disableChunkedEncoding == false));

        s3clientBuilder.credentialsProvider(buildCredentials(LOGGER, clientSettings, webIdentityTokenCredentialsProvider));

        if (clientSettings.pathStyleAccess) {
            s3clientBuilder.forcePathStyle(true);
        }
        if (Strings.hasLength(clientSettings.region)) {
            s3clientBuilder.region(Region.of(clientSettings.region));
        }
        if (Strings.hasLength(clientSettings.endpoint)) {
            s3clientBuilder.endpointOverride(URI.create(clientSettings.endpoint)); // TODO NOMERGE what if URI.create fails?
        }

        return s3clientBuilder;
    }

    static ApacheHttpClient.Builder buildHttpClient(S3ClientSettings clientSettings) {
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        httpClientBuilder.maxConnections(clientSettings.maxConnections);
        httpClientBuilder.socketTimeout(Duration.ofMillis(clientSettings.readTimeoutMillis));

        applyProxyConfiguration(clientSettings, httpClientBuilder);
        return httpClientBuilder;
    }

    static final RetryCondition RETRYABLE_403_RETRY_POLICY = (retryPolicyContext) -> {
        if (RetryCondition.defaultRetryCondition().shouldRetry(retryPolicyContext)) {
            return true;
        }
        if (retryPolicyContext.exception() instanceof AwsServiceException ase) {
            return ase.statusCode() == RestStatus.FORBIDDEN.getStatus() && "InvalidAccessKeyId".equals(ase.awsErrorDetails().errorCode());
        }
        return false;
    };

    static ClientOverrideConfiguration buildConfiguration(S3ClientSettings clientSettings, boolean isStateless) {
        ClientOverrideConfiguration.Builder clientOverrideConfiguration = ClientOverrideConfiguration.builder();

        // TODO: revisit this, does it still make sense to specially retry?
        RetryPolicy.Builder retryPolicy = RetryPolicy.builder();
        retryPolicy.numRetries(clientSettings.maxRetries);
        if (isStateless) {
            // Create a 403 error retyable policy.
            retryPolicy.retryCondition(RETRYABLE_403_RETRY_POLICY);
        }

        clientOverrideConfiguration.retryPolicy(retryPolicy.build());
        clientOverrideConfiguration.putAdvancedOption(SdkAdvancedClientOption.SIGNER, clientSettings.signerOverride.signerFactory.get());
        return clientOverrideConfiguration.build();
    }

    private static void applyProxyConfiguration(S3ClientSettings clientSettings, ApacheHttpClient.Builder httpClientBuilder) {
        // If proxy settings are provided
        if (Strings.hasText(clientSettings.proxyHost)) {
            final var uriBuilder = new URIBuilder();
            uriBuilder.setScheme(clientSettings.proxyScheme.getSchemeString())
                .setHost(clientSettings.proxyHost)
                .setPort(clientSettings.proxyPort);
            final URI proxyUri;
            try {
                proxyUri = uriBuilder.build();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
            httpClientBuilder.proxyConfiguration(
                ProxyConfiguration.builder()
                    .endpoint(proxyUri)
                    .scheme(clientSettings.proxyScheme.getSchemeString())
                    .username(clientSettings.proxyUsername)
                    .password(clientSettings.proxyPassword)
                    .build()
            );
        }
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
                logger.debug("Using a custom provider chain of Web Identity Token and instance profile credentials"); // TODO: fix comment?
                return new PrivilegedAwsCredentialsProvider(
                    AwsCredentialsProviderChain.of(
                        new ErrorLoggingCredentialsProvider(webIdentityTokenCredentialsProvider, LOGGER),
                        // TODO: look into exceptions for these: sounds like they throw if unused? Instead of benignly doing nothing
                        // TODO: Also maybe only one of these?
                        // Thoughts: can I used DefaultCredentialsProvider.create() instead and avoid this problem entirely?
                        // Thoughts: ContainerCredentialsProvider can go last? Then if it throws, that's OK.
                        // new ErrorLoggingCredentialsProvider(ContainerCredentialsProvider.create(), LOGGER),
                        // new ErrorLoggingCredentialsProvider(InstanceProfileCredentialsProvider.create(), LOGGER)
                        new ErrorLoggingCredentialsProvider(DefaultCredentialsProvider.create(), LOGGER)
                    )
                );
            } else {
                logger.debug("Using instance profile credentials"); // TODO: fix comment?
                return new PrivilegedAwsCredentialsProvider(DefaultCredentialsProvider.create());
            }
        } else {
            logger.debug("Using basic key/secret credentials");
            return StaticCredentialsProvider.create(credentials);
        }
    }

    private synchronized void releaseCachedClients() {
        // the clients will shutdown when they will not be used anymore
        for (final AmazonS3Reference clientReference : clientsCache.values()) {
            clientReference.decRef();
        }
        // clear previously cached clients, they will be build lazily
        clientsCache = emptyMap();
        derivedClientSettings = emptyMap();
    }

    public void onBlobStoreClose() {
        releaseCachedClients();
    }

    @Override
    public void close() throws IOException {
        releaseCachedClients();
        webIdentityTokenCredentialsProvider.close();
    }

    /**
     * Wraps calls with {@link SocketAccess#doPrivileged(PrivilegedAction)} where needed.
     */
    static class PrivilegedAwsCredentialsProvider implements AwsCredentialsProvider {
        private final AwsCredentialsProvider delegate;

        private PrivilegedAwsCredentialsProvider(AwsCredentialsProvider delegate) {
            this.delegate = delegate;
        }

        // exposed for tests
        AwsCredentialsProvider getDelegate() {
            return delegate;
        }

        AwsCredentialsProvider getCredentialsProvider() {
            return delegate;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return null;
        }

        @Override
        public Class<AwsCredentialsIdentity> identityType() {
            return delegate.identityType();
        }

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
            return SocketAccess.doPrivileged(() -> delegate.resolveIdentity(request));
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer) {
            return SocketAccess.doPrivileged(() -> delegate.resolveIdentity(consumer));
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity() {
            return SocketAccess.doPrivileged(delegate::resolveIdentity);
        }
    }

    /**
     * Customizes {@link StsAssumeRoleWithWebIdentityCredentialsProvider}.
     *
     * <ul>
     * <li>Reads the location of the web identity token not from AWS_WEB_IDENTITY_TOKEN_FILE, but from a symlink
     * in the plugin directory, so we don't need to create a hardcoded read file permission for the plugin.</li>
     * <li>Supports customization of the STS (Security Token Service) endpoint via a system property, so we can
     * test it against a test fixture.</li>
     * <li>Supports gracefully shutting down the provider and the STS client.</li>
     * </ul>
     */
    static class CustomWebIdentityTokenCredentialsProvider implements AwsCredentialsProvider {

        private static final String STS_HOSTNAME = "https://sts.amazonaws.com";

        static final String WEB_IDENTITY_TOKEN_FILE_LOCATION = "repository-s3/aws-web-identity-token-file";

        private StsAssumeRoleWithWebIdentityCredentialsProvider credentialsProvider;
        private StsClient securityTokenServiceClient;
        private String securityTokenServiceRegion;

        CustomWebIdentityTokenCredentialsProvider(
            Environment environment,
            SystemEnvironment systemEnvironment,
            JvmEnvironment jvmEnvironment,
            Clock clock,
            ResourceWatcherService resourceWatcherService
        ) {
            // Check whether the original environment variable exists. If it doesn't,
            // the system doesn't support AWS web identity tokens
            if (systemEnvironment.getEnv(AWS_WEB_IDENTITY_TOKEN_FILE.name()) == null) {
                return;
            }

            // Make sure that a readable symlink to the token file exists in the plugin config directory
            // AWS_WEB_IDENTITY_TOKEN_FILE exists but we only use Web Identity Tokens if a corresponding symlink exists and is readable
            Path webIdentityTokenFileSymlink = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
            if (Files.exists(webIdentityTokenFileSymlink) == false) {
                LOGGER.warn(
                    "Cannot use AWS Web Identity Tokens: AWS_WEB_IDENTITY_TOKEN_FILE is defined but no corresponding symlink exists "
                        + "in the config directory"
                );
                return;
            }
            if (Files.isReadable(webIdentityTokenFileSymlink) == false) {
                throw new IllegalStateException("Unable to read a Web Identity Token symlink in the config directory");
            }

            String roleArn = systemEnvironment.getEnv(AWS_ROLE_ARN.name());
            if (roleArn == null) {
                LOGGER.warn(
                    "Unable to use a web identity token for authentication. The AWS_WEB_IDENTITY_TOKEN_FILE environment "
                        + "variable is set, but AWS_ROLE_ARN is missing"
                );
                return;
            }

            String roleSessionName = Objects.requireNonNullElseGet(
                systemEnvironment.getEnv(AWS_ROLE_SESSION_NAME.name()),
                // Mimic the default behaviour of the AWS SDK in case the session name is not set
                // See `com.amazonaws.auth.WebIdentityTokenCredentialsProvider#45`
                () -> "aws-sdk-java-" + clock.millis()
            );

            securityTokenServiceClient = createStsClient(systemEnvironment, jvmEnvironment);

            try {
                credentialsProvider = StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
                    .refreshRequest(
                        AssumeRoleWithWebIdentityRequest.builder()
                            .roleArn(roleArn)
                            .roleSessionName(roleSessionName)
                            .webIdentityToken(webIdentityTokenFileSymlink.toString())
                            .build()
                    )
                    .stsClient(securityTokenServiceClient)
                    .build();

                setupFileWatcherToRefreshCredentials(webIdentityTokenFileSymlink, resourceWatcherService);
            } catch (Exception e) {
                securityTokenServiceClient.close();
                throw e;
            }
        }

        private StsClient createStsClient(SystemEnvironment systemEnvironment, JvmEnvironment jvmEnvironment) {
            var securityTokenServiceClientBuilder = StsClient.builder();

            // Check if we need to use regional STS endpoints
            // https://docs.aws.amazon.com/sdkref/latest/guide/feature-sts-regionalized-endpoints.html
            if ("regional".equalsIgnoreCase(systemEnvironment.getEnv("AWS_STS_REGIONAL_ENDPOINTS"))) {
                // AWS_REGION should be injected by the EKS pod identity webhook:
                // https://github.com/aws/amazon-eks-pod-identity-webhook/pull/41
                securityTokenServiceRegion = systemEnvironment.getEnv(AWS_REGION.name());
                if (securityTokenServiceRegion != null) {
                    SocketAccess.doPrivilegedVoid(() -> securityTokenServiceClientBuilder.region(Region.of(securityTokenServiceRegion)));
                } else {
                    LOGGER.warn("Unable to use regional STS endpoints because the AWS_REGION environment variable is not set");
                }
            }
            if (securityTokenServiceRegion == null) {
                // Custom system property used for specifying a mocked version of the STS for testing
                String customStsEndpoint = jvmEnvironment.getProperty("com.amazonaws.sdk.stsMetadataServiceEndpointOverride", STS_HOSTNAME);
                // Set the region explicitly via the endpoint URL, so the AWS SDK doesn't make any guesses internally.

                securityTokenServiceClientBuilder.endpointOverride(URI.create(customStsEndpoint));
            }

            securityTokenServiceClientBuilder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("", "")));
            return SocketAccess.doPrivileged(securityTokenServiceClientBuilder::build);
        }

        /**
         * Sets up a {@link FileWatcher} that runs {@link StsAssumeRoleWithWebIdentityCredentialsProvider#resolveCredentials()} whenever the
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
                        SocketAccess.doPrivilegedVoid(credentialsProvider::resolveCredentials);
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

        String getSecurityTokenServiceRegion() {
            return securityTokenServiceRegion;
        }

        public void close() throws IOException {
            if (credentialsProvider != null) {
                IOUtils.close(() -> credentialsProvider.close(), () -> securityTokenServiceClient.close());
            }
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
            return SocketAccess.doPrivileged(() -> credentialsProvider.resolveIdentity(request));
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer) {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return SocketAccess.doPrivileged(() -> credentialsProvider.resolveIdentity(consumer));
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity() {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return SocketAccess.doPrivileged(credentialsProvider::resolveIdentity);
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

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
            return SocketAccess.doPrivileged(() -> delegate.resolveIdentity(request));
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer) {
            return SocketAccess.doPrivileged(() -> delegate.resolveIdentity(consumer));
        }

        @Override
        public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity() {
            return SocketAccess.doPrivileged(delegate::resolveIdentity);
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

