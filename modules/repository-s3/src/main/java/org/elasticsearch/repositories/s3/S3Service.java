/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import org.apache.http.HttpStatus;
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
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.amazonaws.SDKGlobalConfiguration.AWS_ROLE_ARN_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.AWS_ROLE_SESSION_NAME_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.AWS_WEB_IDENTITY_ENV_VAR;
import static java.util.Collections.emptyMap;

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
     * existing clients. New clients will be build using these new settings. Old
     * clients are usable until released. On release they will be destroyed instead
     * of being returned to the cache.
     */
    public synchronized void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {
        // shutdown all unused clients
        // others will shutdown on their respective release
        releaseCachedClients();
        this.staticClientSettings = Maps.ofEntries(clientsSettings.entrySet());
        derivedClientSettings = emptyMap();
        assert this.staticClientSettings.containsKey("default") : "always at least have 'default'";
        // clients are built lazily by {@link client}
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
    AmazonS3 buildClient(final S3ClientSettings clientSettings) {
        final AmazonS3ClientBuilder builder = buildClientBuilder(clientSettings);
        return SocketAccess.doPrivileged(builder::build);
    }

    protected AmazonS3ClientBuilder buildClientBuilder(S3ClientSettings clientSettings) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(buildCredentials(LOGGER, clientSettings, webIdentityTokenCredentialsProvider));
        final ClientConfiguration clientConfiguration = buildConfiguration(clientSettings, isStateless);
        assert (isStateless == false && clientConfiguration.getRetryPolicy() == PredefinedRetryPolicies.DEFAULT)
            || (isStateless && clientConfiguration.getRetryPolicy() == RETRYABLE_403_RETRY_POLICY) : "invalid retry policy configuration";
        builder.withClientConfiguration(clientConfiguration);

        String endpoint = Strings.hasLength(clientSettings.endpoint) ? clientSettings.endpoint : Constants.S3_HOSTNAME;
        if ((endpoint.startsWith("http://") || endpoint.startsWith("https://")) == false) {
            // Manually add the schema to the endpoint to work around https://github.com/aws/aws-sdk-java/issues/2274
            // TODO: Remove this once fixed in the AWS SDK
            endpoint = clientSettings.protocol.toString() + "://" + endpoint;
        }
        final String region = Strings.hasLength(clientSettings.region) ? clientSettings.region : null;
        LOGGER.debug("using endpoint [{}] and region [{}]", endpoint, region);

        // If the endpoint configuration isn't set on the builder then the default behaviour is to try
        // and work out what region we are in and use an appropriate endpoint - see AwsClientBuilder#setRegion.
        // In contrast, directly-constructed clients use s3.amazonaws.com unless otherwise instructed. We currently
        // use a directly-constructed client, and need to keep the existing behaviour to avoid a breaking change,
        // so to move to using the builder we must set it explicitly to keep the existing behaviour.
        //
        // We do this because directly constructing the client is deprecated (was already deprecated in 1.1.223 too)
        // so this change removes that usage of a deprecated API.
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        if (clientSettings.pathStyleAccess) {
            builder.enablePathStyleAccess();
        }
        if (clientSettings.disableChunkedEncoding) {
            builder.disableChunkedEncoding();
        }
        return builder;
    }

    // pkg private for tests
    static ClientConfiguration buildConfiguration(S3ClientSettings clientSettings, boolean isStateless) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(clientSettings.protocol);

        if (Strings.hasText(clientSettings.proxyHost)) {
            // TODO: remove this leniency, these settings should exist together and be validated
            clientConfiguration.setProxyHost(clientSettings.proxyHost);
            clientConfiguration.setProxyPort(clientSettings.proxyPort);
            clientConfiguration.setProxyProtocol(clientSettings.proxyScheme);
            clientConfiguration.setProxyUsername(clientSettings.proxyUsername);
            clientConfiguration.setProxyPassword(clientSettings.proxyPassword);
        }

        if (Strings.hasLength(clientSettings.signerOverride)) {
            clientConfiguration.setSignerOverride(clientSettings.signerOverride);
        }

        clientConfiguration.setMaxConnections(clientSettings.maxConnections);
        clientConfiguration.setMaxErrorRetry(clientSettings.maxRetries);
        clientConfiguration.setUseThrottleRetries(clientSettings.throttleRetries);
        clientConfiguration.setSocketTimeout(clientSettings.readTimeoutMillis);

        if (isStateless) {
            clientConfiguration.setRetryPolicy(RETRYABLE_403_RETRY_POLICY);
        }

        return clientConfiguration;
    }

    // pkg private for tests
    static AWSCredentialsProvider buildCredentials(
        Logger logger,
        S3ClientSettings clientSettings,
        CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider
    ) {
        final S3BasicCredentials credentials = clientSettings.credentials;
        if (credentials == null) {
            if (webIdentityTokenCredentialsProvider.isActive()) {
                logger.debug("Using a custom provider chain of Web Identity Token and instance profile credentials");
                return new PrivilegedAWSCredentialsProvider(
                    new AWSCredentialsProviderChain(
                        List.of(
                            new ErrorLoggingCredentialsProvider(webIdentityTokenCredentialsProvider, LOGGER),
                            new ErrorLoggingCredentialsProvider(new EC2ContainerCredentialsProviderWrapper(), LOGGER)
                        )
                    )
                );
            } else {
                logger.debug("Using instance profile credentials");
                return new PrivilegedAWSCredentialsProvider(new EC2ContainerCredentialsProviderWrapper());
            }
        } else {
            logger.debug("Using basic key/secret credentials");
            return new AWSStaticCredentialsProvider(credentials);
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
        // shutdown IdleConnectionReaper background thread
        // it will be restarted on new client usage
        IdleConnectionReaper.shutdown();
    }

    public void onBlobStoreClose() {
        releaseCachedClients();
    }

    @Override
    public void close() throws IOException {
        releaseCachedClients();
        webIdentityTokenCredentialsProvider.shutdown();
    }

    static class PrivilegedAWSCredentialsProvider implements AWSCredentialsProvider {
        private final AWSCredentialsProvider credentialsProvider;

        private PrivilegedAWSCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
        }

        AWSCredentialsProvider getCredentialsProvider() {
            return credentialsProvider;
        }

        @Override
        public AWSCredentials getCredentials() {
            return SocketAccess.doPrivileged(credentialsProvider::getCredentials);
        }

        @Override
        public void refresh() {
            SocketAccess.doPrivilegedVoid(credentialsProvider::refresh);
        }
    }

    /**
     * Customizes {@link com.amazonaws.auth.WebIdentityTokenCredentialsProvider}
     *
     * <ul>
     * <li>Reads the location of the web identity token not from AWS_WEB_IDENTITY_TOKEN_FILE, but from a symlink
     * in the plugin directory, so we don't need to create a hardcoded read file permission for the plugin.</li>
     * <li>Supports customization of the STS endpoint via a system property, so we can test it against a test fixture.</li>
     * <li>Supports gracefully shutting down the provider and the STS client.</li>
     * </ul>
     */
    static class CustomWebIdentityTokenCredentialsProvider implements AWSCredentialsProvider {

        private static final String STS_HOSTNAME = "https://sts.amazonaws.com";

        static final String WEB_IDENTITY_TOKEN_FILE_LOCATION = "repository-s3/aws-web-identity-token-file";

        private STSAssumeRoleWithWebIdentitySessionCredentialsProvider credentialsProvider;
        private AWSSecurityTokenService stsClient;
        private String stsRegion;

        CustomWebIdentityTokenCredentialsProvider(
            Environment environment,
            SystemEnvironment systemEnvironment,
            JvmEnvironment jvmEnvironment,
            Clock clock,
            ResourceWatcherService resourceWatcherService
        ) {
            // Check whether the original environment variable exists. If it doesn't,
            // the system doesn't support AWS web identity tokens
            if (systemEnvironment.getEnv(AWS_WEB_IDENTITY_ENV_VAR) == null) {
                return;
            }
            // Make sure that a readable symlink to the token file exists in the plugin config directory
            // AWS_WEB_IDENTITY_TOKEN_FILE exists but we only use Web Identity Tokens if a corresponding symlink exists and is readable
            Path webIdentityTokenFileSymlink = environment.configFile().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
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
            String roleArn = systemEnvironment.getEnv(AWS_ROLE_ARN_ENV_VAR);
            if (roleArn == null) {
                LOGGER.warn(
                    "Unable to use a web identity token for authentication. The AWS_WEB_IDENTITY_TOKEN_FILE environment "
                        + "variable is set, but either AWS_ROLE_ARN is missing"
                );
                return;
            }
            String roleSessionName = Objects.requireNonNullElseGet(
                systemEnvironment.getEnv(AWS_ROLE_SESSION_NAME_ENV_VAR),
                // Mimic the default behaviour of the AWS SDK in case the session name is not set
                // See `com.amazonaws.auth.WebIdentityTokenCredentialsProvider#45`
                () -> "aws-sdk-java-" + clock.millis()
            );
            AWSSecurityTokenServiceClientBuilder stsClientBuilder = AWSSecurityTokenServiceClient.builder();

            // Check if we need to use regional STS endpoints
            // https://docs.aws.amazon.com/sdkref/latest/guide/feature-sts-regionalized-endpoints.html
            if ("regional".equalsIgnoreCase(systemEnvironment.getEnv("AWS_STS_REGIONAL_ENDPOINTS"))) {
                // AWS_REGION should be injected by the EKS pod identity webhook:
                // https://github.com/aws/amazon-eks-pod-identity-webhook/pull/41
                stsRegion = systemEnvironment.getEnv(SDKGlobalConfiguration.AWS_REGION_ENV_VAR);
                if (stsRegion != null) {
                    SocketAccess.doPrivilegedVoid(() -> stsClientBuilder.withRegion(stsRegion));
                } else {
                    LOGGER.warn("Unable to use regional STS endpoints because the AWS_REGION environment variable is not set");
                }
            }
            if (stsRegion == null) {
                // Custom system property used for specifying a mocked version of the STS for testing
                String customStsEndpoint = jvmEnvironment.getProperty("com.amazonaws.sdk.stsMetadataServiceEndpointOverride", STS_HOSTNAME);
                // Set the region explicitly via the endpoint URL, so the AWS SDK doesn't make any guesses internally.
                stsClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(customStsEndpoint, null));
            }
            stsClientBuilder.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
            stsClient = SocketAccess.doPrivileged(stsClientBuilder::build);
            try {
                credentialsProvider = new STSAssumeRoleWithWebIdentitySessionCredentialsProvider.Builder(
                    roleArn,
                    roleSessionName,
                    webIdentityTokenFileSymlink.toString()
                ).withStsClient(stsClient).build();
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
                            credentialsProvider.refresh();
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
            } catch (Exception e) {
                stsClient.shutdown();
                throw e;
            }
        }

        boolean isActive() {
            return credentialsProvider != null;
        }

        String getStsRegion() {
            return stsRegion;
        }

        @Override
        public AWSCredentials getCredentials() {
            Objects.requireNonNull(credentialsProvider, "credentialsProvider is not set");
            return credentialsProvider.getCredentials();
        }

        @Override
        public void refresh() {
            if (credentialsProvider != null) {
                credentialsProvider.refresh();
            }
        }

        public void shutdown() throws IOException {
            if (credentialsProvider != null) {
                IOUtils.close(credentialsProvider, () -> stsClient.shutdown());
            }
        }
    }

    static class ErrorLoggingCredentialsProvider implements AWSCredentialsProvider {

        private final AWSCredentialsProvider delegate;
        private final Logger logger;

        ErrorLoggingCredentialsProvider(AWSCredentialsProvider delegate, Logger logger) {
            this.delegate = Objects.requireNonNull(delegate);
            this.logger = Objects.requireNonNull(logger);
        }

        @Override
        public AWSCredentials getCredentials() {
            try {
                return delegate.getCredentials();
            } catch (Exception e) {
                logger.error(() -> "Unable to load credentials from " + delegate, e);
                throw e;
            }
        }

        @Override
        public void refresh() {
            try {
                delegate.refresh();
            } catch (Exception e) {
                logger.error(() -> "Unable to refresh " + delegate, e);
                throw e;
            }
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

    static final RetryPolicy RETRYABLE_403_RETRY_POLICY = RetryPolicy.builder()
        .withRetryCondition((originalRequest, exception, retriesAttempted) -> {
            if (PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION.shouldRetry(originalRequest, exception, retriesAttempted)) {
                return true;
            }
            if (exception instanceof AmazonServiceException ase) {
                return ase.getStatusCode() == HttpStatus.SC_FORBIDDEN && "InvalidAccessKeyId".equals(ase.getErrorCode());
            }
            return false;
        })
        .withBackoffStrategy(PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY)
        .withMaxErrorRetry(PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY)
        .withHonorMaxErrorRetryInClientConfig(true)
        .withHonorDefaultMaxErrorRetryInRetryMode(true)
        .withHonorDefaultBackoffStrategyInRetryMode(true)
        .build();
}
