/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.WatcherHandle;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_ARN;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_SESSION_NAME;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE;

/**
 * Custom {@link StsWebIdentityTokenFileCredentialsProvider} for the ESQL S3 datasource plugin
 * supporting EKS IRSA (IAM Roles for Service Accounts).
 *
 * <p>Mirrors {@code repository-s3}'s provider of the same name. Differences from the SDK default:
 * <ul>
 *   <li>Reads the web identity token not from {@code AWS_WEB_IDENTITY_TOKEN_FILE} (which in EKS
 *       points to a Kubernetes-injected path the entitlement system blocks) but from a symlink
 *       at {@code ${ES_PATH_CONF}/esql-datasource-s3/aws-web-identity-token-file} that the
 *       operator must create. The plugin's {@code entitlement-policy.yaml} grants read access to
 *       this fixed location.</li>
 *   <li>Allows the STS endpoint to be overridden via the system property
 *       {@code org.elasticsearch.xpack.esql.datasource.s3.stsEndpointOverride} so integration
 *       tests can point at a local fixture.</li>
 *   <li>{@link #close()} releases both the {@link StsWebIdentityTokenFileCredentialsProvider}
 *       and its underlying {@link StsClient} on plugin shutdown.</li>
 * </ul>
 *
 * <p>The provider is "active" only when {@code AWS_WEB_IDENTITY_TOKEN_FILE} is set in the
 * environment AND a readable file exists at the entitled symlink location AND
 * {@code AWS_ROLE_ARN} is set. In every other case it stays inactive and {@link #isActive()}
 * returns {@code false} so callers can omit it from the credentials chain.
 */
public class CustomWebIdentityTokenCredentialsProvider implements AwsCredentialsProvider, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(CustomWebIdentityTokenCredentialsProvider.class);

    /** Operator-managed symlink location, relative to {@code ${ES_PATH_CONF}}. */
    public static final String WEB_IDENTITY_TOKEN_FILE_LOCATION = "esql-datasource-s3/aws-web-identity-token-file";

    /** Test-only STS endpoint override, scoped to this plugin so it does not collide with {@code repository-s3}. */
    static final String STS_ENDPOINT_OVERRIDE_PROPERTY = "org.elasticsearch.xpack.esql.datasource.s3.stsEndpointOverride";

    private StsWebIdentityTokenFileCredentialsProvider credentialsProvider;
    private StsClient securityTokenServiceClient;
    private WatcherHandle<FileWatcher> watcherHandle;

    public CustomWebIdentityTokenCredentialsProvider(Environment environment, Clock clock, ResourceWatcherService resourceWatcherService) {
        this(environment, clock, resourceWatcherService, System::getenv);
    }

    /**
     * Test seam: env-var lookups are routed through {@code envLookup} so unit tests can inject a
     * stub map without manipulating real {@code System.getenv} state.
     */
    CustomWebIdentityTokenCredentialsProvider(
        Environment environment,
        Clock clock,
        ResourceWatcherService resourceWatcherService,
        Function<String, String> envLookup
    ) {
        final String webIdentityTokenFileEnvVar = envLookup.apply(AWS_WEB_IDENTITY_TOKEN_FILE.name());
        if (Strings.hasText(webIdentityTokenFileEnvVar) == false) {
            return;
        }
        if (environment == null) {
            LOGGER.warn(
                "Cannot configure EKS IRSA: node environment is unavailable "
                    + "(AWS_WEB_IDENTITY_TOKEN_FILE=[{}] will not be used for ESQL S3 reads)",
                webIdentityTokenFileEnvVar
            );
            return;
        }

        final Path webIdentityTokenFileLocation = environment.configDir().resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
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
                        a symlink to this token file at location [%s] and this location is not readable.""",
                    webIdentityTokenFileEnvVar,
                    webIdentityTokenFileLocation
                )
            );
        }

        final String roleArn = envLookup.apply(AWS_ROLE_ARN.name());
        if (Strings.hasText(roleArn) == false) {
            LOGGER.warn(
                """
                    Cannot use AWS Web Identity Tokens: AWS_WEB_IDENTITY_TOKEN_FILE is defined as [{}] but Elasticsearch requires \
                    the AWS_ROLE_ARN environment variable to be set to the ARN of the role and this variable is not set.""",
                webIdentityTokenFileEnvVar
            );
            return;
        }

        final String roleSessionName = Objects.requireNonNullElseGet(
            envLookup.apply(AWS_ROLE_SESSION_NAME.name()),
            // Mimic the SDK default when the session name is unset.
            () -> "aws-sdk-java-" + clock.millis()
        );

        final var securityTokenServiceClientBuilder = StsClient.builder();
        final String endpointOverride = System.getProperty(STS_ENDPOINT_OVERRIDE_PROPERTY, null);
        if (endpointOverride != null) {
            securityTokenServiceClientBuilder.endpointOverride(URI.create(endpointOverride));
        }
        securityTokenServiceClientBuilder.credentialsProvider(AnonymousCredentialsProvider.create());
        securityTokenServiceClient = securityTokenServiceClientBuilder.build();

        try {
            credentialsProvider = StsWebIdentityTokenFileCredentialsProvider.builder()
                .roleArn(roleArn)
                .roleSessionName(roleSessionName)
                .webIdentityTokenFile(webIdentityTokenFileLocation)
                .stsClient(securityTokenServiceClient)
                .build();

            setupFileWatcherToRefreshCredentials(webIdentityTokenFileLocation, resourceWatcherService);
        } catch (Exception e) {
            Releasables.close(releasableFromSdkCloseable(credentialsProvider), releasableFromSdkCloseable(securityTokenServiceClient));
            throw e;
        }
    }

    /**
     * Reruns {@link StsWebIdentityTokenFileCredentialsProvider#resolveCredentials()} whenever the
     * symlinked token file changes on disk so rotated EKS service-account tokens are picked up
     * before the cached credentials expire.
     */
    private void setupFileWatcherToRefreshCredentials(Path webIdentityTokenFileSymlink, ResourceWatcherService resourceWatcherService) {
        FileWatcher watcher = new FileWatcher(webIdentityTokenFileSymlink);
        watcher.addListener(new FileChangesListener() {

            @Override
            public void onFileCreated(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileChanged(Path file) {
                if (file.equals(webIdentityTokenFileSymlink)) {
                    LOGGER.debug("AWS web identity token file [{}] changed, updating credentials", file);
                    credentialsProvider.resolveCredentials();
                }
            }
        });
        try {
            watcherHandle = resourceWatcherService.add(watcher, ResourceWatcherService.Frequency.LOW);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching AWS web identity token file [{}]", e, webIdentityTokenFileSymlink);
        }
    }

    /**
     * Returns {@code true} when the provider was successfully wired (env var present, symlinked
     * token readable, role ARN set). Callers gate inclusion of this provider in the credentials
     * chain on this signal.
     */
    public boolean isActive() {
        return credentialsProvider != null;
    }

    @Override
    public void close() throws IOException {
        if (watcherHandle != null) {
            watcherHandle.stop();
        }
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

    @Override
    public String toString() {
        return "CustomWebIdentityTokenCredentialsProvider[" + credentialsProvider + "]";
    }
}
