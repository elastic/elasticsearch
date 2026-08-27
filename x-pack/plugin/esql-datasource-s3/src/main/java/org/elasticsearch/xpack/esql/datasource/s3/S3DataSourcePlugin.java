/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.SdkSystemSetting;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderServices;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;

/**
 * Data source plugin providing S3 storage support for ESQL.
 * Supports s3://, s3a://, and s3n:// URI schemes.
 *
 * <p>Workload-identity sources (EKS IRSA + Pod Identity) are wired lazily on the first
 * {@link #storageProviders(StorageProviderServices)} call rather than at node start, because the
 * instance whose {@code storageProviders} runs is created reflectively by ESQL's SPI discovery and
 * never receives {@code createComponents} (and therefore no {@code PluginServices}). The node-level
 * {@link Environment} and {@code ResourceWatcherService} it needs arrive through the
 * {@link StorageProviderServices} threaded into the SPI. {@code DataSourceModule} owns this
 * instance's {@link #close()}.
 */
public class S3DataSourcePlugin extends Plugin implements DataSourcePlugin {

    private static final Logger LOGGER = LogManager.getLogger(S3DataSourcePlugin.class);

    /**
     * Operator-managed symlink for EKS Pod Identity, relative to {@code ${ES_PATH_CONF}}.
     * The plugin's {@code entitlement-policy.yaml} grants read access to this fixed location.
     */
    public static final String POD_IDENTITY_TOKEN_FILE_LOCATION = "esql-datasource-s3/eks-pod-identity-token";

    /**
     * IRSA web-identity provider, built once on the first {@link #storageProviders} call. The provider
     * self-disables ({@code isActive() == false}) when {@code AWS_WEB_IDENTITY_TOKEN_FILE} is unset, so
     * non-EKS deployments incur no cost beyond construction. Released by {@link #close()}.
     */
    private CustomWebIdentityTokenCredentialsProvider webIdentityProvider;

    /**
     * The entitled path we set on the {@code aws.containerAuthorizationTokenFile} sysprop in
     * {@link #maybeOverrideContainerAuthTokenFile}, so {@link #close()} clears the sysprop only when it
     * still matches what we set (i.e. nothing else has clobbered it). {@code null} when we did not set it.
     */
    private String podIdentitySyspropSetTo;

    /** Guards one-time wiring of the workload-identity sources; mutated only under {@code synchronized(this)}. */
    private boolean workloadIdentityInitialized;

    /** Set to {@code true} by {@link #close()}; prevents post-shutdown init from leaking resources. */
    private boolean closed;

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("s3", "s3a", "s3n");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(StorageProviderServices services) {
        CustomWebIdentityTokenCredentialsProvider provider = initWorkloadIdentitySources(services);
        // Size the async client's connection pool from the single external-read concurrency knob
        // (esql.external.max_concurrent_requests), so the SDK pool matches the per-scheme permit ceiling.
        // services.settings() is the node Settings threaded through the SPI — the path that reaches the client build.
        int maxConnections = ExternalSourceSettings.blobStoreConcurrency(services.settings());
        StorageProviderFactory s3Factory = StorageProviderFactory.of(
            () -> new S3StorageProvider(null, provider, maxConnections),
            S3Configuration::fromQueryConfig,
            cfg -> new S3StorageProvider(cfg, provider, maxConnections)
        );
        return Map.of("s3", s3Factory, "s3a", s3Factory, "s3n", s3Factory);
    }

    /**
     * Builds the IRSA web-identity provider and applies the EKS Pod Identity sysprop redirect exactly
     * once, from the node-level services threaded through the SPI. Returns the (possibly inactive) IRSA
     * provider so callers can hand it to the {@link S3StorageProvider} credentials chain.
     */
    private synchronized CustomWebIdentityTokenCredentialsProvider initWorkloadIdentitySources(StorageProviderServices services) {
        if (closed) {
            return null;
        }
        if (workloadIdentityInitialized == false) {
            // EKS Pod Identity: AWS SDK v2's ContainerCredentialsProvider is final and reads
            // AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE directly. The Kubernetes-injected path
            // (/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token) is
            // outside the entitlement-grantable area, so we redirect the SDK at the entitled symlink
            // location via the JVM system property override (sysprop has higher precedence than env
            // var in SdkSystemSetting). This is JVM-global; document the implication for any other
            // AWS SDK ContainerCredentialsProvider users in the same JVM.
            //
            // Done before building the IRSA provider on purpose: it allocates nothing but can throw on a
            // conflicting sysprop, so failing here leaves no half-built IRSA provider (file watcher, STS
            // client) to leak when storageProviders is retried after the misconfiguration is fixed.
            maybeOverrideContainerAuthTokenFile(services.environment());

            // Build the IRSA web-identity provider so the file watcher and STS client live for the
            // lifetime of the node rather than being rebuilt per query.
            webIdentityProvider = new CustomWebIdentityTokenCredentialsProvider(
                services.environment(),
                Clock.systemUTC(),
                services.resourceWatcherService()
            );

            workloadIdentityInitialized = true;
        }
        return webIdentityProvider;
    }

    @SuppressForbidden(reason = "JVM system property is the only override knob for the final AWS SDK ContainerCredentialsProvider")
    private void maybeOverrideContainerAuthTokenFile(Environment environment) {
        String envValue = System.getenv(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable());
        if (Strings.hasText(envValue) == false) {
            return;
        }
        if (environment == null) {
            LOGGER.warn(
                "Cannot redirect EKS Pod Identity token file: node environment is unavailable "
                    + "(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE=[{}] will be used as-is)",
                envValue
            );
            return;
        }
        String entitledPath = environment.configDir().resolve(POD_IDENTITY_TOKEN_FILE_LOCATION).toString();
        String existing = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        if (existing != null && existing.equals(entitledPath) == false) {
            // Something else already pinned the SDK to a different token-file path. We cannot override it
            // without breaking that component, and leaving it in place would silently point our chain's
            // ContainerCredentialsProvider at the wrong token file. Fail fast rather than mis-authenticate.
            throw new IllegalStateException(
                Strings.format(
                    "Cannot configure EKS Pod Identity for the S3 data source: the JVM system property [%s] is already set to [%s], "
                        + "but it must point at the entitled token symlink [%s]. Remove the conflicting "
                        + "-D%s setting (or align it with the entitled location) before starting the node.",
                    SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property(),
                    existing,
                    entitledPath,
                    SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property()
                )
            );
        }
        System.setProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property(), entitledPath);
        podIdentitySyspropSetTo = entitledPath;
        LOGGER.debug("Redirected AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE [{}] to [{}] for EKS Pod Identity", envValue, entitledPath);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator v = new FileDataSourceValidator("s3", S3Configuration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        try {
            // CustomWebIdentityTokenCredentialsProvider is Closeable; IOUtils.close tolerates null.
            IOUtils.close(webIdentityProvider);
            webIdentityProvider = null;
        } finally {
            clearPodIdentitySysprop();
        }
    }

    /**
     * Symmetric counterpart to {@link #maybeOverrideContainerAuthTokenFile}. Clears the JVM-global
     * {@code aws.containerAuthorizationTokenFile} sysprop, but only if its current value still equals
     * the entitled path we set. If something else clobbered it after we set it, we leave it alone.
     */
    @SuppressForbidden(reason = "symmetric cleanup of the JVM-global sysprop set by maybeOverrideContainerAuthTokenFile")
    private void clearPodIdentitySysprop() {
        if (podIdentitySyspropSetTo == null) {
            return;
        }
        String current = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        if (podIdentitySyspropSetTo.equals(current)) {
            System.clearProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        }
        podIdentitySyspropSetTo = null;
    }
}
