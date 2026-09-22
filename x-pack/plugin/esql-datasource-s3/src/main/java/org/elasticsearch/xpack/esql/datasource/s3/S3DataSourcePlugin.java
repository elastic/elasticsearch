/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.watcher.ResourceWatcherService;
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
import java.util.function.Function;

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
     * IRSA web-identity provider, built once on the first {@link #storageProviders} call. The provider
     * self-disables ({@code isActive() == false}) when {@code AWS_WEB_IDENTITY_TOKEN_FILE} is unset, so
     * non-EKS deployments incur no cost beyond construction. Released by {@link #close()}.
     */
    private CustomWebIdentityTokenCredentialsProvider webIdentityProvider;

    /**
     * Pod Identity container-credentials provider, built once alongside the IRSA provider. Reads the
     * entitled token file itself rather than redirecting the JVM-wide
     * {@code aws.containerAuthorizationTokenFile} system property. Released by {@link #close()}.
     */
    private EsqlContainerCredentialsProvider containerCredentialsProvider;

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
        WorkloadIdentitySources sources = initWorkloadIdentitySources(services);
        // Size the async client's connection pool from the single external-read concurrency knob
        // (esql.external.max_concurrent_requests), so the SDK pool matches the per-scheme permit ceiling.
        // services.settings() is the node Settings threaded through the SPI — the path that reaches the client build.
        int maxConnections = ExternalSourceSettings.blobStoreConcurrency(services.settings());
        StorageProviderFactory s3Factory = StorageProviderFactory.of(
            () -> new S3StorageProvider(null, sources.webIdentity(), sources.containerCredentials(), maxConnections),
            S3Configuration::fromQueryConfig,
            cfg -> new S3StorageProvider(cfg, sources.webIdentity(), sources.containerCredentials(), maxConnections)
        );
        return Map.of("s3", s3Factory, "s3a", s3Factory, "s3n", s3Factory);
    }

    /**
     * Builds the IRSA and Pod Identity providers exactly once from the node-level services threaded
     * through the SPI. Returns both (each possibly inactive) so callers can hand them to the
     * {@link S3StorageProvider} credentials chain.
     */
    private synchronized WorkloadIdentitySources initWorkloadIdentitySources(StorageProviderServices services) {
        if (closed) {
            return new WorkloadIdentitySources(null, null);
        }
        if (workloadIdentityInitialized == false) {
            buildWorkloadIdentitySources(services.environment(), services.resourceWatcherService(), System::getenv);
        }
        return new WorkloadIdentitySources(webIdentityProvider, containerCredentialsProvider);
    }

    /**
     * Test seam: wires workload-identity sources with an injectable env lookup so unit tests can
     * assert Pod Identity behaviour without manipulating real {@code System.getenv} state. Must be
     * called before {@link #storageProviders} on a fresh plugin instance.
     */
    synchronized void initializeWorkloadIdentityForTesting(
        Environment environment,
        ResourceWatcherService resourceWatcherService,
        Function<String, String> envLookup
    ) {
        if (workloadIdentityInitialized || closed) {
            throw new IllegalStateException("workload-identity sources already initialized or plugin closed");
        }
        buildWorkloadIdentitySources(environment, resourceWatcherService, envLookup);
    }

    /**
     * Builds Pod Identity first, then IRSA. Pod Identity is first so a construction failure there
     * cannot leave a live IRSA provider (STS client / file watcher) to leak when
     * {@code workloadIdentityInitialized} stays false and init is retried. If IRSA construction
     * throws after Pod Identity succeeded, the container provider is closed in the {@code finally}.
     */
    private void buildWorkloadIdentitySources(
        Environment environment,
        ResourceWatcherService resourceWatcherService,
        Function<String, String> envLookup
    ) {
        EsqlContainerCredentialsProvider container = null;
        CustomWebIdentityTokenCredentialsProvider irsa = null;
        try {
            // Pod Identity: own the container-credentials exchange so we never write the JVM-global
            // aws.containerAuthorizationTokenFile system property (which would redirect repository-s3
            // and every other AWS SDK client in the process).
            container = new EsqlContainerCredentialsProvider(environment, resourceWatcherService, envLookup);
            // IRSA web-identity provider: file watcher and STS client live for the node lifetime.
            irsa = new CustomWebIdentityTokenCredentialsProvider(environment, Clock.systemUTC(), resourceWatcherService, envLookup);
            containerCredentialsProvider = container;
            webIdentityProvider = irsa;
            workloadIdentityInitialized = true;
            if (containerCredentialsProvider.isActive()) {
                LOGGER.debug(
                    "Configured EKS Pod Identity for S3 data sources via entitled token at [{}]",
                    EsqlContainerCredentialsProvider.POD_IDENTITY_TOKEN_FILE_LOCATION
                );
            }
            // Ownership transferred to fields; clear locals so finally does not close them.
            container = null;
            irsa = null;
        } finally {
            IOUtils.closeWhileHandlingException(container, irsa);
        }
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator v = new FileDataSourceValidator("s3", S3Configuration::fromMap, supportedSchemes()).withAdditionalDatasetKeys(
            Set.of("region")
        )
            .withDeprecatedDatasourceKey(
                "region",
                "[region] on a data source is deprecated and will be ignored; "
                    + "set [region] on the dataset instead, or [sts_region] for the STS endpoint region on a federated source, "
                    + "or omit it to have the bucket region detected automatically"
            )
            .withResourceCheck(S3ResourceCheck::validate);
        return Map.of(v.type(), v);
    }

    @Override
    public Set<String> datasourceSecretSettingNames() {
        return S3Configuration.secretFieldNames();
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        try {
            // Both providers are Closeable; IOUtils.close tolerates null.
            IOUtils.close(webIdentityProvider, containerCredentialsProvider);
        } finally {
            webIdentityProvider = null;
            containerCredentialsProvider = null;
        }
    }

    private record WorkloadIdentitySources(
        CustomWebIdentityTokenCredentialsProvider webIdentity,
        EsqlContainerCredentialsProvider containerCredentials
    ) {}
}
