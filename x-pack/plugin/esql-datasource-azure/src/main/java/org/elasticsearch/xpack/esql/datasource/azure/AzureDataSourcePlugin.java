/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderServices;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Data source plugin providing Azure Blob Storage support for ESQL.
 * Supports the wasbs:// and wasb:// URI schemes.
 * <p>
 * Usage in ESQL:
 * <pre>
 *   EXTERNAL "wasbs://account.blob.core.windows.net/container/path/data.parquet"
 *   EXTERNAL "wasbs://account.blob.core.windows.net/container/path/data.parquet"
 *     WITH {"account": "myaccount", "key": "...", "endpoint": "https://myaccount.blob.core.windows.net"}
 * </pre>
 * <p>
 * The node-level {@link Environment} needed to resolve the AKS Workload Identity token symlink
 * ({@code ${ES_PATH_CONF}/esql-datasource-azure/azure-federated-token}) arrives through the
 * {@link StorageProviderServices} threaded into {@link #storageProviders(StorageProviderServices)}.
 * Without it the workload-identity chain is {@code ManagedIdentity}-only.
 * <p>
 * Azure is not in the released ship set yet (S3 is the released cloud provider), so registration is
 * gated on {@link #ESQL_EXTERNAL_AZURE_FEATURE_FLAG}: available in snapshot/development builds, disabled
 * in release. When the gate is off the {@code wasbs}/{@code wasb} schemes are not registered, so an
 * Azure source resolves to the generic "Unsupported storage scheme" rejection.
 */
public class AzureDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates the Azure storage provider. Snapshot-on, release-off; override in release with
     * {@code -Des.esql_external_azure_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_AZURE_FEATURE_FLAG = new FeatureFlag("esql_external_azure");

    private static boolean enabled() {
        return ESQL_EXTERNAL_AZURE_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of("wasbs", "wasb");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(StorageProviderServices services) {
        if (enabled() == false) {
            return Map.of();
        }
        Environment environment = services.environment();
        ExecutorService executor = services.executor();
        // Size the connection pool from the single external-read concurrency knob
        // (esql.external.max_concurrent_requests), so the SDK pool matches the per-scheme permit ceiling.
        // services.settings() is the node Settings threaded through the SPI — the path that reaches the client build.
        int maxConnections = ExternalSourceSettings.blobStoreConcurrency(services.settings());
        StorageProviderFactory azureFactory = StorageProviderFactory.of(
            () -> new AzureStorageProvider(null, environment, executor, maxConnections),
            AzureConfiguration::fromQueryConfig,
            cfg -> new AzureStorageProvider(cfg, environment, executor, maxConnections)
        );
        return Map.of("wasbs", azureFactory, "wasb", azureFactory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        DataSourceValidator v = new FileDataSourceValidator("azure", AzureConfiguration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }
}
