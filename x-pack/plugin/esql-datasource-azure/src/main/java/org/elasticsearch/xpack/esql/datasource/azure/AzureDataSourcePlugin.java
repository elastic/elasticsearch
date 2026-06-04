/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Data source plugin providing Azure Blob Storage support for ESQL.
 * Supports the wasbs:// and wasb:// URI schemes.
 * <p>
 * Usage in ESQL:
 * <pre>
 *   EXTERNAL "wasbs://account.blob.core.windows.net/container/path/data.parquet"
 *   EXTERNAL "wasbs://account.blob.core.windows.net/container/path/data.parquet"
 *     WITH (account="myaccount", key="...", endpoint="https://myaccount.blob.core.windows.net")
 * </pre>
 */
public class AzureDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Node-level {@link Environment} captured during {@link #createComponents}. Static (rather
     * than instance) on purpose: {@link org.elasticsearch.xpack.esql.plugin.EsqlPlugin}'s
     * {@code DataSourcePlugin} SPI creates its own {@link AzureDataSourcePlugin} instance via
     * reflection (see {@code PluginsService#createExtension}), so the instance whose
     * {@link #storageProviders} runs is NOT the same one whose {@code createComponents} got the
     * {@link PluginServices}. Hoisting the reference to a static field lets both instances see
     * the same {@code Environment} for resolving the AKS federated-token symlink.
     */
    private static final AtomicReference<Environment> ENVIRONMENT = new AtomicReference<>();

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("wasbs", "wasb");
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        // Capture the node-level environment so we can resolve the AKS Workload Identity token
        // symlink (${ES_PATH_CONF}/esql-datasource-azure/azure-federated-token) when building
        // BlobServiceClients. Without it the workload-identity chain is ManagedIdentity-only.
        ENVIRONMENT.compareAndSet(null, services.environment());
        return List.of();
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        // Read the singleton inside the factory lambdas (not at method entry) so that a future
        // call ordering where storageProviders runs before createComponents still picks up the
        // environment on the next factory invocation, instead of permanently capturing null.
        // See ENVIRONMENT javadoc.
        StorageProviderFactory azureFactory = StorageProviderFactory.of(
            () -> new AzureStorageProvider((AzureConfiguration) null, ENVIRONMENT.get()),
            AzureConfiguration::fromQueryConfig,
            cfg -> new AzureStorageProvider(cfg, ENVIRONMENT.get())
        );
        return Map.of("wasbs", azureFactory, "wasb", azureFactory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator v = new FileDataSourceValidator("azure", AzureConfiguration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }
}
