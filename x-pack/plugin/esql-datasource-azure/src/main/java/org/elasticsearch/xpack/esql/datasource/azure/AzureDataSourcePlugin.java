/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Set;

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

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("wasbs", "wasb");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        StorageProviderFactory azureFactory = new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return new AzureStorageProvider((AzureConfiguration) null);
            }

            @Override
            public StorageProvider create(Settings settings, Map<String, Object> config) {
                if (config == null || config.isEmpty()) {
                    return create(settings);
                }
                AzureConfiguration azureConfig = AzureConfiguration.fromMap(config);
                return new AzureStorageProvider(azureConfig);
            }
        };
        return Map.of("wasbs", azureFactory, "wasb", azureFactory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators() {
        DataSourceValidator v = new FileDataSourceValidator("azure_blob", AzureConfiguration::fromMap, Set.of("wasbs://", "wasb://"));
        return Map.of(v.type(), v);
    }
}
