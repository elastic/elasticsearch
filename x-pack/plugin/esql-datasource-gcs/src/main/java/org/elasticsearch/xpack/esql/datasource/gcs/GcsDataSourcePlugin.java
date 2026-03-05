/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Data source plugin providing Google Cloud Storage support for ESQL.
 * Supports the gs:// URI scheme.
 * <p>
 * Usage in ESQL:
 * <pre>
 *   EXTERNAL "gs://my-bucket/data/sales.parquet"
 *   EXTERNAL "gs://my-bucket/data/sales.parquet" WITH (credentials="{ ... service account JSON ... }", project_id="my-project")
 * </pre>
 */
public class GcsDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        StorageProviderFactory gcsFactory = new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return new GcsStorageProvider((GcsConfiguration) null);
            }

            @Override
            public StorageProvider create(Settings settings, Map<String, Object> config) {
                if (config == null || config.isEmpty()) {
                    return create(settings);
                }
                GcsConfiguration gcsConfig = GcsConfiguration.fromFields(
                    Objects.toString(config.get("credentials"), null),
                    Objects.toString(config.get("project_id"), null),
                    Objects.toString(config.get("endpoint"), null),
                    Objects.toString(config.get("token_uri"), null)
                );
                return new GcsStorageProvider(gcsConfig);
            }
        };
        return Map.of("gs", gcsFactory);
    }
}
