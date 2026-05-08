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
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

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
    public Set<String> supportedSchemes() {
        return Set.of("gs");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        StorageProviderFactory gcsFactory = StorageProviderFactory.of(
            () -> new GcsStorageProvider((GcsConfiguration) null),
            GcsConfiguration::fromQueryConfig,
            GcsStorageProvider::new
        );
        return Map.of("gs", gcsFactory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator v = new FileDataSourceValidator("gcs", GcsConfiguration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }
}
