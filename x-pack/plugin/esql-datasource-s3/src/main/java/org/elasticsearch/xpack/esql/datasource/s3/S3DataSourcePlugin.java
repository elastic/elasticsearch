/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

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
 * Data source plugin providing S3 storage support for ESQL.
 * Supports s3://, s3a://, and s3n:// URI schemes.
 */
public class S3DataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("s3", "s3a", "s3n");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        StorageProviderFactory s3Factory = StorageProviderFactory.of(
            () -> new S3StorageProvider(null),
            S3Configuration::fromQueryConfig,
            S3StorageProvider::new
        );
        return Map.of("s3", s3Factory, "s3a", s3Factory, "s3n", s3Factory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator v = new FileDataSourceValidator("s3", S3Configuration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }
}
