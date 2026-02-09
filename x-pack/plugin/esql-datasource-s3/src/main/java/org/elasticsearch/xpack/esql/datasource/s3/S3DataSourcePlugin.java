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
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;

/**
 * Data source plugin providing S3 storage support for ESQL.
 * Supports s3://, s3a://, and s3n:// URI schemes.
 */
public class S3DataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        StorageProviderFactory s3Factory = new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return new S3StorageProvider(null);
            }

            @Override
            public StorageProvider create(Settings settings, Map<String, Object> config) {
                if (config == null || config.isEmpty()) {
                    return create(settings);
                }
                S3Configuration s3Config = S3Configuration.fromFields(
                    (String) config.get("access_key"),
                    (String) config.get("secret_key"),
                    (String) config.get("endpoint"),
                    (String) config.get("region")
                );
                return new S3StorageProvider(s3Config);
            }
        };
        return Map.of("s3", s3Factory, "s3a", s3Factory, "s3n", s3Factory);
    }
}
