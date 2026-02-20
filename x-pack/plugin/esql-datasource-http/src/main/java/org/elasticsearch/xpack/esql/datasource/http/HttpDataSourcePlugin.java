/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.http.local.LocalStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Data source plugin that provides HTTP/HTTPS and local file storage providers
 * for ESQL external data sources.
 *
 * <p>This plugin provides:
 * <ul>
 *   <li>HTTP/HTTPS storage provider for reading from web servers</li>
 *   <li>Local file system storage provider for testing and development</li>
 * </ul>
 *
 * <p>These implementations have no heavy external dependencies and use JDK's
 * built-in {@code HttpClient} and {@code java.nio} APIs.
 *
 * <p>The executor for async HTTP I/O is injected via the
 * {@link DataSourcePlugin#storageProviders(Settings, ExecutorService)} SPI method,
 * backed by the ES GENERIC thread pool.
 */
public class HttpDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        return Map.of(
            "http",
            s -> new HttpStorageProvider(HttpConfiguration.defaults(), executor),
            "https",
            s -> new HttpStorageProvider(HttpConfiguration.defaults(), executor),
            "file",
            s -> new LocalStorageProvider()
        );
    }
}
