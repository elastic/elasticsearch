/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.http.local.LocalStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
 *
 * <p>Registration of both providers is gated per scheme. HTTP/HTTPS requires
 * {@link #ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG} (snapshot-on, release-off). Local file access
 * is on by default — the security gate is the {@code esql.datasource.local_allowed_paths} node setting,
 * not a feature flag. When a gate is off the relevant schemes are not registered, so any query targeting
 * them resolves to the generic "unsupported storage scheme" rejection.
 */
public class HttpDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates provisioning HTTP/HTTPS ({@code http://}, {@code https://}) data sources/datasets. Snapshot-on,
     * release-off; override in release with {@code -Des.esql_external_datasources_http_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG = new FeatureFlag("esql_external_datasources_http");

    /**
     * Gates local-file ({@code file://}) data sources/datasets. Snapshot-on, release-off; override in release
     * with {@code -Des.esql_external_datasources_local_feature_flag_enabled=true}.
     * Access is further controlled at runtime by the {@code esql.datasource.local_allowed_paths} node setting.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG = new FeatureFlag("esql_external_datasources_local");

    private static boolean httpEnabled() {
        return ESQL_EXTERNAL_DATASOURCES_HTTP_FEATURE_FLAG.isEnabled();
    }

    private static boolean localEnabled() {
        return ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        Set<String> schemes = new HashSet<>();
        if (httpEnabled()) {
            schemes.add("http");
            schemes.add("https");
        }
        if (localEnabled()) {
            schemes.add("file");
        }
        return Set.copyOf(schemes);
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        if (httpEnabled() == false && localEnabled() == false) {
            return Map.of();
        }
        Map<String, StorageProviderFactory> providers = new HashMap<>();
        if (httpEnabled()) {
            // http and https share one factory: the provider is scheme-agnostic and reads the scheme off each URI.
            StorageProviderFactory httpFactory = StorageProviderFactory.noConfigKeys(
                () -> new HttpStorageProvider(HttpConfiguration.defaults(), executor)
            );
            providers.put("http", httpFactory);
            providers.put("https", httpFactory);
        }
        if (localEnabled()) {
            providers.put("file", StorageProviderFactory.noConfigKeys(LocalStorageProvider::new));
        }
        return Map.copyOf(providers);
    }

    /**
     * Registers CRUD validators for the unauthenticated {@code http} and {@code local} data source types,
     * each only when its gate is enabled. Both reuse {@link FileDataSourceValidator}: it enforces the
     * dataset resource scheme and (once {@code EsqlPlugin} attaches the format-config-key resolver)
     * validates dataset-level format options, while {@link NoAuthDataSourceConfiguration} accepts only an
     * explicit {@code auth=anonymous} and rejects any other datasource-level setting since these sources carry
     * no credentials or tunables.
     *
     * <p>The {@code http} type covers both {@code http://} and {@code https://}; the {@code local} type
     * covers {@code file://}. When a gate is disabled the type is absent, so a PUT for it fails with the
     * generic "unknown data source type" error from the CRUD layer.
     */
    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        Map<String, DataSourceValidator> validators = new HashMap<>();
        if (httpEnabled()) {
            DataSourceValidator http = new FileDataSourceValidator("http", NoAuthDataSourceConfiguration::fromMap, Set.of("http", "https"));
            validators.put(http.type(), http);
        }
        if (localEnabled()) {
            DataSourceValidator local = new FileDataSourceValidator("local", NoAuthDataSourceConfiguration::fromMap, Set.of("file"));
            validators.put(local.type(), local);
        }
        return validators;
    }
}
