/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Format-agnostic manager for creating StorageObject instances from paths.
 *
 * <p>This class provides a high-level API for accessing storage objects without
 * knowledge of specific file formats (Parquet, CSV, etc.). It routes requests
 * to appropriate StorageProvider implementations based on the URI scheme:
 * <ul>
 *   <li>{@code s3://}, {@code s3a://}, {@code s3n://} → S3StorageProvider</li>
 *   <li>{@code gs://} → GcsStorageProvider</li>
 *   <li>{@code http://}, {@code https://} → HttpStorageProvider</li>
 *   <li>{@code file://} → LocalStorageProvider</li>
 * </ul>
 *
 * <p>The manager uses a StorageProviderRegistry for scheme-based provider lookup,
 * allowing pluggable provider discovery. When configuration is provided via
 * {@link #newStorageObject(String, Object)}, providers are created directly
 * with the specified configuration, bypassing the registry.
 *
 * <p>This design ensures format-agnostic storage access - the manager has no
 * knowledge of Parquet, CSV, or any other file format. Format-specific logic
 * is handled by FormatReader implementations that consume StorageObject instances.
 *
 * <p><b>Note:</b> This class is not part of the SPI as it depends on specific
 * provider implementations. It lives in the datasources package alongside
 * the provider registry.
 */
public class StorageManager implements Closeable {
    private final StorageProviderRegistry registry;
    private final Settings settings;
    private final List<StorageProvider> perQueryProviders = new ArrayList<>();

    public StorageManager(StorageProviderRegistry registry, Settings settings) {
        if (registry == null) {
            throw new IllegalArgumentException("registry cannot be null");
        }
        this.registry = registry;
        this.settings = settings;
    }

    public StorageProvider provider(StoragePath path) {
        return registry.provider(path);
    }

    public StorageProvider provider(StoragePath path, Object config) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        return createProviderWithConfig(path, scheme, config);
    }

    public StorageObject newStorageObject(String path, Object config) {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }

        StoragePath storagePath = StoragePath.of(path);
        String scheme = storagePath.scheme().toLowerCase(Locale.ROOT);

        StorageProvider provider = createProviderWithConfig(storagePath, scheme, config);
        return provider.newObject(storagePath);
    }

    public StorageObject newStorageObject(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }

        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = registry.provider(storagePath);
        return provider.newObject(storagePath);
    }

    public boolean supportsScheme(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return false;
        }
        return registry.hasProvider(scheme.toLowerCase(Locale.ROOT));
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(perQueryProviders);
    }

    @SuppressWarnings("unchecked")
    private StorageProvider createProviderWithConfig(StoragePath path, String scheme, Object config) {
        if (registry.hasProvider(scheme) == false) {
            throw new IllegalArgumentException(
                "Unsupported storage scheme: "
                    + scheme
                    + ". "
                    + "No storage provider registered for this scheme. "
                    + "Install the appropriate data source plugin (e.g., esql-datasource-http for http/https/file, "
                    + "esql-datasource-s3 for s3, esql-datasource-gcs for gs)."
            );
        }

        // When config is provided, create a fresh provider with the per-query config
        if (config instanceof Map<?, ?> configMap && configMap.isEmpty() == false) {
            StorageProvider provider = registry.createProvider(scheme, settings, (Map<String, Object>) configMap);
            perQueryProviders.add(provider);
            return provider;
        }

        // Fall back to the default registered provider
        return registry.provider(path);
    }
}
