/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.local.LocalStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
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

    public StorageManager(StorageProviderRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("registry cannot be null");
        }
        this.registry = registry;
    }

    public StorageProvider provider(StoragePath path) {
        return registry.provider(path);
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
        // Providers created via registry are not tracked, so there's nothing to close.
        // Providers created via newStorageObject with config are short-lived and not tracked.
        // This method satisfies the Closeable interface but is effectively a no-op.
    }

    private StorageProvider createProviderWithConfig(StoragePath path, String scheme, Object config) {
        // Route S3 schemes (s3://, s3a://, s3n://) - check registry for S3 provider
        if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            if (registry.hasProvider(scheme)) {
                return registry.provider(path);
            }
            throw new IllegalArgumentException(
                "S3 storage provider not available. " + "Please install the S3 data source plugin to enable S3 support."
            );
        }

        // Route HTTP schemes (http://, https://) to HttpStorageProvider
        if (scheme.equals("http") || scheme.equals("https")) {
            // If config is wrong type, try registry first
            boolean isHttpConfig = config instanceof HttpConfiguration;
            boolean isMapConfig = config instanceof Map;
            if (config != null && isHttpConfig == false && isMapConfig == false) {
                if (registry.hasProvider(scheme)) {
                    // Use registry-based provider instead
                    return registry.provider(path);
                }
                throw new IllegalArgumentException("HTTP scheme requires HttpConfiguration, got: " + config.getClass().getName());
            }

            // If config is null, HttpConfiguration, or Map, try registry first (preferred approach)
            if (registry.hasProvider(scheme)) {
                return registry.provider(path);
            }

            // HttpStorageProvider requires an ExecutorService, which we don't have here.
            // This is a limitation - HttpStorageProvider should be registered in the registry
            // with an executor rather than created via this method.
            throw new IllegalArgumentException(
                "HttpStorageProvider requires an ExecutorService. "
                    + "Please register HttpStorageProvider in the registry with an executor, "
                    + "or use newStorageObject(String) to use a registered provider."
            );
        }

        // Route file:// scheme to LocalStorageProvider
        if (scheme.equals("file")) {
            boolean isMapConfig = config instanceof Map;
            boolean isEmptyMap = isMapConfig && ((Map<?, ?>) config).isEmpty();
            if (config != null && (isMapConfig == false || isEmptyMap == false)) {
                throw new IllegalArgumentException("file:// scheme does not accept configuration, got: " + config.getClass().getName());
            }
            return new LocalStorageProvider();
        }

        // Unknown scheme - try registry as fallback
        if (registry.hasProvider(scheme)) {
            return registry.provider(path);
        }

        throw new IllegalArgumentException(
            "Unsupported storage scheme: "
                + scheme
                + ". "
                + "Supported schemes: http://, https://, file://. "
                + "For S3 support, install the S3 data source plugin."
        );
    }
}
