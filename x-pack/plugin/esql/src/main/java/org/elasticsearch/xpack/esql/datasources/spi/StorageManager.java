/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.StorageProviderRegistry;
import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.local.LocalStorageProvider;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.datasources.s3.S3StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;

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
 */
public class StorageManager implements Closeable {
    private final StorageProviderRegistry registry;

    /**
     * Creates a StorageManager with a pre-configured registry.
     *
     * @param registry the storage provider registry containing registered providers
     * @throws IllegalArgumentException if registry is null
     */
    public StorageManager(StorageProviderRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("registry cannot be null");
        }
        this.registry = registry;
    }

    /**
     * Gets a StorageProvider for the given path using the registry.
     * The provider is obtained via scheme-based lookup from the registry.
     *
     * @param path the storage path
     * @return a StorageProvider instance for the path's scheme
     * @throws IllegalArgumentException if path is null or no provider is registered for the scheme
     */
    public StorageProvider getProvider(StoragePath path) {
        return registry.getProvider(path);
    }

    /**
     * Creates a StorageObject from a path string with optional configuration.
     *
     * <p>This method routes to the appropriate StorageProvider based on the URI scheme
     * and creates the provider with the given configuration. The config parameter
     * can be one of:
     * <ul>
     *   <li>{@link S3Configuration} for S3 schemes (s3://, s3a://, s3n://)</li>
     *   <li>{@link HttpConfiguration} for HTTP schemes (http://, https://)</li>
     *   <li>{@code null} or no config for file:// scheme (uses defaults)</li>
     * </ul>
     *
     * <p>For HTTP providers, an ExecutorService is required. If config is an
     * HttpConfiguration but no executor is available, this method will throw
     * an IllegalArgumentException. In practice, HttpStorageProvider should be
     * registered in the registry with an executor rather than created via this method.
     *
     * @param path the path string (e.g., "s3://bucket/file.csv", "https://example.com/data.parquet")
     * @param config the provider-specific configuration, or null to use defaults
     * @return a StorageObject instance for the given path
     * @throws IllegalArgumentException if path is null, invalid, or config is incompatible with scheme
     */
    public StorageObject newStorageObject(String path, Object config) {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }

        StoragePath storagePath = StoragePath.of(path);
        String scheme = storagePath.scheme().toLowerCase(Locale.ROOT);

        StorageProvider provider = createProviderWithConfig(storagePath, scheme, config);
        return provider.newObject(storagePath);
    }

    /**
     * Creates a StorageObject from a path string without configuration.
     * Uses the registry to obtain a provider for the path's scheme.
     *
     * @param path the path string (e.g., "s3://bucket/file.csv", "https://example.com/data.parquet")
     * @return a StorageObject instance for the given path
     * @throws IllegalArgumentException if path is null, invalid, or no provider is registered for the scheme
     */
    public StorageObject newStorageObject(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }

        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = registry.getProvider(storagePath);
        return provider.newObject(storagePath);
    }

    /**
     * Checks if a scheme is supported by a registered provider in the registry.
     *
     * @param scheme the URI scheme to check (e.g., "s3", "http", "file")
     * @return true if a provider is registered for the scheme, false otherwise
     */
    public boolean supportsScheme(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return false;
        }
        return registry.hasProvider(scheme.toLowerCase(Locale.ROOT));
    }

    /**
     * Closes all providers managed by this manager.
     *
     * <p>Note: Providers obtained via {@link #getProvider(StoragePath)} are created
     * by the registry and are not tracked by this manager. Those providers should
     * be closed by their callers. This method primarily serves to satisfy the
     * Closeable interface and can be used to clean up any internally managed resources.
     *
     * @throws IOException if an error occurs while closing
     */
    @Override
    public void close() throws IOException {
        // Providers created via registry are not tracked, so there's nothing to close.
        // Providers created via newStorageObject with config are short-lived and not tracked.
        // This method satisfies the Closeable interface but is effectively a no-op.
    }

    /**
     * Creates a StorageProvider with the given configuration for the specified scheme.
     * This method handles routing different schemes to their appropriate providers.
     *
     * @param path the storage path
     * @param scheme the normalized scheme (lowercase)
     * @param config the provider configuration, or null
     * @return a StorageProvider instance
     * @throws IllegalArgumentException if the scheme is not supported or config is incompatible
     */
    private StorageProvider createProviderWithConfig(StoragePath path, String scheme, Object config) {
        // Route S3 schemes (s3://, s3a://, s3n://) to S3StorageProvider
        if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            if (config == null) {
                return new S3StorageProvider(null);
            } else if (config instanceof S3Configuration s3Config) {
                return new S3StorageProvider(s3Config);
            } else {
                throw new IllegalArgumentException("S3 scheme requires S3Configuration, got: " + config.getClass().getName());
            }
        }

        // Route HTTP schemes (http://, https://) to HttpStorageProvider
        if (scheme.equals("http") || scheme.equals("https")) {
            // If config is wrong type (e.g., S3Configuration), try registry first
            if (config != null && config instanceof HttpConfiguration == false) {
                if (registry.hasProvider(scheme)) {
                    // Use registry-based provider instead
                    return registry.getProvider(path);
                }
                throw new IllegalArgumentException("HTTP scheme requires HttpConfiguration, got: " + config.getClass().getName());
            }

            // If config is null or HttpConfiguration, try registry first (preferred approach)
            if (registry.hasProvider(scheme)) {
                return registry.getProvider(path);
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
            if (config != null) {
                throw new IllegalArgumentException("file:// scheme does not accept configuration, got: " + config.getClass().getName());
            }
            return new LocalStorageProvider();
        }

        // Unknown scheme - try registry as fallback
        if (registry.hasProvider(scheme)) {
            return registry.getProvider(path);
        }

        throw new IllegalArgumentException(
            "Unsupported storage scheme: " + scheme + ". " + "Supported schemes: s3://, s3a://, s3n://, http://, https://, file://"
        );
    }
}
