/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for StorageProvider implementations, keyed by URI scheme.
 * Allows pluggable discovery of storage providers based on the scheme
 * portion of a StoragePath (e.g., "http", "https", "s3", "file").
 *
 * <p>Providers are created lazily on first access rather than eagerly at startup,
 * so heavy dependencies (S3 client, HTTP client, etc.) are only loaded when
 * an EXTERNAL query actually targets that backend.
 *
 * <p>Registration methods are intended for single-threaded initialization only
 * (called from the {@link DataSourceModule} constructor).
 *
 * <p>This registry implements Closeable to properly close all created providers
 * when the registry is no longer needed.
 */
public class StorageProviderRegistry implements Closeable {
    private final Map<String, StorageProviderFactory> factories = new ConcurrentHashMap<>();
    private final Map<String, StorageProvider> providers = new ConcurrentHashMap<>();
    private final List<StorageProvider> createdProviders = new ArrayList<>();
    private final Settings settings;

    public StorageProviderRegistry(Settings settings) {
        this.settings = settings != null ? settings : Settings.EMPTY;
    }

    public void registerFactory(String scheme, StorageProviderFactory factory) {
        if (Strings.isNullOrEmpty(scheme)) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        if (factory == null) {
            throw new IllegalArgumentException("Factory cannot be null");
        }
        factories.put(scheme.toLowerCase(Locale.ROOT), factory);
    }

    public StorageProvider provider(StoragePath path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        StorageProvider provider = providers.get(scheme);
        if (provider == null) {
            provider = createDefaultProvider(scheme);
        }
        return provider;
    }

    public boolean hasProvider(String scheme) {
        if (Strings.isNullOrEmpty(scheme)) {
            return false;
        }
        String normalized = scheme.toLowerCase(Locale.ROOT);
        return factories.containsKey(normalized) || providers.containsKey(normalized);
    }

    public StorageProvider createProvider(String scheme, Settings settings, Map<String, Object> config) {
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);

        // When config is null/empty, fall back to the default registered provider
        if (config == null || config.isEmpty()) {
            StorageProvider provider = providers.get(normalizedScheme);
            if (provider == null) {
                provider = createDefaultProvider(normalizedScheme);
            }
            return provider;
        }

        // Create a fresh provider with the per-query config
        StorageProviderFactory factory = factories.get(normalizedScheme);
        if (factory == null) {
            throw new IllegalArgumentException("No SPI storage factory registered for scheme: " + scheme);
        }
        return factory.create(settings, config);
    }

    private synchronized StorageProvider createDefaultProvider(String normalizedScheme) {
        // Double-check after acquiring lock
        StorageProvider provider = providers.get(normalizedScheme);
        if (provider != null) {
            return provider;
        }
        StorageProviderFactory factory = factories.get(normalizedScheme);
        if (factory == null) {
            throw new IllegalArgumentException("No storage provider registered for scheme: " + normalizedScheme);
        }
        provider = factory.create(settings);
        providers.put(normalizedScheme, provider);
        createdProviders.add(provider);
        return provider;
    }

    @Override
    public void close() throws IOException {
        List<StorageProvider> toClose = new ArrayList<>(createdProviders);
        createdProviders.clear();
        providers.clear();
        IOUtils.close(toClose);
    }
}
