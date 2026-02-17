/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

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
 * <p>Registration methods ({@link #registerWithProvider}, {@link #setSpiFactories})
 * are intended for single-threaded initialization only (called from the
 * {@link DataSourceModule} constructor). No concurrent writes occur, so no
 * synchronization is needed around the provider list.
 *
 * <p>This registry implements Closeable to properly close all registered providers
 * when the registry is no longer needed.
 */
public class StorageProviderRegistry implements Closeable {
    private final Map<String, StorageProvider> providers = new ConcurrentHashMap<>();
    private final List<StorageProvider> createdProviders = new ArrayList<>();
    private Map<String, StorageProviderFactory> spiFactories = Map.of();

    public void registerWithProvider(String scheme, StorageProvider provider) {
        if (scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        if (provider == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }
        providers.put(scheme.toLowerCase(Locale.ROOT), provider);
        createdProviders.add(provider);
    }

    public StorageProvider provider(StoragePath path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        String scheme = path.scheme();
        StorageProvider provider = providers.get(scheme.toLowerCase(Locale.ROOT));
        if (provider == null) {
            throw new IllegalArgumentException("No storage provider registered for scheme: " + scheme);
        }
        return provider;
    }

    public boolean hasProvider(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return false;
        }
        return providers.containsKey(scheme.toLowerCase(Locale.ROOT));
    }

    public void setSpiFactories(Map<String, StorageProviderFactory> spiFactories) {
        this.spiFactories = spiFactories;
    }

    public StorageProvider createProvider(String scheme, Settings settings, Map<String, Object> config) {
        String normalizedScheme = scheme.toLowerCase(Locale.ROOT);

        // When config is null/empty, fall back to the default registered provider
        if (config == null || config.isEmpty()) {
            StorageProvider provider = providers.get(normalizedScheme);
            if (provider == null) {
                throw new IllegalArgumentException("No storage provider registered for scheme: " + scheme);
            }
            return provider;
        }

        // Create a fresh provider with the per-query config
        StorageProviderFactory spiFactory = spiFactories.get(normalizedScheme);
        if (spiFactory == null) {
            throw new IllegalArgumentException("No SPI storage factory registered for scheme: " + scheme);
        }
        return spiFactory.create(settings, config);
    }

    @Override
    public void close() throws IOException {
        List<StorageProvider> toClose = new ArrayList<>(createdProviders);
        createdProviders.clear();
        IOUtils.close(toClose);
    }
}
