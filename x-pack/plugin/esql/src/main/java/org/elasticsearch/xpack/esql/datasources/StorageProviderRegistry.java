/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

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
 * This registry implements Closeable to properly close all registered providers
 * when the registry is no longer needed.
 */
public class StorageProviderRegistry implements Closeable {
    private final Map<String, StorageProviderFactory> factories = new ConcurrentHashMap<>();
    private final List<StorageProvider> createdProviders = new ArrayList<>();

    public void register(String scheme, StorageProviderFactory factory) {
        if (scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        if (factory == null) {
            throw new IllegalArgumentException("Factory cannot be null");
        }
        factories.put(scheme.toLowerCase(Locale.ROOT), factory);
    }

    public void registerWithProvider(String scheme, StorageProvider provider) {
        if (scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        if (provider == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }
        factories.put(scheme.toLowerCase(Locale.ROOT), path -> provider);
        synchronized (createdProviders) {
            createdProviders.add(provider);
        }
    }

    public StorageProviderFactory unregister(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return null;
        }
        return factories.remove(scheme.toLowerCase(Locale.ROOT));
    }

    public StorageProvider provider(StoragePath path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        String scheme = path.scheme();
        StorageProviderFactory factory = factories.get(scheme.toLowerCase(Locale.ROOT));
        if (factory == null) {
            throw new IllegalArgumentException("No storage provider registered for scheme: " + scheme);
        }
        return factory.create(path);
    }

    public boolean hasProvider(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return false;
        }
        return factories.containsKey(scheme.toLowerCase(Locale.ROOT));
    }

    @Override
    public void close() throws IOException {
        List<StorageProvider> toClose;
        synchronized (createdProviders) {
            toClose = new ArrayList<>(createdProviders);
            createdProviders.clear();
        }
        IOUtils.close(toClose);
    }
}
