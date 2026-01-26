/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for StorageProvider implementations, keyed by URI scheme.
 * Allows pluggable discovery of storage providers based on the scheme
 * portion of a StoragePath (e.g., "http", "https", "s3", "file").
 */
public class StorageProviderRegistry {
    private final Map<String, StorageProviderFactory> factories = new ConcurrentHashMap<>();
    
    /**
     * Registers a StorageProviderFactory for a specific URI scheme.
     * 
     * @param scheme the URI scheme (e.g., "http", "s3")
     * @param factory the factory to create StorageProvider instances
     * @throws IllegalArgumentException if scheme is null or empty
     */
    public void register(String scheme, StorageProviderFactory factory) {
        if (scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        if (factory == null) {
            throw new IllegalArgumentException("Factory cannot be null");
        }
        factories.put(scheme.toLowerCase(Locale.ROOT), factory);
    }
    
    /**
     * Unregisters a StorageProviderFactory for a specific URI scheme.
     * 
     * @param scheme the URI scheme to unregister
     * @return the previously registered factory, or null if none was registered
     */
    public StorageProviderFactory unregister(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return null;
        }
        return factories.remove(scheme.toLowerCase(Locale.ROOT));
    }
    
    /**
     * Gets a StorageProvider for the given path.
     * 
     * @param path the storage path
     * @return a StorageProvider instance
     * @throws IllegalArgumentException if no provider is registered for the scheme
     */
    public StorageProvider getProvider(StoragePath path) {
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
    
    /**
     * Checks if a provider is registered for the given scheme.
     * 
     * @param scheme the URI scheme to check
     * @return true if a provider is registered, false otherwise
     */
    public boolean hasProvider(String scheme) {
        if (scheme == null || scheme.isEmpty()) {
            return false;
        }
        return factories.containsKey(scheme.toLowerCase(Locale.ROOT));
    }
}
