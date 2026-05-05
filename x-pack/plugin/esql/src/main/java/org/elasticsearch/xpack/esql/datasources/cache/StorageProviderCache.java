/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * LRU cache for {@link StorageProvider} instances, keyed by {@code (scheme, config)}.
 *
 * <p>When a query supplies a {@code WITH} clause (non-empty config), a cloud client
 * (S3, GCS, Azure) is constructed per {@link StorageProvider}. Without this cache,
 * consecutive calls to {@code StorageProviderRegistry.createProvider()} with the same
 * config would each create a new client instance and its associated connection pool.
 * This cache ensures that the same config reuses the same client across calls.
 *
 * <p>Entries expire after {@link #DEFAULT_TTL_MINUTES} minutes of inactivity.
 * On eviction, the removed provider is closed so that connection pools and cloud
 * resources are released.
 *
 * <p>The cache is bounded at {@link #MAX_ENTRIES} entries — large enough to serve
 * typical workloads (a handful of distinct S3 credentials) while preventing unbounded
 * growth when queries use many distinct configs.
 *
 * <p>Thread-safe: backed by {@link Cache#computeIfAbsent}.
 */
public class StorageProviderCache implements Closeable {

    private static final Logger logger = LogManager.getLogger(StorageProviderCache.class);

    /** Maximum number of distinct (scheme, config) entries retained. */
    static final int MAX_ENTRIES = 32;

    /** Default TTL in minutes for cached providers. */
    static final long DEFAULT_TTL_MINUTES = 5L;

    /**
     * Cache key that combines the URI scheme with the config map.
     * Full map equality is used — two configs are the same key only if they
     * contain identical key-value pairs.
     *
     * @param scheme normalized (lower-case) URI scheme, e.g. {@code "s3"}
     * @param config the WITH-clause config map from the query
     */
    public record CacheKey(String scheme, Map<String, Object> config) {}

    /**
     * Factory interface for creating a new {@link StorageProvider} on cache miss.
     */
    @FunctionalInterface
    public interface ProviderFactory {
        StorageProvider create() throws Exception;
    }

    private final Cache<CacheKey, StorageProvider> cache;

    public StorageProviderCache() {
        this(DEFAULT_TTL_MINUTES);
    }

    StorageProviderCache(long ttlMinutes) {
        this.cache = CacheBuilder.<CacheKey, StorageProvider>builder()
            .setMaximumWeight(MAX_ENTRIES)
            .weigher((key, value) -> 1)
            .setExpireAfterWrite(TimeValue.timeValueMinutes(ttlMinutes))
            .removalListener(notification -> {
                StorageProvider evicted = notification.getValue();
                if (evicted != null) {
                    try {
                        evicted.close();
                    } catch (IOException e) {
                        logger.warn("Failed to close evicted StorageProvider for scheme [{}]", notification.getKey().scheme(), e);
                    }
                }
            })
            .build();
    }

    /**
     * Returns a cached provider for the given key, or creates one via the factory on a cache miss.
     * The factory is invoked at most once per key under concurrent access.
     *
     * @param key     the cache key (scheme + config)
     * @param factory supplier to create the provider on a miss; may throw any exception
     * @return the cached or newly created provider
     * @throws Exception if the factory throws during a cache miss
     */
    public StorageProvider getOrCreate(CacheKey key, ProviderFactory factory) throws Exception {
        try {
            return cache.computeIfAbsent(key, k -> factory.create());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException("Failed to create StorageProvider for key " + key, cause);
        }
    }

    /** Removes all entries and closes the associated providers. */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public void close() {
        invalidateAll();
    }
}
