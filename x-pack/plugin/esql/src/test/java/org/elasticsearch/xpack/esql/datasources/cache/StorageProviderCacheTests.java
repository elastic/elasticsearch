/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests for {@link StorageProviderCache}: hit/miss behaviour, provider reuse,
 * close-on-eviction, and registry-close behaviour.
 */
public class StorageProviderCacheTests extends ESTestCase {

    /**
     * Minimal StorageProvider that tracks whether it has been closed.
     * Used to verify that cache eviction triggers close().
     */
    static class TrackingProvider implements StorageProvider {
        final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public StorageObject newObject(StoragePath path) {
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return null;
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.StorageIterator listObjects(StoragePath prefix, boolean recursive)
            throws IOException {
            return null;
        }

        @Override
        public boolean exists(StoragePath path) throws IOException {
            return false;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of();
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }

    public void testSameConfigReturnsSameProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        Map<String, Object> config = Map.of("key", "value");
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", config);

        AtomicInteger supplierCalls = new AtomicInteger();
        Configured<StorageProvider> result1 = cache.getOrCreate(key, () -> {
            supplierCalls.incrementAndGet();
            return Configured.empty(new TrackingProvider());
        });
        Configured<StorageProvider> result2 = cache.getOrCreate(key, () -> {
            supplierCalls.incrementAndGet();
            return Configured.empty(new TrackingProvider());
        });

        assertThat(result1, sameInstance(result2));
        assertThat(result1.value(), sameInstance(result2.value()));
        assertEquals("supplier should only be called once for the same key", 1, supplierCalls.get());
    }

    public void testDifferentConfigReturnsDifferentProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        Map<String, Object> configA = Map.of("key", "value-a");
        Map<String, Object> configB = Map.of("key", "value-b");
        StorageProviderCache.CacheKey keyA = new StorageProviderCache.CacheKey("s3", configA);
        StorageProviderCache.CacheKey keyB = new StorageProviderCache.CacheKey("s3", configB);

        Configured<StorageProvider> providerA = cache.getOrCreate(keyA, () -> Configured.empty(new TrackingProvider()));
        Configured<StorageProvider> providerB = cache.getOrCreate(keyB, () -> Configured.empty(new TrackingProvider()));

        assertNotSame("different configs should yield different providers", providerA.value(), providerB.value());
    }

    public void testDifferentSchemeReturnsDifferentProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        Map<String, Object> config = Map.of("key", "value");
        StorageProviderCache.CacheKey keyS3 = new StorageProviderCache.CacheKey("s3", config);
        StorageProviderCache.CacheKey keyGcs = new StorageProviderCache.CacheKey("gs", config);

        Configured<StorageProvider> s3Provider = cache.getOrCreate(keyS3, () -> Configured.empty(new TrackingProvider()));
        Configured<StorageProvider> gcsProvider = cache.getOrCreate(keyGcs, () -> Configured.empty(new TrackingProvider()));

        assertNotSame("different schemes should yield different providers", s3Provider.value(), gcsProvider.value());
    }

    public void testInvalidateAllClosesProviders() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("a", "b"));

        TrackingProvider provider = new TrackingProvider();
        cache.getOrCreate(key, () -> Configured.empty(provider));

        assertEquals("provider should not be closed before invalidation", 0, provider.closeCalls.get());
        cache.invalidateAll();
        assertEquals("provider should be closed after invalidateAll", 1, provider.closeCalls.get());
    }

    public void testCloseInvalidatesAndClosesProviders() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("region", "us-east-1"));

        TrackingProvider provider = new TrackingProvider();
        cache.getOrCreate(key, () -> Configured.empty(provider));

        cache.close();
        assertEquals("provider should be closed when cache is closed", 1, provider.closeCalls.get());
    }

    public void testSupplierExceptionPropagates() {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("bad", "config"));

        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> {
            cache.getOrCreate(key, () -> { throw new IllegalArgumentException("bad credentials"); });
        });
        assertEquals("bad credentials", thrown.getMessage());
    }

    public void testConsumedKeysCachedAlongsideProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("access_key", "ak"));

        Configured<StorageProvider> first = cache.getOrCreate(key, () -> new Configured<>(new TrackingProvider(), Set.of("access_key")));
        Configured<StorageProvider> second = cache.getOrCreate(
            key,
            () -> { throw new AssertionError("supplier must not be re-invoked on hit"); }
        );

        assertThat(second, sameInstance(first));
        assertEquals(Set.of("access_key"), second.consumedKeys());
    }
}
