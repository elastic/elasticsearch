/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache.MAX_ENTRIES;
import static org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache.unwrap;

/**
 * Tests for {@link StorageProviderCache}: hit/miss behaviour, pin-while-in-use,
 * idle TTL sweep, idle cap, and invalidate/close.
 */
public class StorageProviderCacheTests extends ESTestCase {

    /**
     * Minimal StorageProvider that tracks whether it has been closed.
     * Used to verify that true eviction / invalidate triggers close().
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
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
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
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("key", "value"));

        AtomicInteger supplierCalls = new AtomicInteger();
        Configured<StorageProvider> result1 = cache.getOrCreate(key, () -> {
            supplierCalls.incrementAndGet();
            return Configured.empty(new TrackingProvider());
        });
        Configured<StorageProvider> result2 = cache.getOrCreate(key, () -> {
            supplierCalls.incrementAndGet();
            return Configured.empty(new TrackingProvider());
        });
        try {
            assertSame(unwrap(result1.value()), unwrap(result2.value()));
            assertEquals("supplier should only be called once for the same key", 1, supplierCalls.get());
        } finally {
            result1.value().close();
            result2.value().close();
        }
    }

    public void testDifferentConfigReturnsDifferentProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey keyA = new StorageProviderCache.CacheKey("s3", Map.of("key", "value-a"));
        StorageProviderCache.CacheKey keyB = new StorageProviderCache.CacheKey("s3", Map.of("key", "value-b"));

        Configured<StorageProvider> providerA = cache.getOrCreate(keyA, () -> Configured.empty(new TrackingProvider()));
        Configured<StorageProvider> providerB = cache.getOrCreate(keyB, () -> Configured.empty(new TrackingProvider()));
        try {
            assertNotSame("different configs should yield different providers", unwrap(providerA.value()), unwrap(providerB.value()));
        } finally {
            providerA.value().close();
            providerB.value().close();
        }
    }

    public void testDifferentSchemeReturnsDifferentProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        Map<String, Object> config = Map.of("key", "value");
        StorageProviderCache.CacheKey keyS3 = new StorageProviderCache.CacheKey("s3", config);
        StorageProviderCache.CacheKey keyGcs = new StorageProviderCache.CacheKey("gs", config);

        Configured<StorageProvider> s3Provider = cache.getOrCreate(keyS3, () -> Configured.empty(new TrackingProvider()));
        Configured<StorageProvider> gcsProvider = cache.getOrCreate(keyGcs, () -> Configured.empty(new TrackingProvider()));
        try {
            assertNotSame("different schemes should yield different providers", unwrap(s3Provider.value()), unwrap(gcsProvider.value()));
        } finally {
            s3Provider.value().close();
            gcsProvider.value().close();
        }
    }

    public void testInvalidateAllClosesIdleProviders() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("a", "b"));

        TrackingProvider provider = new TrackingProvider();
        Configured<StorageProvider> lease = cache.getOrCreate(key, () -> Configured.empty(provider));
        lease.value().close();

        assertEquals("provider should not be closed before invalidation", 0, provider.closeCalls.get());
        cache.invalidateAll();
        assertEquals("provider should be closed after invalidateAll", 1, provider.closeCalls.get());
    }

    public void testCloseInvalidatesAndClosesIdleProviders() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("region", "us-east-1"));

        TrackingProvider provider = new TrackingProvider();
        Configured<StorageProvider> lease = cache.getOrCreate(key, () -> Configured.empty(provider));
        lease.value().close();

        cache.close();
        assertEquals("provider should be closed when cache is closed", 1, provider.closeCalls.get());
    }

    public void testInvalidateAllWithOutstandingBorrowDefersTrueClose() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("k", "v"));

        TrackingProvider provider = new TrackingProvider();
        Configured<StorageProvider> lease = cache.getOrCreate(key, () -> Configured.empty(provider));
        cache.invalidateAll();
        assertEquals("in-use provider must survive invalidateAll", 0, provider.closeCalls.get());
        lease.value().close();
        assertEquals("last lease return after invalidateAll true-closes", 1, provider.closeCalls.get());
    }

    public void testCloseLeaseIsNoOpForNonPooledProvider() {
        TrackingProvider provider = new TrackingProvider();
        StorageProviderCache.closeLease(provider);
        StorageProviderCache.closeLease(null);
        assertEquals(0, provider.closeCalls.get());
    }

    public void testSupplierExceptionPropagates() {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("bad", "config"));

        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> cache.getOrCreate(key, () -> {
            throw new IllegalArgumentException("bad credentials");
        }));
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
        try {
            assertSame(unwrap(first.value()), unwrap(second.value()));
            assertEquals(Set.of("access_key"), second.consumedKeys());
        } finally {
            first.value().close();
            second.value().close();
        }
    }

    public void testReturningOneLeaseKeepsSharedInstance() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("k", "v"));
        TrackingProvider provider = new TrackingProvider();

        Configured<StorageProvider> first = cache.getOrCreate(key, () -> Configured.empty(provider));
        Configured<StorageProvider> second = cache.getOrCreate(key, () -> { throw new AssertionError("must hit"); });
        first.value().close();
        Configured<StorageProvider> third = cache.getOrCreate(key, () -> { throw new AssertionError("still pooled after one return"); });
        try {
            assertSame(provider, unwrap(second.value()));
            assertSame(provider, unwrap(third.value()));
            assertEquals(0, provider.closeCalls.get());
        } finally {
            second.value().close();
            third.value().close();
        }
    }

    public void testIdempotentLeaseClose() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", Map.of("k", "v"));
        TrackingProvider provider = new TrackingProvider();

        Configured<StorageProvider> lease = cache.getOrCreate(key, () -> Configured.empty(provider));
        lease.value().close();
        lease.value().close();
        assertEquals(0, provider.closeCalls.get());

        Configured<StorageProvider> again = cache.getOrCreate(key, () -> { throw new AssertionError("still pooled"); });
        try {
            assertSame(provider, unwrap(again.value()));
        } finally {
            again.value().close();
        }
    }

    public void testIdleCapDoesNotEvictHeldProvider() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        TrackingProvider held = new TrackingProvider();
        Configured<StorageProvider> heldLease = cache.getOrCreate(
            new StorageProviderCache.CacheKey("s3", Map.of("held", "true")),
            () -> Configured.empty(held)
        );

        List<TrackingProvider> idle = new ArrayList<>(MAX_ENTRIES);
        for (int i = 0; i < MAX_ENTRIES; i++) {
            TrackingProvider tracking = new TrackingProvider();
            idle.add(tracking);
            cache.getOrCreate(idleKey(i), () -> Configured.empty(tracking)).value().close();
        }
        assertEquals(0, held.closeCalls.get());
        for (TrackingProvider tracking : idle) {
            assertEquals(0, tracking.closeCalls.get());
        }

        TrackingProvider extra = new TrackingProvider();
        cache.getOrCreate(idleKey(MAX_ENTRIES), () -> Configured.empty(extra)).value().close();

        assertEquals("held provider must stay pinned", 0, held.closeCalls.get());
        assertEquals("newest idle should not be the eviction victim", 0, extra.closeCalls.get());
        int closedIdle = 0;
        for (TrackingProvider tracking : idle) {
            closedIdle += tracking.closeCalls.get();
        }
        assertEquals("exactly one idle entry should be evicted", 1, closedIdle);
        heldLease.value().close();
    }

    public void testIdleTtlSweepOnDifferentKeyTrueCloses() throws Exception {
        AtomicLong now = new AtomicLong();
        StorageProviderCache cache = new StorageProviderCache(TimeValue.timeValueNanos(10), now::get);
        StorageProviderCache.CacheKey keyC = new StorageProviderCache.CacheKey("s3", Map.of("c", "1"));
        StorageProviderCache.CacheKey keyOther = new StorageProviderCache.CacheKey("s3", Map.of("other", "1"));

        TrackingProvider c = new TrackingProvider();
        cache.getOrCreate(keyC, () -> Configured.empty(c)).value().close();
        now.set(10);

        TrackingProvider other = new TrackingProvider();
        Configured<StorageProvider> otherLease = cache.getOrCreate(keyOther, () -> Configured.empty(other));
        try {
            assertEquals("sweep of a different key must true-close idle C", 1, c.closeCalls.get());
        } finally {
            otherLease.value().close();
        }

        TrackingProvider c2 = new TrackingProvider();
        Configured<StorageProvider> cLease = cache.getOrCreate(keyC, () -> Configured.empty(c2));
        try {
            assertSame(c2, unwrap(cLease.value()));
            assertNotSame(c, unwrap(cLease.value()));
        } finally {
            cLease.value().close();
        }
    }

    public void testHeldProviderSurvivesTtlUntilReturnedThenSwept() throws Exception {
        AtomicLong now = new AtomicLong();
        StorageProviderCache cache = new StorageProviderCache(TimeValue.timeValueNanos(10), now::get);
        StorageProviderCache.CacheKey keyC = new StorageProviderCache.CacheKey("s3", Map.of("c", "1"));

        TrackingProvider c = new TrackingProvider();
        Configured<StorageProvider> held = cache.getOrCreate(keyC, () -> Configured.empty(c));
        now.set(100);
        Configured<StorageProvider> other = cache.getOrCreate(
            new StorageProviderCache.CacheKey("s3", Map.of("other", "1")),
            () -> Configured.empty(new TrackingProvider())
        );
        other.value().close();
        assertEquals("leased provider must not expire", 0, c.closeCalls.get());

        held.value().close();
        Configured<StorageProvider> other2 = cache.getOrCreate(
            new StorageProviderCache.CacheKey("s3", Map.of("other", "2")),
            () -> Configured.empty(new TrackingProvider())
        );
        other2.value().close();
        assertEquals("just-returned provider is not yet idle-expired", 0, c.closeCalls.get());

        now.set(110);
        Configured<StorageProvider> other3 = cache.getOrCreate(
            new StorageProviderCache.CacheKey("s3", Map.of("other", "3")),
            () -> Configured.empty(new TrackingProvider())
        );
        other3.value().close();
        assertEquals(1, c.closeCalls.get());
    }

    public void testMutableConfigMapIsSnapshotted() throws Exception {
        StorageProviderCache cache = new StorageProviderCache();
        HashMap<String, Object> config = new HashMap<>();
        config.put("k", "v");
        StorageProviderCache.CacheKey key = new StorageProviderCache.CacheKey("s3", config);
        TrackingProvider provider = new TrackingProvider();
        Configured<StorageProvider> first = cache.getOrCreate(key, () -> Configured.empty(provider));
        config.put("k", "mutated");
        TrackingProvider secondProvider = new TrackingProvider();
        Configured<StorageProvider> second = cache.getOrCreate(key, () -> Configured.empty(secondProvider));
        try {
            assertNotSame("mutating the caller's map must not alias the pooled key", unwrap(first.value()), unwrap(second.value()));
            assertEquals(0, provider.closeCalls.get());
        } finally {
            first.value().close();
            second.value().close();
        }
    }

    private static StorageProviderCache.CacheKey idleKey(int i) {
        return new StorageProviderCache.CacheKey("s3", Map.of("i", Integer.toString(i)));
    }
}
