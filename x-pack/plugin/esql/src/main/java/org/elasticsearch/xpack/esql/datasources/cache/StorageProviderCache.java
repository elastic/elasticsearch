/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Pool of live {@link StorageProvider} clients, keyed by {@code (scheme, config)}.
 *
 * <p>When a query supplies a {@code WITH} clause (non-empty config), a cloud client
 * (S3, GCS, Azure) is constructed per {@link StorageProvider}. This pool ensures that
 * the same config reuses the same client across overlapping calls instead of building
 * a new HTTP pool each time.
 *
 * <p>Borrow protocol matches snapshot S3 {@code AmazonS3Reference} ({@code tryIncRef} /
 * {@code close() = decRef} / {@code closeInternal} at 0). Unlike snapshot S3, this pool keeps
 * an extra map-held ref so an idle client can be reused until TTL; last user close does not
 * immediately shut the SDK client.
 *
 * <p>In-use entries are pinned ({@code refCount() > 1}) so a long scan cannot observe
 * {@code Connection pool shut down}. Idle entries (map ref only) expire after
 * {@link #DEFAULT_TTL_MINUTES} minutes from the last return, swept on the next
 * {@link #getOrCreate}. Idle count is capped at {@link #MAX_ENTRIES}; leased entries
 * do not count toward the cap and are never true-closed by eviction.
 *
 * <p>Callers of {@link #getOrCreate} must {@code close()} the returned provider exactly
 * as a borrow. Empty-config default providers are not pooled here.
 */
public class StorageProviderCache implements Closeable {

    private static final Logger logger = LogManager.getLogger(StorageProviderCache.class);

    /** Maximum number of distinct idle (scheme, config) entries retained. */
    static final int MAX_ENTRIES = 32;

    /** Default idle TTL in minutes for pooled providers with no outstanding borrows. */
    static final long DEFAULT_TTL_MINUTES = 5L;

    /**
     * Cache key that combines the URI scheme with the config map.
     * Full map equality is used — two configs are the same key only if they
     * contain identical key-value pairs.
     *
     * @param scheme normalized (lower-case) URI scheme, e.g. {@code "s3"}
     * @param config the configuration map from the query
     */
    public record CacheKey(String scheme, Map<String, Object> config) {}

    /** Supplier invoked on cache miss; returns the provider plus the keys it consumed from config. */
    @FunctionalInterface
    public interface ProviderFactory {
        Configured<StorageProvider> create() throws Exception;
    }

    private final ConcurrentHashMap<CacheKey, Entry> entries = new ConcurrentHashMap<>();
    private final long idleTtlNanos;
    private final LongSupplier nanoClock;

    public StorageProviderCache() {
        this(TimeValue.timeValueMinutes(DEFAULT_TTL_MINUTES), System::nanoTime);
    }

    StorageProviderCache(TimeValue idleTtl, LongSupplier nanoClock) {
        if (idleTtl == null) {
            throw new IllegalArgumentException("idleTtl cannot be null");
        }
        if (nanoClock == null) {
            throw new IllegalArgumentException("nanoClock cannot be null");
        }
        this.idleTtlNanos = idleTtl.nanos();
        this.nanoClock = nanoClock;
    }

    /**
     * Returns a pooled lease for the given key, creating the underlying provider on a miss.
     * The factory is invoked at most once per key under concurrent access (miss path is
     * synchronized, matching {@code S3ClientsManager.ClientsHolder#client}).
     *
     * <p>The returned {@link Configured#value()} is a wrapper: {@code close()} returns the
     * lease to the pool and is idempotent. It does not shut down the SDK client.
     *
     * @param key     the cache key (scheme + config)
     * @param factory supplier to create the provider on a miss; may throw any exception
     * @return a new lease wrapping the cached provider, paired with its consumed-key set
     * @throws Exception if the factory throws during a cache miss
     */
    public Configured<StorageProvider> getOrCreate(CacheKey key, ProviderFactory factory) throws Exception {
        Map<String, Object> config = Map.copyOf(key.config());
        CacheKey stableKey = config == key.config() ? key : new CacheKey(key.scheme(), config);
        sweepExpired();
        Entry existing = entries.get(stableKey);
        if (existing != null && pin(existing)) {
            evictExcessIdle();
            return wrap(existing);
        }
        synchronized (this) {
            existing = entries.get(stableKey);
            if (existing != null && pin(existing)) {
                evictExcessIdle();
                return wrap(existing);
            }
            Configured<StorageProvider> created = factory.create();
            Entry entry = new Entry(stableKey, created, nanoClock.getAsLong());
            entry.mustIncRef();
            entries.put(stableKey, entry);
            evictExcessIdle();
            return wrap(entry);
        }
    }

    /** Removes all map slots. In-flight leases keep their clients alive until returned. */
    public void invalidateAll() {
        synchronized (this) {
            for (CacheKey key : Set.copyOf(entries.keySet())) {
                Entry entry = entries.remove(key);
                if (entry != null) {
                    entry.close();
                }
            }
        }
    }

    @Override
    public void close() {
        invalidateAll();
    }

    /** Inner provider when {@code provider} is a pool lease; otherwise {@code provider} itself. */
    static StorageProvider unwrap(StorageProvider provider) {
        return provider instanceof PooledStorageProvider pooled ? pooled.delegate : provider;
    }

    /**
     * {@code close()} on a WITH-config pool lease returns the client to the pool. No-op for
     * registry defaults and {@code null} — those must never be true-closed by query code.
     */
    public static void closeLease(StorageProvider provider) {
        if (provider instanceof PooledStorageProvider pooled) {
            pooled.close();
        }
    }

    /** {@code true} when {@code close()} is a pool return, not a client shutdown. */
    public static boolean isPooledLease(StorageProvider provider) {
        return provider instanceof PooledStorageProvider;
    }

    private Configured<StorageProvider> wrap(Entry entry) {
        return new Configured<>(new PooledStorageProvider(entry), entry.consumedKeys);
    }

    private void sweepExpired() {
        long now = nanoClock.getAsLong();
        for (Map.Entry<CacheKey, Entry> mapEntry : entries.entrySet()) {
            Entry entry = mapEntry.getValue();
            if (entry.refCount() == 1 && now - entry.lastAccessNanos >= idleTtlNanos) {
                dropMapSlotIfIdle(mapEntry.getKey(), entry, true);
            }
        }
    }

    /**
     * True-close oldest idle entries until at most {@link #MAX_ENTRIES} idle remain.
     * Leased entries ({@code refCount() > 1}) are skipped.
     */
    private void evictExcessIdle() {
        while (true) {
            CacheKey oldestKey = null;
            Entry oldest = null;
            long oldestAccess = Long.MAX_VALUE;
            int idle = 0;
            for (Map.Entry<CacheKey, Entry> mapEntry : entries.entrySet()) {
                Entry entry = mapEntry.getValue();
                if (entry.refCount() != 1) {
                    continue;
                }
                idle++;
                if (entry.lastAccessNanos <= oldestAccess) {
                    oldestAccess = entry.lastAccessNanos;
                    oldestKey = mapEntry.getKey();
                    oldest = entry;
                }
            }
            if (idle <= MAX_ENTRIES || oldest == null) {
                return;
            }
            dropMapSlotIfIdle(oldestKey, oldest, false);
        }
    }

    // Same lock as pin() and lease return: only unmap+close when still idle.
    private void dropMapSlotIfIdle(CacheKey key, Entry entry, boolean expireOnly) {
        synchronized (entry) {
            if (entry.refCount() != 1) {
                return;
            }
            if (expireOnly && nanoClock.getAsLong() - entry.lastAccessNanos < idleTtlNanos) {
                return;
            }
            if (entries.remove(key, entry) == false) {
                return;
            }
            entry.close();
        }
    }

    private static boolean pin(Entry entry) {
        synchronized (entry) {
            return entry.tryIncRef();
        }
    }

    /**
     * Map-slot refcounted provider. Ref 1 is the map; extra refs are borrows.
     * {@link #close()} drops one ref (the map's, when called from eviction / invalidate).
     */
    private static final class Entry extends AbstractRefCounted implements Releasable {
        private final CacheKey key;
        private final StorageProvider provider;
        private final Set<String> consumedKeys;
        private volatile long lastAccessNanos;

        Entry(CacheKey key, Configured<StorageProvider> created, long nowNanos) {
            this.key = key;
            this.provider = created.value();
            this.consumedKeys = created.consumedKeys();
            this.lastAccessNanos = nowNanos;
        }

        @Override
        public void close() {
            decRef();
        }

        @Override
        protected void closeInternal() {
            try {
                provider.close();
            } catch (Exception e) {
                logger.warn("Failed to close StorageProvider for scheme [{}]", key.scheme(), e);
            }
        }
    }

    /**
     * Per-borrow wrapper. {@link #close()} returns the lease; the inner client stays pooled.
     */
    private final class PooledStorageProvider implements StorageProvider {
        private final StorageProvider delegate;
        private final Releasable lease;

        PooledStorageProvider(Entry entry) {
            this.delegate = entry.provider;
            this.lease = Releasables.releaseOnce(() -> {
                synchronized (entry) {
                    entry.lastAccessNanos = nanoClock.getAsLong();
                    entry.decRef();
                }
                evictExcessIdle();
            });
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return delegate.newObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return delegate.newObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return delegate.newObject(path, length, lastModified);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
            return delegate.listObjects(prefix, recursive);
        }

        @Override
        public boolean exists(StoragePath path) throws IOException {
            return delegate.exists(path);
        }

        @Override
        public List<String> supportedSchemes() {
            return delegate.supportedSchemes();
        }

        @Override
        public boolean supportsStableMetadata() {
            return delegate.supportsStableMetadata();
        }

        @Override
        public void close() {
            lease.close();
        }
    }
}
