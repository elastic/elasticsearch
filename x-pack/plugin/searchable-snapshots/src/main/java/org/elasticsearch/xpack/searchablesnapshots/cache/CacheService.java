/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * {@link CacheService} maintains a cache entry for all files read from cached searchable snapshot directories (see {@link CacheDirectory})
 */
public class CacheService extends AbstractLifecycleComponent {

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = Setting.byteSizeSetting("xpack.searchable.snapshot.cache.size",
        new ByteSizeValue(1, ByteSizeUnit.GB),                  // TODO: size the default value according to disk space
        new ByteSizeValue(0, ByteSizeUnit.BYTES),               // min // NORELEASE
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),  // max
        Setting.Property.NodeScope);

    private final ReleasableLock cacheInvalidationLock;
    private final ReleasableLock cacheAccessLock;
    private final Cache<String, CacheFile> cache;

    public CacheService(final Settings settings) {
        this.cache = CacheBuilder.<String, CacheFile>builder()
            .setMaximumWeight(SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes())
            .weigher((key, entry) -> entry.getLength())
            // NORELEASE This does not immediately free space on disk, as cache file are only deleted when all index inputs
            // are done with reading/writing the cache file
            .removalListener(notification -> IOUtils.closeWhileHandlingException(() -> notification.getValue().startEviction()))
            .build();

        // Prevent new CacheFile objects to be added to the cache while the cache is being fully or partially invalidated
        // This can happen because CacheFile objects might execute listeners at eviction time, potentially forcing more
        // objects (like CacheDirectory's index inputs) to get new CacheFile.
        final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
        this.cacheInvalidationLock = new ReleasableLock(cacheLock.writeLock());
        this.cacheAccessLock = new ReleasableLock(cacheLock.readLock());
    }

    @Override
    protected void doStart() {
        // NORELEASE TODO clean up (or rebuild) cache from disk as a node crash may leave cached files
    }

    @Override
    protected void doStop() {
        try (ReleasableLock ignored = cacheInvalidationLock.acquire()) {
            cache.invalidateAll();
        }
    }

    @Override
    protected void doClose() {
    }

    private void ensureLifecycleStarted() {
        final Lifecycle.State state = lifecycleState();
        if (state != Lifecycle.State.STARTED) {
            throw new IllegalStateException("Failed to read data from cache: cache service is not started [" + state + "]");
        }
    }

    public CacheFile get(final String name, final long length, final Path file) throws Exception {
        ensureLifecycleStarted();
        try (ReleasableLock ignored = cacheAccessLock.acquire()) {
            ensureLifecycleStarted();
            return cache.computeIfAbsent(toCacheKey(file), key -> {
                // generate a random UUID for the name of the cache file on disk
                final String uuid = UUIDs.randomBase64UUID();
                // resolve the cache file on disk w/ the expected cached file
                final Path path = file.getParent().resolve(uuid);
                assert Files.notExists(path) : "cache file already exists " + path;

                return new CacheFile(name, length, path);
            });
        }
    }

    /**
     * Remove from cache all entries that match the given predicate.
     *
     * @param predicate the predicate to evaluate
     */
    public void removeFromCache(final Predicate<String> predicate) {
        try (ReleasableLock ignored = cacheInvalidationLock.acquire()) {
            for (String cacheKey : cache.keys()) {
                if (predicate.test(cacheKey)) {
                    cache.invalidate(cacheKey);
                }
            }
            cache.refresh();
        }
    }

    /**
     * Computes the cache key associated to the given Lucene cached file
     *
     * @param cacheFile the cached file
     * @return the cache key
     */
    private static String toCacheKey(final Path cacheFile) { // TODO Fix this. Cache Key should be computed from snapshot id/index id/shard
        return cacheFile.toAbsolutePath().toString();
    }
}
