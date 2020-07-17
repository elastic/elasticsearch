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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * {@link CacheService} maintains a cache entry for all files read from searchable snapshot directories (
 * see {@link org.elasticsearch.index.store.SearchableSnapshotDirectory})
 */
public class CacheService extends AbstractLifecycleComponent {

    private static final String SETTINGS_PREFIX = "xpack.searchable.snapshot.cache.";

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "size",
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),  // TODO: size the default value according to disk space
        new ByteSizeValue(0, ByteSizeUnit.BYTES),               // min
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),  // max
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "range_size",
        new ByteSizeValue(32, ByteSizeUnit.MB),                 // default
        new ByteSizeValue(4, ByteSizeUnit.KB),                  // min
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),  // max
        Setting.Property.NodeScope
    );

    private final Cache<CacheKey, CacheFile> cache;
    private final ByteSizeValue cacheSize;
    private final Runnable cacheCleaner;
    private final ByteSizeValue rangeSize;

    public CacheService(final Runnable cacheCleaner, final Settings settings) {
        this(cacheCleaner, SNAPSHOT_CACHE_SIZE_SETTING.get(settings), SNAPSHOT_CACHE_RANGE_SIZE_SETTING.get(settings));
    }

    // exposed for tests
    public CacheService(final Runnable cacheCleaner, final ByteSizeValue cacheSize, final ByteSizeValue rangeSize) {
        this.cacheSize = Objects.requireNonNull(cacheSize);
        this.cacheCleaner = Objects.requireNonNull(cacheCleaner);
        this.rangeSize = Objects.requireNonNull(rangeSize);
        this.cache = CacheBuilder.<CacheKey, CacheFile>builder()
            .setMaximumWeight(cacheSize.getBytes())
            .weigher((key, entry) -> entry.getLength())
            // NORELEASE This does not immediately free space on disk, as cache file are only deleted when all index inputs
            // are done with reading/writing the cache file
            .removalListener(notification -> IOUtils.closeWhileHandlingException(() -> notification.getValue().startEviction()))
            .build();
    }

    public static Path getShardCachePath(ShardPath shardPath) {
        return shardPath.getDataPath().resolve("snapshot_cache");
    }

    @Override
    protected void doStart() {
        cacheCleaner.run();
    }

    @Override
    protected void doStop() {
        cache.invalidateAll();
    }

    @Override
    protected void doClose() {}

    private void ensureLifecycleStarted() {
        final Lifecycle.State state = lifecycleState();
        assert state != Lifecycle.State.INITIALIZED : state;
        if (state != Lifecycle.State.STARTED) {
            throw new IllegalStateException("Failed to read data from cache: cache service is not started [" + state + "]");
        }
    }

    /**
     * @return the cache size (in bytes)
     */
    public long getCacheSize() {
        return cacheSize.getBytes();
    }

    /**
     * @return the cache range size (in bytes)
     */
    public int getRangeSize() {
        return Math.toIntExact(rangeSize.getBytes());
    }

    public CacheFile get(final CacheKey cacheKey, final long fileLength, final Path cacheDir) throws Exception {
        ensureLifecycleStarted();
        return cache.computeIfAbsent(cacheKey, key -> {
            ensureLifecycleStarted();
            // generate a random UUID for the name of the cache file on disk
            final String uuid = UUIDs.randomBase64UUID();
            // resolve the cache file on disk w/ the expected cached file
            final Path path = cacheDir.resolve(uuid);
            assert Files.notExists(path) : "cache file already exists " + path;

            return new CacheFile(key.toString(), fileLength, path);
        });
    }

    /**
     * Invalidate cache entries with keys matching the given predicate
     *
     * @param predicate the predicate to evaluate
     */
    public void removeFromCache(final Predicate<CacheKey> predicate) {
        for (CacheKey cacheKey : cache.keys()) {
            if (predicate.test(cacheKey)) {
                cache.invalidate(cacheKey);
            }
        }
        cache.refresh();
    }
}
