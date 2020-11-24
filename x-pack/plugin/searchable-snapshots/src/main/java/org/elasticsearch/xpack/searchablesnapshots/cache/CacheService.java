/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.toIntBytes;

/**
 * {@link CacheService} maintains a cache entry for all files read from searchable snapshot directories (see
 * {@link org.elasticsearch.index.store.SearchableSnapshotDirectory}).
 *
 * Cache files created by this service are periodically synchronized on disk in order to make the cached data durable
 * (see {@link #synchronizeCache()} for more information).
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

    public static final ByteSizeValue MIN_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(4, ByteSizeUnit.KB);
    public static final ByteSizeValue MAX_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES);
    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "range_size",
        new ByteSizeValue(32, ByteSizeUnit.MB),                 // default
        MIN_SNAPSHOT_CACHE_RANGE_SIZE,                          // min
        MAX_SNAPSHOT_CACHE_RANGE_SIZE,                          // max
        Setting.Property.NodeScope
    );

    public static final TimeValue MIN_SNAPSHOT_CACHE_SYNC_INTERVAL = TimeValue.timeValueSeconds(1L);
    public static final Setting<TimeValue> SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + "sync.interval",
        TimeValue.timeValueSeconds(60L),                        // default
        MIN_SNAPSHOT_CACHE_SYNC_INTERVAL,                       // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING = Setting.intSetting(
        SETTINGS_PREFIX + "sync.max_files",
        10_000,                                                 // default
        0,                                                      // min
        Integer.MAX_VALUE,                                      // max
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> SNAPSHOT_CACHE_SYNC_SHUTDOWN_TIMEOUT = Setting.timeSetting(
        SETTINGS_PREFIX + "sync.shutdown_timeout",
        TimeValue.timeValueSeconds(10L),                        // default
        TimeValue.ZERO,                                         // min
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(CacheService.class);

    private final ThreadPool threadPool;
    private final ConcurrentLinkedQueue<CacheFile> cacheFilesToSync;
    private final AtomicLong numberOfCacheFilesToSync;
    private final CacheSynchronizationTask cacheSyncTask;
    private final TimeValue cacheSyncStopTimeout;
    private final ReentrantLock cacheSyncLock;
    private final Cache<CacheKey, CacheFile> cache;
    private final ByteSizeValue cacheSize;
    private final Runnable cacheCleaner;
    private final ByteSizeValue rangeSize;

    private volatile int maxCacheFilesToSyncAtOnce;

    public CacheService(
        final Settings settings,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final Runnable cacheCleaner
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        this.cacheCleaner = Objects.requireNonNull(cacheCleaner);
        this.rangeSize = SNAPSHOT_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.cache = CacheBuilder.<CacheKey, CacheFile>builder()
            .setMaximumWeight(cacheSize.getBytes())
            .weigher((key, entry) -> entry.getLength())
            // NORELEASE This does not immediately free space on disk, as cache file are only deleted when all index inputs
            // are done with reading/writing the cache file
            .removalListener(notification -> onCacheFileRemoval(notification.getValue()))
            .build();
        this.numberOfCacheFilesToSync = new AtomicLong();
        this.cacheSyncLock = new ReentrantLock();
        this.cacheFilesToSync = new ConcurrentLinkedQueue<>();
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.maxCacheFilesToSyncAtOnce = SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING, this::setMaxCacheFilesToSyncAtOnce);
        this.cacheSyncTask = new CacheSynchronizationTask(threadPool, SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING, this::setCacheSyncInterval);
        this.cacheSyncStopTimeout = SNAPSHOT_CACHE_SYNC_SHUTDOWN_TIMEOUT.get(settings);
    }

    public static Path getShardCachePath(ShardPath shardPath) {
        return resolveSnapshotCache(shardPath.getDataPath());
    }

    static Path resolveSnapshotCache(Path path) {
        return path.resolve("snapshot_cache");
    }

    @Override
    protected void doStart() {
        cacheSyncTask.rescheduleIfNecessary();
        cacheCleaner.run();
    }

    @Override
    protected void doStop() {
        boolean acquired = false;
        try {
            try {
                acquired = cacheSyncLock.tryLock(cacheSyncStopTimeout.duration(), cacheSyncStopTimeout.timeUnit());
                if (acquired == false) {
                    logger.warn("failed to acquire cache sync lock in [{}], cache might be partially persisted", cacheSyncStopTimeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("interrupted while waiting for cache sync lock", e);
            }
            cacheSyncTask.close();
            cache.invalidateAll();
        } finally {
            if (acquired) {
                cacheSyncLock.unlock();
            }
        }
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
        return toIntBytes(rangeSize.getBytes());
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

            final SetOnce<CacheFile> cacheFile = new SetOnce<>();
            cacheFile.set(new CacheFile(key.toString(), fileLength, path, () -> onCacheFileUpdate(cacheFile.get())));
            return cacheFile.get();
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

    void setCacheSyncInterval(TimeValue interval) {
        cacheSyncTask.setInterval(interval);
    }

    private void setMaxCacheFilesToSyncAtOnce(int maxCacheFilesToSyncAtOnce) {
        this.maxCacheFilesToSyncAtOnce = maxCacheFilesToSyncAtOnce;
    }

    /**
     * This method is invoked when a {@link CacheFile} notifies the current {@link CacheService} that it needs to be fsync on disk.
     * <p>
     * It adds the {@link CacheFile} instance to current set of cache files to synchronize.
     *
     * @param cacheFile the instance that needs to be fsync
     */
    private void onCacheFileUpdate(CacheFile cacheFile) {
        assert cacheFile != null;
        cacheFilesToSync.offer(cacheFile);
        numberOfCacheFilesToSync.incrementAndGet();
    }

    /**
     * This method is invoked after a {@link CacheFile} is evicted from the cache.
     * <p>
     * It notifies the {@link CacheFile}'s eviction listeners that the instance is evicted.
     *
     * @param cacheFile the evicted instance
     */
    private void onCacheFileRemoval(CacheFile cacheFile) {
        IOUtils.closeWhileHandlingException(cacheFile::startEviction);
    }

    // used in tests
    boolean isCacheFileToSync(CacheFile cacheFile) {
        return cacheFilesToSync.contains(cacheFile);
    }

    /**
     * Synchronize the cache files and their parent directories on disk.
     *
     * This method synchronizes the cache files that have been updated since the last time the method was invoked. To be able to do this,
     * the cache files must notify the {@link CacheService} when they need to be fsync. When a {@link CacheFile} notifies the service the
     * {@link CacheFile} instance is added to the current queue of cache files to synchronize referenced by {@link #cacheFilesToSync}.
     *
     * Cache files are serially synchronized using the {@link CacheFile#fsync()} method. When the {@link CacheFile#fsync()} call returns a
     * non empty set of completed ranges this method also fsync the shard's snapshot cache directory, which is the parent directory of the
     * cache entry. Note that cache files might be evicted during the synchronization.
     */
    protected void synchronizeCache() {
        cacheSyncLock.lock();
        try {
            long count = 0L;
            final Set<Path> cacheDirs = new HashSet<>();
            final long startTimeNanos = threadPool.relativeTimeInNanos();
            final long maxCacheFilesToSync = Math.min(numberOfCacheFilesToSync.get(), this.maxCacheFilesToSyncAtOnce);

            for (long i = 0L; i < maxCacheFilesToSync; i++) {
                if (lifecycleState() != Lifecycle.State.STARTED) {
                    logger.debug("stopping cache synchronization (cache service is closing)");
                    return;
                }

                final CacheFile cacheFile = cacheFilesToSync.poll();
                assert cacheFile != null;

                final long value = numberOfCacheFilesToSync.decrementAndGet();
                assert value >= 0 : value;
                final Path cacheFilePath = cacheFile.getFile();
                try {
                    final SortedSet<Tuple<Long, Long>> ranges = cacheFile.fsync();
                    if (ranges.isEmpty() == false) {
                        logger.trace(
                            "cache file [{}] synchronized with [{}] completed range(s)",
                            cacheFilePath.getFileName(),
                            ranges.size()
                        );
                        final Path cacheDir = cacheFilePath.toAbsolutePath().getParent();
                        if (cacheDirs.add(cacheDir)) {
                            try {
                                IOUtils.fsync(cacheDir, true, false);
                                logger.trace("cache directory [{}] synchronized", cacheDir);
                            } catch (Exception e) {
                                assert e instanceof IOException : e;
                                logger.warn(() -> new ParameterizedMessage("failed to synchronize cache directory [{}]", cacheDir), e);
                            }
                        }
                        // TODO Index searchable snapshot shard information + cache file ranges in Lucene
                        count += 1L;
                    }
                } catch (Exception e) {
                    assert e instanceof IOException : e;
                    logger.warn(() -> new ParameterizedMessage("failed to fsync cache file [{}]", cacheFilePath.getFileName()), e);
                }
            }
            if (logger.isDebugEnabled()) {
                final long elapsedNanos = threadPool.relativeTimeInNanos() - startTimeNanos;
                logger.debug(
                    "cache files synchronization is done ([{}] cache files synchronized in [{}])",
                    count,
                    TimeValue.timeValueNanos(elapsedNanos)
                );
            }
        } finally {
            cacheSyncLock.unlock();
        }
    }

    class CacheSynchronizationTask extends AbstractAsyncTask {

        CacheSynchronizationTask(ThreadPool threadPool, TimeValue interval) {
            super(logger, Objects.requireNonNull(threadPool), Objects.requireNonNull(interval), true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            synchronizeCache();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "cache_synchronization_task";
        }
    }
}
