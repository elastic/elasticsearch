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
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

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

    /**
     * If a search needs data from the repository then we expand it to a larger contiguous range whose size is determined by this setting,
     * in anticipation of needing nearby data in subsequent reads. Repository reads typically have quite high latency (think ~100ms) and
     * the default of 32MB for this setting represents the approximate point at which size starts to matter. In other words, reads of
     * ranges smaller than 32MB don't usually happen much quicker, so we may as well expand all the way to 32MB ranges.
     */
    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "range_size",
        new ByteSizeValue(32, ByteSizeUnit.MB),                 // default
        MIN_SNAPSHOT_CACHE_RANGE_SIZE,                          // min
        MAX_SNAPSHOT_CACHE_RANGE_SIZE,                          // max
        Setting.Property.NodeScope
    );

    /**
     * Starting up a shard involves reading small parts of some files from the repository, independently of the pre-warming process. If we
     * expand those ranges using {@link CacheService#SNAPSHOT_CACHE_RANGE_SIZE_SETTING} then we end up reading quite a few 32MB ranges. If
     * we read enough of these ranges for the restore throttling rate limiter to kick in then all the read threads will end up waiting on
     * the throttle, blocking subsequent reads. By using a smaller read size during restore we avoid clogging up the rate limiter so much.
     */
    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "recovery_range_size",
        new ByteSizeValue(128, ByteSizeUnit.KB),                // default
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
    private final PersistentCache persistentCache;
    private final Cache<CacheKey, CacheFile> cache;
    private final ByteSizeValue cacheSize;
    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;
    private final KeyedLock<ShardEviction> shardsEvictionLock;
    private final Set<ShardEviction> evictedShards;

    private volatile int maxCacheFilesToSyncAtOnce;

    public CacheService(
        final Settings settings,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final PersistentCache persistentCache
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        this.rangeSize = SNAPSHOT_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.recoveryRangeSize = SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings);
        this.cache = CacheBuilder.<CacheKey, CacheFile>builder()
            .setMaximumWeight(cacheSize.getBytes())
            .weigher((key, entry) -> entry.getLength())
            // NORELEASE This does not immediately free space on disk, as cache file are only deleted when all index inputs
            // are done with reading/writing the cache file
            .removalListener(notification -> onCacheFileRemoval(notification.getValue()))
            .build();
        this.persistentCache = Objects.requireNonNull(persistentCache);
        this.shardsEvictionLock = new KeyedLock<>();
        this.evictedShards = ConcurrentCollections.newConcurrentSet();
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

    public static Path resolveSnapshotCache(Path path) {
        return path.resolve("snapshot_cache");
    }

    @Override
    protected void doStart() {
        persistentCache.repopulateCache(this);
        cacheSyncTask.rescheduleIfNecessary();
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
        } finally {
            try {
                persistentCache.close();
            } catch (Exception e) {
                logger.warn("failed to close persistent cache", e);
            } finally {
                if (acquired) {
                    cacheSyncLock.unlock();
                }
            }
        }
    }

    @Override
    protected void doClose() {}

    private void ensureLifecycleInitializing() {
        final Lifecycle.State state = lifecycleState();
        assert state == Lifecycle.State.INITIALIZED : state;
        if (state != Lifecycle.State.INITIALIZED) {
            throw new IllegalStateException("Failed to read data from cache: cache service is not initializing [" + state + "]");
        }
    }

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

    /**
     * @return the cache range size (in bytes) to use during recovery (until post_recovery)
     */
    public int getRecoveryRangeSize() {
        return toIntBytes(recoveryRangeSize.getBytes());
    }

    /**
     * Retrieves the {@link CacheFile} instance associated with the specified {@link CacheKey} in the cache. If the key is not already
     * associated with a {@link CacheFile}, this method creates a new instance using the given file length and cache directory.
     *
     * @param cacheKey   the cache key whose associated {@link CacheFile} instance is to be returned or computed for if non-existent
     * @param fileLength the length of the cache file (required to compute a new instance)
     * @param cacheDir   the cache directory where the cache file on disk is created  (required to compute a new instance)
     * @return the current  {@link CacheFile} instance (existing or computed)
     * @throws Exception if this method is used when the {@link CacheService} is not started
     */
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
            cacheFile.set(new CacheFile(key, fileLength, path, () -> onCacheFileUpdate(cacheFile.get())));
            return cacheFile.get();
        });
    }

    /**
     * Computes a new {@link CacheFile} instance using the specified cache file information (file length, file name, parent directory and
     * already available cache ranges) and associates it with the specified {@link CacheKey} in the cache. If the key is already
     * associated with a {@link CacheFile}, the previous instance is replaced by a new one.
     *
     * This method can only be used before the {@link CacheService} is started.
     *
     * @param cacheKey        the cache key with which the new {@link CacheFile} instance is to be associated
     * @param fileLength      the length of the cache file
     * @param cacheDir        the cache directory where the cache file on disk is located
     * @param cacheFileUuid   the name of the cache file on disk (should be a UUID)
     * @param cacheFileRanges the set of ranges that are known to be already available/completed for this cache file
     * @throws Exception if this method is used when the {@link CacheService} is not initializing
     */
    void put(
        final CacheKey cacheKey,
        final long fileLength,
        final Path cacheDir,
        final String cacheFileUuid,
        final SortedSet<Tuple<Long, Long>> cacheFileRanges
    ) throws Exception {

        ensureLifecycleInitializing();
        final Path path = cacheDir.resolve(cacheFileUuid);
        if (Files.exists(path) == false) {
            throw new FileNotFoundException("Cache file [" + path + "] not found");
        }

        final SetOnce<CacheFile> cacheFile = new SetOnce<>();
        cacheFile.set(new CacheFile(cacheKey, fileLength, path, cacheFileRanges, () -> onCacheFileUpdate(cacheFile.get())));
        cache.put(cacheKey, cacheFile.get());
    }

    /**
     * Evicts the cache file associated with the specified cache key.
     *
     * @param cacheKey the {@link CacheKey} whose associated {@link CacheFile} must be evicted from cache
     */
    public void removeFromCache(final CacheKey cacheKey) {
        cache.invalidate(cacheKey);
    }

    /**
     * Marks the specified searchable snapshot shard as evicted in cache. Cache files associated with this shard will be evicted from cache.
     *
     * @param snapshotId the {@link SnapshotId}
     * @param indexId the {@link SnapshotId}
     * @param shardId the {@link SnapshotId}
     */
    public void markShardAsEvictedInCache(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        final ShardEviction shardEviction = new ShardEviction(snapshotId.getUUID(), indexId.getName(), shardId);
        if (evictedShards.add(shardEviction)) {
            threadPool.generic().submit(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    runIfShardMarkedAsEvictedInCache(shardEviction, () -> {
                        assert shardsEvictionLock.isHeldByCurrentThread(shardEviction);
                        final Map<CacheKey, CacheFile> cacheFilesToEvict = new HashMap<>();
                        cache.forEach((cacheKey, cacheFile) -> {
                            if (shardEviction.matches(cacheKey)) {
                                cacheFilesToEvict.put(cacheKey, cacheFile);
                            }
                        });
                        for (Map.Entry<CacheKey, CacheFile> cacheFile : cacheFilesToEvict.entrySet()) {
                            try {
                                cache.invalidate(cacheFile.getKey(), cacheFile.getValue());
                            } catch (RuntimeException e) {
                                assert false : e;
                                logger.warn(() -> new ParameterizedMessage("failed to evict cache file {}", cacheFile.getKey()), e);
                            }
                        }
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e;
                    logger.warn(
                        () -> new ParameterizedMessage("failed to evict cache files associated with evicted shard {}", shardEviction),
                        e
                    );
                }
            });
        }
    }

    /**
     * Allows to run the specified {@link Runnable} if the shard represented by the triplet ({@link SnapshotId}, {@link IndexId},
     * {@link SnapshotId}) is still marked as evicted at the time this method is executed. The @link Runnable} will be executed
     * while the current thread is holding the lock associated to the shard.
     *
     * @param snapshotId the snapshot the evicted searchable snapshots shard belongs to
     * @param indexId    the index in the snapshot the evicted searchable snapshots shard belongs to
     * @param shardId    the searchable snapshots shard id
     * @param runnable   a runnable to execute
     */
    public void runIfShardMarkedAsEvictedInCache(SnapshotId snapshotId, IndexId indexId, ShardId shardId, Runnable runnable) {
        runIfShardMarkedAsEvictedInCache(new ShardEviction(snapshotId.getUUID(), indexId.getName(), shardId), runnable);
    }

    /**
     * Allows to run the specified {@link Runnable} if the shard represented by {@link ShardEviction} is still marked as evicted at the time
     * this method is executed. The @link Runnable} will be executed while the current thread is holding the lock associated to the shard.
     *
     * @param shardEviction a {@link ShardEviction} representing the shard marked as evicted
     * @param runnable      a runnable to execute
     */
    private void runIfShardMarkedAsEvictedInCache(ShardEviction shardEviction, Runnable runnable) {
        try (Releasable ignored = shardsEvictionLock.acquire(shardEviction)) {
            boolean success = false;
            try {
                if (evictedShards.remove(shardEviction)) {
                    runnable.run();
                }
                success = true;
            } finally {
                assert success : "shard eviction should be successful: " + shardEviction;
                if (success == false) {
                    final boolean added = evictedShards.add(shardEviction);
                    assert added : shardEviction;
                }
            }
        }
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
     * It notifies the {@link CacheFile}'s eviction listeners that the instance is evicted and removes it from the persistent cache.
     *
     * @param cacheFile the evicted instance
     */
    private void onCacheFileRemoval(CacheFile cacheFile) {
        IOUtils.closeWhileHandlingException(cacheFile::startEviction);
        try {
            persistentCache.removeCacheFile(cacheFile);
        } catch (Exception e) {
            assert e instanceof IOException : e;
            logger.warn("failed to remove cache file from persistent cache", e);
        }
    }

    // used in tests
    boolean isCacheFileToSync(CacheFile cacheFile) {
        return cacheFilesToSync.contains(cacheFile);
    }

    // used in tests
    PersistentCache getPersistentCache() {
        return persistentCache;
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

                final CacheKey cacheKey = cacheFile.getCacheKey();
                if (evictedShards.contains(
                    new ShardEviction(cacheKey.getSnapshotUUID(), cacheKey.getSnapshotIndexName(), cacheKey.getShardId())
                )) {
                    logger.debug("cache file belongs to a shard marked as evicted, skipping synchronization for [{}]", cacheKey);
                    continue;
                }

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
                        boolean shouldPersist = cacheDirs.contains(cacheDir);
                        if (shouldPersist == false) {
                            try {
                                IOUtils.fsync(cacheDir, true, false); // TODO evict cache file if fsync failed
                                logger.trace("cache directory [{}] synchronized", cacheDir);
                                cacheDirs.add(cacheDir);
                                shouldPersist = true;
                            } catch (Exception e) {
                                assert e instanceof IOException : e;
                                shouldPersist = false;
                                logger.warn(() -> new ParameterizedMessage("failed to synchronize cache directory [{}]", cacheDir), e);
                            }
                        }
                        if (shouldPersist) {
                            persistentCache.addCacheFile(cacheFile, ranges);
                            count += 1L;
                        }
                    }
                } catch (Exception e) {
                    assert e instanceof IOException : e;
                    logger.warn(() -> new ParameterizedMessage("failed to fsync cache file [{}]", cacheFilePath.getFileName()), e);
                }
            }
            if (count > 0 || persistentCache.hasDeletions()) {
                try {
                    persistentCache.commit();
                } catch (IOException e) {
                    logger.error("failed to commit persistent cache after synchronization", e);
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

    /**
     * Represents the searchable snapshots information of a shard that has been removed from the node. These information are kept around
     * to evict the cache files associated to that shard.
     */
    private static class ShardEviction {

        private final String snapshotUUID;
        private final String snapshotIndexName;
        private final ShardId shardId;

        private ShardEviction(String snapshotUUID, String snapshotIndexName, ShardId shardId) {
            this.snapshotUUID = snapshotUUID;
            this.snapshotIndexName = snapshotIndexName;
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardEviction that = (ShardEviction) o;
            return Objects.equals(snapshotUUID, that.snapshotUUID)
                && Objects.equals(snapshotIndexName, that.snapshotIndexName)
                && Objects.equals(shardId, that.shardId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotUUID, snapshotIndexName, shardId);
        }

        @Override
        public String toString() {
            return "[snapshotUUID=" + snapshotUUID + ", snapshotIndexName=" + snapshotIndexName + ", shardId=" + shardId + ']';
        }

        boolean matches(CacheKey cacheKey) {
            return Objects.equals(snapshotUUID, cacheKey.getSnapshotUUID())
                && Objects.equals(snapshotIndexName, cacheKey.getSnapshotIndexName())
                && Objects.equals(shardId, cacheKey.getShardId());
        }
    }
}
