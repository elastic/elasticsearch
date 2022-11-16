/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

/**
 * {@link CacheService} maintains a cache entry for all files read from searchable snapshot directories (see
 * {@link SearchableSnapshotDirectory}).
 *
 * Cache files created by this service are periodically synchronized on disk in order to make the cached data durable
 * (see {@link #synchronizeCache()} for more information).
 */
public class CacheService extends AbstractLifecycleComponent {

    private static final String SETTINGS_PREFIX = "xpack.searchable.snapshot.cache.";

    public static final ByteSizeValue MIN_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(4, ByteSizeUnit.KB);
    public static final ByteSizeValue MAX_SNAPSHOT_CACHE_RANGE_SIZE = ByteSizeValue.ofBytes(Integer.MAX_VALUE);

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
    private final ConcurrentLinkedQueue<CacheFileEvent> cacheFilesEventsQueue;
    private final CacheFile.ModificationListener cacheFilesListener;
    private final Map<Path, Long> cacheFilesSyncExceptionsLogs;
    private final Map<Path, Long> cacheDirsSyncExceptionsLogs;
    private final AtomicLong numberOfCacheFilesEvents;
    private final CacheSynchronizationTask cacheSyncTask;
    private final TimeValue cacheSyncStopTimeout;
    private final ReentrantLock cacheSyncLock;
    private final PersistentCache persistentCache;
    private final Cache<CacheKey, CacheFile> cache;
    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;
    private final Map<ShardEviction, Future<?>> pendingShardsEvictions;
    private final ReadWriteLock shardsEvictionsLock;
    private final Object shardsEvictionsMutex;

    private volatile int maxCacheFilesToSyncAtOnce;
    private boolean allowShardsEvictions;

    public CacheService(
        final Settings settings,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final PersistentCache persistentCache
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.rangeSize = SNAPSHOT_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.recoveryRangeSize = SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings);
        this.cache = CacheBuilder.<CacheKey, CacheFile>builder()
            .weigher((key, entry) -> entry.getLength())
            // NORELEASE This does not immediately free space on disk, as cache file are only deleted when all index inputs
            // are done with reading/writing the cache file
            .removalListener(notification -> onCacheFileEviction(notification.getValue()))
            .build();
        this.persistentCache = Objects.requireNonNull(persistentCache);
        this.cacheSyncLock = new ReentrantLock();
        this.numberOfCacheFilesEvents = new AtomicLong();
        this.cacheFilesEventsQueue = new ConcurrentLinkedQueue<>();
        this.cacheFilesListener = new CacheFileModificationListener();
        this.cacheFilesSyncExceptionsLogs = new HashMap<>();
        this.cacheDirsSyncExceptionsLogs = new HashMap<>();
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.maxCacheFilesToSyncAtOnce = SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING, this::setMaxCacheFilesToSyncAtOnce);
        this.cacheSyncTask = new CacheSynchronizationTask(threadPool, SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING, this::setCacheSyncInterval);
        this.cacheSyncStopTimeout = SNAPSHOT_CACHE_SYNC_SHUTDOWN_TIMEOUT.get(settings);
        this.shardsEvictionsLock = new ReentrantReadWriteLock();
        this.pendingShardsEvictions = new HashMap<>();
        this.shardsEvictionsMutex = new Object();
        this.allowShardsEvictions = true;
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
                waitForAllPendingShardsEvictions();
            } finally {
                try {
                    persistentCache.close();
                } catch (Exception e) {
                    logger.warn("failed to close persistent cache", e);
                } finally {
                    cacheFilesSyncExceptionsLogs.clear();
                    cacheDirsSyncExceptionsLogs.clear();
                    if (acquired) {
                        cacheSyncLock.unlock();
                    }
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

            return new CacheFile(key, fileLength, path, this.cacheFilesListener);
        });
    }

    /**
     * Get the number of bytes cached for the given shard id in the given snapshot id.
     * @param shardId    shard id
     * @param snapshotId snapshot id
     * @return number of bytes cached
     */
    public long getCachedSize(ShardId shardId, SnapshotId snapshotId) {
        return persistentCache.getCacheSize(shardId, snapshotId);
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
        final SortedSet<ByteRange> cacheFileRanges
    ) throws Exception {

        ensureLifecycleInitializing();
        final Path path = cacheDir.resolve(cacheFileUuid);
        if (Files.exists(path) == false) {
            throw new FileNotFoundException("Cache file [" + path + "] not found");
        }
        cache.put(cacheKey, new CacheFile(cacheKey, fileLength, path, cacheFileRanges, this.cacheFilesListener));
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
     * @param snapshotUUID      the snapshot's unique identifier
     * @param snapshotIndexName the name of the index in the snapshot
     * @param shardId           the {@link ShardId}
     */
    public void markShardAsEvictedInCache(String snapshotUUID, String snapshotIndexName, ShardId shardId) {
        synchronized (shardsEvictionsMutex) {
            if (allowShardsEvictions) {
                final ShardEviction shardEviction = new ShardEviction(snapshotUUID, snapshotIndexName, shardId);
                pendingShardsEvictions.computeIfAbsent(shardEviction, shard -> threadPool.generic().submit(new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        processShardEviction(shardEviction);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> format("failed to evict cache files associated with shard %s", shardEviction), e);
                        assert false : e;
                    }
                }));
            }
        }
    }

    /**
     * Waits for the cache files associated with the shard represented by ({@link SnapshotId}, {@link IndexId}, {@link SnapshotId}) to be
     * evicted if the shard is marked as evicted in cache at the time this method is executed.
     *
     * @param snapshotUUID      the snapshot's unique identifier
     * @param snapshotIndexName the name of the index in the snapshot
     * @param shardId           the {@link ShardId}
     */
    public void waitForCacheFilesEvictionIfNeeded(String snapshotUUID, String snapshotIndexName, ShardId shardId) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        final Future<?> future;
        synchronized (shardsEvictionsMutex) {
            if (allowShardsEvictions == false) {
                throw new AlreadyClosedException("Cannot wait for shard eviction to be processed, cache is stopping");
            }
            future = pendingShardsEvictions.get(new ShardEviction(snapshotUUID, snapshotIndexName, shardId));
            if (future == null) {
                return;
            }
        }
        FutureUtils.get(future);
    }

    /**
     * Evicts the cache files associated to the specified {@link ShardEviction}.
     *
     * @param shardEviction the shard eviction to process
     */
    private void processShardEviction(ShardEviction shardEviction) {
        assert isPendingShardEviction(shardEviction) : "shard is not marked as evicted: " + shardEviction;
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

        shardsEvictionsLock.readLock().lock();
        try {
            try {
                final boolean canEvict;
                synchronized (shardsEvictionsMutex) {
                    canEvict = allowShardsEvictions;
                }
                if (canEvict) {
                    final List<CacheFile> cacheFilesToEvict = new ArrayList<>();
                    cache.forEach((cacheKey, cacheFile) -> {
                        if (shardEviction.matches(cacheKey)) {
                            cacheFilesToEvict.add(cacheFile);
                        }
                    });
                    for (CacheFile cacheFile : cacheFilesToEvict) {
                        try {
                            cache.invalidate(cacheFile.getCacheKey(), cacheFile);
                        } catch (RuntimeException e) {
                            logger.warn(() -> format("failed to evict cache file %s", cacheFile.getCacheKey()), e);
                            assert false : e;
                        }
                    }
                    logger.debug(
                        "shard eviction [{}] processed with [{}] cache files invalidated",
                        shardEviction,
                        cacheFilesToEvict.size()
                    );
                }
            } finally {
                synchronized (shardsEvictionsMutex) {
                    final Future<?> removedFuture = pendingShardsEvictions.remove(shardEviction);
                    assert removedFuture != null;
                }
            }
        } finally {
            shardsEvictionsLock.readLock().unlock();
        }
    }

    /**
     * Waits for all pending shard evictions to complete.
     */
    private void waitForAllPendingShardsEvictions() {
        synchronized (shardsEvictionsMutex) {
            allowShardsEvictions = false;
        }
        boolean success = false;
        try {
            if (shardsEvictionsLock.writeLock().tryLock(10L, TimeUnit.SECONDS) == false) {
                logger.warn("waiting for shards evictions to be processed");
                shardsEvictionsLock.writeLock().lock(); // wait indefinitely
            }
            success = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted while waiting shards evictions to be processed", e);
        } finally {
            if (success) {
                shardsEvictionsLock.writeLock().unlock();
            }
        }
    }

    boolean isPendingShardEviction(ShardEviction shardEviction) {
        synchronized (shardsEvictionsMutex) {
            return pendingShardsEvictions.get(shardEviction) != null;
        }
    }

    // used in tests
    Map<ShardEviction, Future<?>> pendingShardsEvictions() {
        synchronized (shardsEvictionsMutex) {
            return Map.copyOf(pendingShardsEvictions);
        }
    }

    void setCacheSyncInterval(TimeValue interval) {
        cacheSyncTask.setInterval(interval);
    }

    private void setMaxCacheFilesToSyncAtOnce(int maxCacheFilesToSyncAtOnce) {
        this.maxCacheFilesToSyncAtOnce = maxCacheFilesToSyncAtOnce;
    }

    /**
     * This method is invoked after a {@link CacheFile} is evicted from the cache.
     * <p>
     * It notifies the {@link CacheFile}'s eviction listeners that the instance is evicted.
     *
     * @param cacheFile the evicted instance
     */
    private static void onCacheFileEviction(CacheFile cacheFile) {
        IOUtils.closeWhileHandlingException(cacheFile::startEviction);
    }

    // used in tests
    boolean isCacheFileToSync(CacheFile cacheFile) {
        return cacheFilesEventsQueue.stream()
            .filter(event -> event.type == CacheFileEventType.NEEDS_FSYNC)
            .anyMatch(event -> event.value == cacheFile);
    }

    // used in tests
    PersistentCache getPersistentCache() {
        return persistentCache;
    }

    // used in tests
    long getNumberOfCacheFilesEvents() {
        return numberOfCacheFilesEvents.get();
    }

    /**
     * @return the approximate number of events that are present in the cache files events queue. Note that this requires an O(N)
     * computation and should be used with caution for debugging or testing purpose only.
     */
    // used in tests
    long getCacheFilesEventsQueueSize() {
        return cacheFilesEventsQueue.size();
    }

    /**
     * Synchronize the cache files and their parent directories on disk.
     *
     * This method synchronizes the cache files that have been updated since the last time the method was invoked. To be able to do this,
     * the cache files must notify the {@link CacheService} when they need to be fsync. When a {@link CacheFile} notifies the service the
     * {@link CacheFile} instance is added to the current queue of cache files events referenced by {@link #cacheFilesEventsQueue}.
     *
     * Cache files are serially synchronized using the {@link CacheFile#fsync()} method. When the {@link CacheFile#fsync()} call returns a
     * non empty set of completed ranges this method also fsync the shard's snapshot cache directory, which is the parent directory of the
     * cache entry. Note that cache files might be evicted during the synchronization.
     */
    // public for tests only
    public void synchronizeCache() {
        cacheSyncLock.lock();
        try {
            final Set<Path> cacheDirs = new HashSet<>();
            final long startTimeNanos = threadPool.relativeTimeInNanos();
            final long maxCacheFilesToSync = Math.min(numberOfCacheFilesEvents.get(), this.maxCacheFilesToSyncAtOnce);

            if (cacheFilesSyncExceptionsLogs.isEmpty() == false || cacheDirsSyncExceptionsLogs.isEmpty() == false) {
                final long expiredTimeNanos = Math.min(0L, startTimeNanos - TimeUnit.MINUTES.toNanos(10L));
                cacheFilesSyncExceptionsLogs.values().removeIf(lastLogTimeNanos -> expiredTimeNanos >= lastLogTimeNanos);
                cacheDirsSyncExceptionsLogs.values().removeIf(lastLogTimeNanos -> expiredTimeNanos >= lastLogTimeNanos);
            }

            long updates = 0L;
            long deletes = 0L;
            long errors = 0L;

            while ((updates + errors) < maxCacheFilesToSync) {
                if (lifecycleState() != Lifecycle.State.STARTED) {
                    logger.debug("stopping cache synchronization (cache service is closing)");
                    break;
                }
                final CacheFileEvent event = cacheFilesEventsQueue.poll();
                if (event == null) {
                    logger.debug("stopping cache synchronization (no more events to synchronize)");
                    break;
                }

                final long numberOfEvents = numberOfCacheFilesEvents.decrementAndGet();
                assert numberOfEvents >= 0L : numberOfEvents;

                final CacheFile cacheFile = event.value;
                final Path cacheDir = cacheFile.getFile().toAbsolutePath().getParent();
                try {
                    switch (event.type) {
                        case DELETE -> {
                            logger.trace("deleting cache file [{}] from persistent cache", cacheFile.getFile().getFileName());
                            persistentCache.removeCacheFile(cacheFile);
                            deletes += 1L;
                        }
                        case NEEDS_FSYNC -> {
                            final SortedSet<ByteRange> ranges = cacheFile.fsync();
                            logger.trace(
                                "cache file [{}] synchronized with [{}] completed range(s)",
                                cacheFile.getFile().getFileName(),
                                ranges.size()
                            );
                            if (ranges.isEmpty() == false) {
                                boolean shouldPersist = cacheDirs.contains(cacheDir);
                                if (shouldPersist == false) {
                                    try {
                                        IOUtils.fsync(cacheDir, true, false); // TODO evict cache file if fsync failed
                                        logger.trace("cache directory [{}] synchronized", cacheDir);
                                        cacheDirs.add(cacheDir);
                                        shouldPersist = true;
                                    } catch (Exception e) {
                                        if (cacheDirsSyncExceptionsLogs.putIfAbsent(cacheDir, startTimeNanos) == null) {
                                            logger.warn(() -> "failed to synchronize cache directory [" + cacheDir + "]", e);
                                        }
                                        assert e instanceof IOException : e;
                                        shouldPersist = false;
                                    }
                                }
                                if (shouldPersist) {
                                    persistentCache.addCacheFile(cacheFile, ranges);
                                    updates += 1L;
                                }
                            }
                        }
                        default -> throw new IllegalArgumentException("Unknown cache file event [" + event + ']');
                    }
                } catch (Exception e) {
                    if (cacheFilesSyncExceptionsLogs.putIfAbsent(cacheDir, startTimeNanos) == null) {
                        logger.warn(
                            () -> format("failed to process [%s] for cache file [%s]", event.type, cacheFile.getFile().getFileName()),
                            e
                        );
                    }
                    assert e instanceof IOException : e;
                    errors += 1L;
                }
            }
            if (updates > 0 || deletes > 0) {
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
                    updates,
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

    private class CacheFileModificationListener implements CacheFile.ModificationListener {

        /**
         * This method is invoked when a {@link CacheFile} notifies the current {@link CacheService} that it needs to be fsync on disk.
         *
         * @param cacheFile the instance that needs to be fsync
         */
        @Override
        public void onCacheFileNeedsFsync(CacheFile cacheFile) {
            cacheFilesEventsQueue.offer(new CacheFileEvent(CacheFileEventType.NEEDS_FSYNC, cacheFile));
            numberOfCacheFilesEvents.incrementAndGet();
        }

        /**
         * This method is invoked after a {@link CacheFile} is deleted from the disk.
         *
         * @param cacheFile the deleted instance
         */
        @Override
        public void onCacheFileDelete(CacheFile cacheFile) {
            cacheFilesEventsQueue.offer(new CacheFileEvent(CacheFileEventType.DELETE, cacheFile));
            numberOfCacheFilesEvents.incrementAndGet();
        }
    }

    /**
     * Represents the searchable snapshots information of a shard that has been removed from the node. These information are kept around
     * to evict the cache files associated to that shard.
     */
    static class ShardEviction {

        private final String snapshotUUID;
        private final String snapshotIndexName;
        private final ShardId shardId;

        ShardEviction(String snapshotUUID, String snapshotIndexName, ShardId shardId) {
            this.snapshotUUID = snapshotUUID;
            this.snapshotIndexName = snapshotIndexName;
            this.shardId = shardId;
        }

        public String getSnapshotUUID() {
            return snapshotUUID;
        }

        public String getSnapshotIndexName() {
            return snapshotIndexName;
        }

        public ShardId getShardId() {
            return shardId;
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

    private enum CacheFileEventType {
        NEEDS_FSYNC,
        DELETE
    }

    /**
     * Represents an event that occurred on a specified {@link CacheFile}
     */
    public static class CacheFileEvent {

        public final CacheFileEventType type;
        public final CacheFile value;

        private CacheFileEvent(CacheFileEventType type, CacheFile value) {
            assert type != null;
            this.type = type;
            assert value != null;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final CacheFileEvent event = (CacheFileEvent) o;
            return type == event.type && value == event.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, value);
        }

        @Override
        public String toString() {
            return "cache file event [type=" + type + ", value=" + value + ']';
        }
    }
}
