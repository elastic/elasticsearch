/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.isSearchableSnapshotStore;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.toIntBytes;

/**
 * {@link CacheService} maintains a cache entry for all files read from searchable snapshot directories (see
 * {@link org.elasticsearch.index.store.SearchableSnapshotDirectory}).
 *
 * Cache files created by this service are periodically synchronized on disk in order to make the cached data durable. The synchronization
 * is executed over all the searchable snapshot shards that exist in the cluster state and are assigned to the local node at the time the
 * method {@link #synchronizeCache()} is executed.
 */
public class CacheService extends AbstractLifecycleComponent implements ClusterStateListener {

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

    public static final TimeValue MIN_SNAPSHOT_CACHE_SYNC_INTERVAL = TimeValue.timeValueSeconds(10L);
    public static final Setting<TimeValue> SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + "sync_interval",
        TimeValue.timeValueSeconds(60L),                        // default
        MIN_SNAPSHOT_CACHE_SYNC_INTERVAL,                       // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(CacheService.class);

    private final ClusterService clusterService;
    private final NodeEnvironment nodeEnvironment;
    private final ThreadPool threadPool;
    private final CacheSynchronizationTask cacheSyncTask;
    private final Cache<CacheKey, CacheFile> cache;
    private final ByteSizeValue cacheSize;
    private final Runnable cacheCleaner;
    private final ByteSizeValue rangeSize;

    public CacheService(
        final Settings settings,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final NodeEnvironment nodeEnvironment,
        final Runnable cacheCleaner
    ) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.nodeEnvironment = Objects.requireNonNull(nodeEnvironment);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        this.cacheCleaner = Objects.requireNonNull(cacheCleaner);
        this.rangeSize = SNAPSHOT_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.cache = CacheBuilder.<CacheKey, CacheFile>builder()
            .setMaximumWeight(cacheSize.getBytes())
            .weigher((key, entry) -> entry.getLength())
            // NORELEASE This does not immediately free space on disk, as cache file are only deleted when all index inputs
            // are done with reading/writing the cache file
            .removalListener(notification -> IOUtils.closeWhileHandlingException(() -> notification.getValue().startEviction()))
            .build();

        if (DiscoveryNode.isDataNode(settings)) {
            final TimeValue syncInterval = SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.get(settings);
            this.cacheSyncTask = new CacheSynchronizationTask(threadPool, syncInterval);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING, this::setCacheSyncInterval);
            clusterService.addListener(this);
        } else {
            this.cacheSyncTask = null;
        }
    }

    public static Path getShardCachePath(ShardPath shardPath) {
        return resolveSnapshotCache(shardPath.getDataPath());
    }

    static Path resolveSnapshotCache(Path path) {
        return path.resolve("snapshot_cache");
    }

    @Override
    protected void doStart() {
        cacheCleaner.run();
    }

    @Override
    protected void doStop() {
        cache.invalidateAll();
        if (cacheSyncTask != null) {
            cacheSyncTask.close();
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

    // used in tests
    CacheSynchronizationTask getCacheSyncTask() {
        return cacheSyncTask;
    }

    void setCacheSyncInterval(TimeValue interval) {
        assert cacheSyncTask != null;
        cacheSyncTask.setInterval(interval);
    }

    /**
     * Reschedule the {@link CacheSynchronizationTask} if the local data node is hosting searchable snapshot shards.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        assert cacheSyncTask != null;

        if (event.routingTableChanged()) {
            final ClusterState clusterState = event.state();
            final DiscoveryNode localNode = clusterState.getNodes().getLocalNode();
            assert localNode.isDataNode();

            final boolean shouldSynchronize = hasSearchableSnapshotShards(clusterState, localNode.getId());
            cacheSyncTask.allowReschedule.set(shouldSynchronize);

            if (shouldSynchronize == false) {
                logger.trace("canceling cache synchronization task (no searchable snapshots shard(s) assigned to local node)");
                cacheSyncTask.cancel();

            } else if (cacheSyncTask.isScheduled() == false) {
                logger.trace("scheduling cache synchronization task (searchable snapshots shard(s) assigned to local node)");
                cacheSyncTask.rescheduleIfNecessary();
            }
        }
    }

    /**
     * Synchronize the cache files and dirs.
     *
     * This method iterates over all the searchable snapshot shards assigned to the local node in order to execute {@link CacheFile#fsync()}
     * on cache entries belonging to shards. When at least one {@link CacheFile#fsync()} call returns a non empty set of completed ranges
     * this method also fsync the shard's snapshot cache directory, which is the parent directory of the cache entries. Note that this
     * method is best effort as cache entries might be evicted during iterations and cache files/dirs removed from disk.
     */
    protected synchronized void synchronizeCache() {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            return;
        }
        final ClusterState clusterState = clusterService.state();
        final RoutingNode routingNode = clusterState.getRoutingNodes().node(clusterState.getNodes().getLocalNodeId());
        assert routingNode != null;

        final long startTimeNanos = threadPool.relativeTimeInNanos();
        for (ShardRouting shardRouting : routingNode) {
            if (shardRouting.active()) {
                final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(shardRouting.index());
                final Settings indexSettings = indexMetadata.getSettings();
                if (isSearchableSnapshotStore(indexSettings)) {
                    final ShardId shardId = shardRouting.shardId();
                    final SnapshotId snapshotId = new SnapshotId(
                        SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                        SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
                    );
                    final IndexId indexId = new IndexId(
                        SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                        SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
                    );

                    boolean syncDirectory = false;
                    for (Tuple<CacheKey, CacheFile> entry : cache.entries()) {
                        final CacheKey cacheKey = entry.v1();
                        if (cacheKey.belongsTo(snapshotId, indexId, shardId)) {
                            final CacheFile cacheFile = entry.v2();
                            try {
                                final SortedSet<Tuple<Long, Long>> ranges = cacheFile.fsync();
                                if (ranges.isEmpty() == false) {
                                    logger.trace(
                                        "{} cache file [{}] synchronized with [{}] completed range(s)",
                                        shardId,
                                        cacheFile.getFile().getFileName(),
                                        ranges.size()
                                    );
                                    syncDirectory = true;
                                    // TODO Index searchable snapshot shard information + cache file ranges in Lucene
                                }
                            } catch (Exception e) {
                                logger.warn(
                                    () -> new ParameterizedMessage(
                                        "{} failed to fsync cache file [{}]",
                                        shardId,
                                        cacheFile.getFile().getFileName()
                                    ),
                                    e
                                );
                            }
                        }
                    }

                    if (syncDirectory) {
                        assert IndexMetadata.INDEX_DATA_PATH_SETTING.exists(indexSettings) == false;
                        for (Path shardPath : nodeEnvironment.availableShardPaths(shardId)) {
                            final Path snapshotCacheDir = resolveSnapshotCache(shardPath).resolve(snapshotId.getUUID());
                            if (Files.exists(snapshotCacheDir)) {
                                try {
                                    IOUtils.fsync(snapshotCacheDir, true, false);
                                    logger.trace("{} cache directory [{}] synchronized", shardId, snapshotCacheDir);
                                } catch (Exception e) {
                                    logger.warn(
                                        () -> new ParameterizedMessage(
                                            "{} failed to synchronize cache directory [{}]",
                                            shardId,
                                            snapshotCacheDir
                                        ),
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        if (logger.isDebugEnabled()) {
            final long elapsedNanos = threadPool.relativeTimeInNanos() - startTimeNanos;
            logger.debug("cache files synchronized in [{}]", TimeValue.timeValueNanos(elapsedNanos));
        }
    }

    static boolean hasSearchableSnapshotShards(final ClusterState clusterState, final String nodeId) {
        final RoutingNode routingNode = clusterState.getRoutingNodes().node(nodeId);
        if (routingNode != null) {
            for (ShardRouting shardRouting : routingNode) {
                if (shardRouting.active()) {
                    final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(shardRouting.index());
                    if (isSearchableSnapshotStore(indexMetadata.getSettings())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    class CacheSynchronizationTask extends AbstractAsyncTask {

        private final AtomicBoolean allowReschedule = new AtomicBoolean(false);

        CacheSynchronizationTask(ThreadPool threadPool, TimeValue interval) {
            super(logger, Objects.requireNonNull(threadPool), Objects.requireNonNull(interval), true);
        }

        @Override
        protected boolean mustReschedule() {
            return allowReschedule.get();
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
