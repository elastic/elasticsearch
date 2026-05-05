/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.LazyRangeMissingHandler;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.metering.RecoveryMetricsCollector;
import org.elasticsearch.xpack.stateless.utils.IndexingShardWarmingComparator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput.BUFFER_SIZE;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

public class SharedBlobCacheWarmingService {

    public enum Type {
        INDEXING_EARLY(true),
        INDEXING(true),
        INDEXING_MERGE(false),
        // search shard recovery doesn't guarantee that all of region 0 has been cached, because header reads served from
        // index shards are served at page rather than region granularity.
        SEARCH(false),
        HOLLOWING(true),
        UNHOLLOWING(true);

        final boolean skipsWarmingForRegion0Locations;

        Type(boolean skipsWarmingForRegion0Locations) {
            this.skipsWarmingForRegion0Locations = skipsWarmingForRegion0Locations;
        }
    }

    public static final String BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC = "es.blob_cache_warming.page_aligned_bytes.total";
    public static final String BLOB_CACHE_WARMING_ID_LOOKUP_PREWARM_REQS_TOTAL_METRIC =
        "es.blob_cache_warming.id_lookup_prewarm_reqs.total";

    /** Region of a blob **/
    private record BlobRegion(BlobFile blob, int region) {}

    /** Range of a blob to warm in cache, with a listener to complete once it is warmed **/
    private record BlobRange(BlobLocation blobLocation, long position, long length, ActionListener<Void> listener)
        implements
            Comparable<BlobRange> {

        /**
         * Ranges are ordered by decreasing positions in order to fetch them backwards: when fetched from indexing shards, ranges are
         * rounded down more aggressively. By ordering them in backward order, we try to avoid small page aligned forward reads.
         **/
        private static final Comparator<BlobRange> COMPARATOR = Comparator.comparingLong(BlobRange::position).reversed();

        @Override
        public int compareTo(BlobRange other) {
            return COMPARATOR.compare(this, other);
        }
    }

    /** Queue of ranges to warm for a blob region **/
    private static class BlobRangesQueue {

        private final BlobRegion blobRegion;
        private final PriorityBlockingQueue<BlobRange> queue = new PriorityBlockingQueue<>();
        private final AtomicInteger counter = new AtomicInteger();
        private final AtomicLong maxBlobLength = new AtomicLong();

        BlobRangesQueue(BlobRegion blobRegion) {
            this.blobRegion = Objects.requireNonNull(blobRegion);
        }

        /**
         * Adds a range to warm in cache for the current blob region, returning {@code true} if a warming task must be created to warm the
         * range.
         *
         * @param blobLocation  the blob location of the file
         * @param position      the position in the blob where warming must start
         * @param length        the length of bytes to warm
         * @param listener      the listener to complete once the range is warmed
         * @return {@code true} if a warming task must be created to warm the range, {@code false} otherwise
         */
        private boolean add(BlobLocation blobLocation, long position, long length, ActionListener<Void> listener) {
            maxBlobLength.accumulateAndGet(blobLocation.offset() + blobLocation.fileLength(), Math::max);
            queue.add(new BlobRange(blobLocation, position, length, listener));
            return counter.incrementAndGet() == 1;
        }
    }

    private static final Logger logger = LogManager.getLogger(SharedBlobCacheWarmingService.class);

    public static final String PREWARMING_RANGE_MINIMIZATION_STEP_SETTING_NAME = "stateless.blob_cache_warming.minimization_step";
    public static final Setting<ByteSizeValue> PREWARMING_RANGE_MINIMIZATION_STEP = new Setting<>(
        PREWARMING_RANGE_MINIMIZATION_STEP_SETTING_NAME,
        settings -> ByteSizeValue.ofBytes(SHARED_CACHE_RANGE_SIZE_SETTING.get(settings).getBytes() / 4).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, PREWARMING_RANGE_MINIMIZATION_STEP_SETTING_NAME),
        new Setting.Validator<>() {
            @Override
            public void validate(ByteSizeValue value) {
                if (value.getBytes() < 0) {
                    throw new SettingsException("setting [{}] must be non-negative", PREWARMING_RANGE_MINIMIZATION_STEP_SETTING_NAME);
                }
                if (value.getBytes() % SharedBytes.PAGE_SIZE != 0L) {
                    throw new SettingsException(
                        "setting [{}] must be integer multiple of {}",
                        PREWARMING_RANGE_MINIMIZATION_STEP_SETTING_NAME,
                        SharedBytes.PAGE_SIZE
                    );
                }
            }

            @Override
            public void validate(ByteSizeValue value, Map<Setting<?>, Object> settings) {
                final ByteSizeValue rangeSize = (ByteSizeValue) settings.get(SHARED_CACHE_RANGE_SIZE_SETTING);
                if (rangeSize.getBytes() % value.getBytes() != 0L) {
                    throw new SettingsException(
                        "setting [{}] must be integer multiple of setting [{}]",
                        SHARED_CACHE_RANGE_SIZE_SETTING.getKey(),
                        PREWARMING_RANGE_MINIMIZATION_STEP_SETTING_NAME
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(SHARED_CACHE_RANGE_SIZE_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING = Setting.boolSetting(
        "stateless.blob_cache_warming.prewarm_index_shard_for_id_lookups",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // The ratio of term dictionary files to prewarm if prewarming for id lookups.
    // e.g. 0.1 means the first 10% of each .tim file is prewarmed.
    public static final Setting<Double> ID_LOOKUP_PREWARM_RATIO_SETTING = Setting.doubleSetting(
        "stateless.blob_cache_warming.id_lookup_prewarm_ratio",
        0.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME = "stateless.search.offline_warming";
    public static final Setting<Boolean> SEARCH_OFFLINE_WARMING_ENABLED_SETTING = Setting.boolSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING = Setting.boolSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".prefetch_commits.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When search shard recovery waits for blob cache warming on a <strong>relocation target</strong> whose source is not shutting down,
     * and <strong>some node</strong> in the cluster has shutdown metadata, the maximum time to wait before resuming recovery anyway.
     * Non-relocation recoveries do not use this setting.
     */
    public static final Setting<TimeValue> SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING = Setting.timeSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_timeout_relocation_with_shutdown",
        TimeValue.timeValueSeconds(60),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Same situation as {@link #SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING} (relocation target, source not shutting
     * down) but when <strong>no</strong> node in the cluster has shutdown metadata.
     */
    public static final Setting<TimeValue> SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING = Setting.timeSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_timeout_relocation",
        TimeValue.timeValueMinutes(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When the shard is <strong>not</strong> a relocation target, cluster shutdown metadata is not present, and another active copy of the
     * shard exists in the routing table, the maximum time to wait for cache warming before resuming recovery.
     */
    public static final Setting<TimeValue> SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING = Setting.timeSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_timeout_non_relocation",
        TimeValue.timeValueMinutes(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Upper bound on the SIGTERM grace period from shutdown metadata used when computing the shutdown deadline for relocation-source
     * warming timeouts. The effective grace is {@code min(metadata grace, this cap)} so long cluster grace periods do not dominate the
     * calculation (defaults to 14 minutes, i.e. just-in-time for CSP timeout).
     */
    public static final Setting<TimeValue> SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING = Setting.timeSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_grace_period_cap",
        TimeValue.timeValueMinutes(14),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Factor applied to the equal per-shard share of remaining shutdown time when the relocation source is shutting down (SIGTERM with
     * grace period).
     */
    public static final Setting<Double> SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING = Setting.doubleSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_source_shutdown_share_factor",
        // we allow up to 32 concurrent recoveries on the search tier, so we can increase this or make it adaptive
        // keeping it conservative initially.
        1,
        0.0,
        32.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> UPLOAD_PREWARM_MAX_SIZE_SETTING = Setting.byteSizeSetting(
        "stateless.blob_cache_warming.upload_prewarm_max_size",
        ByteSizeValue.ofMb(16),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private final Executor fetchExecutor;
    private final Executor uploadPrewarmFetchExecutor;
    private final ThrottledTaskRunner throttledTaskRunner;
    private final ThrottledTaskRunner cfeThrottledTaskRunner;
    private final LongCounter cacheWarmingPageAlignedBytesTotalMetric;
    private final LongCounter idLookupPrewarmReqsTotalMetric;
    private final long prewarmingRangeMinimizationStep;
    private volatile boolean prefetchCommitsForSearchShardRecovery;
    private volatile boolean searchOfflineWarmingEnabled;
    private volatile boolean prewarmIndexShardForIdLookupsEnabled;
    private volatile double idLookupPrewarmRatio;
    private volatile long maxUploadPrewarmSize;
    private final WarmingRatioProvider warmingRatioProvider;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile TimeValue searchRecoveryWarmingRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingNonRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingGracePeriodCap;
    private volatile double searchRecoveryWarmingSourceShutdownShareFactor;

    public SharedBlobCacheWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        ClusterSettings clusterSettings,
        WarmingRatioProvider warmingRatioProvider
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.warmingRatioProvider = warmingRatioProvider;
        this.fetchExecutor = threadPool.executor(StatelessPlugin.PREWARM_THREAD_POOL);
        this.uploadPrewarmFetchExecutor = threadPool.executor(StatelessPlugin.UPLOAD_PREWARM_THREAD_POOL);

        // the PREWARM_THREAD_POOL does the actual work but we want to limit the number of prewarming tasks in flight at once so that each
        // one completes sooner, so we use a ThrottledTaskRunner. The throttle limit is a little more than the threadpool size just to avoid
        // having the PREWARM_THREAD_POOL stall while the next task is being queued up
        this.throttledTaskRunner = new ThrottledTaskRunner(
            "prewarming-cache",
            1 + threadPool.info(StatelessPlugin.PREWARM_THREAD_POOL).getMax(),
            threadPool.generic() // TODO should be DIRECT, forks to the fetch pool pretty much straight away, but see ES-8448
        );
        // We fork cfe prewarming to the generic pool to avoid blocking stateless_fill_vbcc_cache threads,
        // since their completion can also happen on that pool (and it is sized only for copying prefilled buffers to disk).
        // We have to throttle it, so we do not potentially overload the generic pool with I/O tasks.
        this.cfeThrottledTaskRunner = new ThrottledTaskRunner("cfe-prewarming-cache", 2, threadPool.generic());
        this.cacheWarmingPageAlignedBytesTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC, "Total bytes warmed in cache", "bytes");
        this.idLookupPrewarmReqsTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(BLOB_CACHE_WARMING_ID_LOOKUP_PREWARM_REQS_TOTAL_METRIC, "Total id lookup prewarm requests", "unit");
        this.prewarmingRangeMinimizationStep = clusterSettings.get(PREWARMING_RANGE_MINIMIZATION_STEP).getBytes();
        clusterSettings.initializeAndWatch(
            SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING,
            value -> this.prefetchCommitsForSearchShardRecovery = value
        );
        clusterSettings.initializeAndWatch(SEARCH_OFFLINE_WARMING_ENABLED_SETTING, value -> this.searchOfflineWarmingEnabled = value);
        clusterSettings.initializeAndWatch(UPLOAD_PREWARM_MAX_SIZE_SETTING, value -> this.maxUploadPrewarmSize = value.getBytes());
        clusterSettings.initializeAndWatch(
            PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING,
            value -> this.prewarmIndexShardForIdLookupsEnabled = value
        );
        clusterSettings.initializeAndWatch(ID_LOOKUP_PREWARM_RATIO_SETTING, value -> this.idLookupPrewarmRatio = value);
        clusterSettings.initializeAndWatch(
            SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
            value -> this.searchRecoveryWarmingRelocationWithShutdownTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING,
            value -> this.searchRecoveryWarmingRelocationTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING,
            value -> this.searchRecoveryWarmingNonRelocationTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
            value -> this.searchRecoveryWarmingGracePeriodCap = value
        );
        clusterSettings.initializeAndWatch(
            SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
            value -> this.searchRecoveryWarmingSourceShutdownShareFactor = value
        );
    }

    public void warmCacheBeforeUpload(VirtualBatchedCompoundCommit vbcc, ActionListener<Void> listener) {
        assert vbcc.isFrozen();
        long totalSizeInBytes = vbcc.getTotalSizeInBytes();
        long warmSize = Math.min(totalSizeInBytes, maxUploadPrewarmSize);
        var cacheKey = new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName());
        int endingRegion = cacheService.getEndingRegion(warmSize);
        int regionSize = cacheService.getRegionSize();

        try (var listeners = new RefCountingListener(listener.map(ignored -> null))) {
            for (int region = 0; region <= endingRegion; region++) {
                final long regionOffset = (long) region * regionSize;
                cacheService.maybeFetchRegion(
                    cacheKey,
                    region,
                    // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService to
                    // fully utilize each region. So we just pass it with a value that covers the current region.
                    totalSizeInBytes,
                    (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> ActionListener
                        .completeWith(completionListener, () -> {
                            assert streamFactory == null : streamFactory;
                            try (OutputStream output = new OutputStream() {

                                private final ByteBuffer byteBuffer = writeBuffer.get();
                                private int bytesFlushed = 0;

                                @Override
                                public void write(int b) throws IOException {
                                    byteBuffer.put((byte) b);
                                    if (byteBuffer.hasRemaining() == false) {
                                        doFlush(false);
                                    }
                                }

                                @Override
                                public void write(byte[] b, int off, int len) throws IOException {
                                    int toWrite = len;
                                    while (toWrite > 0) {
                                        int toPut = Math.min(byteBuffer.remaining(), toWrite);
                                        byteBuffer.put(b, off + (len - toWrite), toPut);
                                        toWrite -= toPut;
                                        if (byteBuffer.hasRemaining() == false) {
                                            doFlush(false);
                                        }
                                    }
                                }

                                // We don't override the flush method as we only want to do cache aligned flushes - when the buffer is
                                // full or on close.
                                private void doFlush(boolean closeFlush) throws IOException {
                                    int position = byteBuffer.position();
                                    var bytesCopied = SharedBytes.copyBufferToCacheFileAligned(
                                        channel,
                                        bytesFlushed + channelPos,
                                        byteBuffer
                                    );
                                    bytesFlushed += bytesCopied;
                                    assert closeFlush || bytesCopied == position : bytesCopied + " != " + position;
                                    assert closeFlush || position % SharedBytes.PAGE_SIZE == 0;
                                    assert position > 0;
                                }

                                @Override
                                public void close() throws IOException {
                                    if (byteBuffer.position() > 0) {
                                        doFlush(true);
                                    }
                                    assert byteBuffer.position() == 0;
                                    progressUpdater.accept(bytesFlushed);
                                }
                            }) {
                                long absolutePos = regionOffset + relativePos;
                                vbcc.getBytesByRange(absolutePos, Math.toIntExact(Math.min(len, totalSizeInBytes - regionOffset)), output);
                                return null;
                            }
                        }),
                    uploadPrewarmFetchExecutor,
                    listeners.acquire().map(b -> null)
                );
            }
        }
    }

    public void warmCacheForMerge(
        String mergeId,
        ShardId shardId,
        Store store,
        MergePolicy.OneMerge merge,
        Function<String, BlobLocation> blobLocationResolver
    ) {
        warmCacheMerge(mergeId, shardId, store, merge.segments, blobLocationResolver, merge::isAborted, ActionListener.noop());
    }

    protected void warmCacheMerge(
        String mergeId,
        ShardId shardId,
        Store store,
        List<SegmentCommitInfo> segmentsToMerge,
        Function<String, BlobLocation> blobLocationResolver,
        BooleanSupplier mergeCancelled,
        ActionListener<Void> listener
    ) {
        Type type = Type.INDEXING_MERGE;
        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache [" + type + "] for " + shardId + ", store is closing"));
        } else {
            boolean success = false;
            try {
                WarmingRun warmingRun = new WarmingRun(type, shardId, "merge=" + mergeId, Map.of("prewarming_type", type.name()));
                Set<String> filesToWarm = new HashSet<>();
                final Map<String, BlobLocation> fileLocations = new HashMap<>();

                for (SegmentCommitInfo segmentCommitInfo : segmentsToMerge) {
                    try {
                        filesToWarm.addAll(segmentCommitInfo.files());
                    } catch (IOException e) {
                        listener.onFailure(e);
                        return;
                    }
                }

                for (String fileToWarm : filesToWarm) {
                    // File might not be uploaded yet
                    BlobLocation location = blobLocationResolver.apply(fileToWarm);
                    if (location != null) {
                        fileLocations.put(fileToWarm, location);
                    }
                }
                success = true;
                try (
                    var warmer = new MergeWarmer(
                        warmingRun,
                        store::isClosing,
                        fileLocations,
                        segmentsToMerge.size(),
                        mergeCancelled,
                        BlobStoreCacheDirectory.unwrapDirectory(store.directory()),
                        ActionListener.runAfter(listener, store::decRef)
                    )
                ) {
                    warmer.run();
                }
            } finally {
                if (success == false) {
                    store.decRef();
                }
            }

        }

    }

    /**
     * Warms the cache to optimize cache hits during the recovery or unhollowing. The warming happens concurrently
     * with the recovery or the unhollowing and doesn't block it.
     *
     * <p>
     * Warming runs concurrently with recovery unless the caller waits on {@code listener}; use {@link ActionListener#noop()} when
     * completion does not need to be observed.
     * </p>
     *
     * <p>
     * This method uses the list of files of the recovered commit to identify which region(s) of the compound commit blob are likely to be
     * accessed first. It then tries to fetch every region to write them in cache. Note that regions are fetched completely, ie not only the
     * parts required for accessing one or more files. If the cache is under contention then one or more regions may be skipped and not
     * warmed up. If a region is pending to be written to cache by another thread, the warmer skips the region and starts warming the next
     * one without waiting for the region to be available in cache.
     * </p>
     *
     * @param type a type of which warming this is (to distinguish between the many that may be performed in log messages)
     * @param indexShard the shard to warm in cache
     * @param commit the commit to be recovered
     * @param endOffsetsToWarm optional up-to offset (exclusive) in the {@code BlobFile} to warm in cache,
     *                         in addition to regular recovery warming, grouped by {@code BlobFile}s
     */
    public void warmCacheForShardRecoveryOrUnhollowing(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Long> endOffsetsToWarm,
        ActionListener<Void> listener
    ) {
        warmCache(type, indexShard, commit, directory, endOffsetsToWarm, false, listener);
    }

    public void warmCacheForShardRecoveryOrUnhollowing(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Long> endOffsetsToWarm,
        boolean preWarmForIdLookup,
        ActionListener<Void> listener
    ) {
        warmCache(type, indexShard, commit, directory, endOffsetsToWarm, preWarmForIdLookup, listener);
    }

    /**
     * Search shard recovery path: warms the blob cache for the recovered commit and completes {@code resumeRecoveryListener} when recovery
     * may proceed. When {@code endOffsetsToWarm} is non-null (internal replicated-files path), {@link #searchRecoveryTimeout} decides
     * whether to race warming against a timeout; otherwise recovery resumes as soon as warming has been scheduled (fire-and-forget via
     * {@link ActionListener#noop()}).
     *
     * Notice that this may synchronously invoke the listener.
     */
    public void warmCacheForSearchShardRecovery(
        ClusterState clusterState,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Long> endOffsetsToWarm,
        ActionListener<Void> resumeRecoveryListener
    ) {
        SearchRecoveryTimeout plan = endOffsetsToWarm != null
            ? searchRecoveryTimeout(clusterState, indexShard)
            : SearchRecoveryTimeout.skip();
        if (plan.awaitWarming()) {
            warmCache(
                Type.SEARCH,
                indexShard,
                commit,
                directory,
                endOffsetsToWarm,
                false,
                searchRecoveryWarmingListener(plan.timeout(), plan.timeoutContext(), indexShard, resumeRecoveryListener)
            );
        } else {
            warmCache(Type.SEARCH, indexShard, commit, directory, endOffsetsToWarm, false, ActionListener.noop());
            resumeRecoveryListener.onResponse(null);
        }
    }

    protected void warmCache(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Long> endOffsetsToWarm,
        boolean preWarmForIdLookup,
        ActionListener<Void> listener
    ) {
        ShardId shardId = indexShard.shardId();
        Store store = indexShard.store();
        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache [" + type + "] for " + shardId + ", store is closing"));
        } else {
            try (var listeners = new RefCountingListener(ActionListener.runAfter(listener, store::decRef))) {
                // special search shard prewarming based on timestamp range of CCs (more recent data is warmed more)
                if (type == Type.SEARCH && (prefetchCommitsForSearchShardRecovery || searchOfflineWarmingEnabled)) {
                    SubscribableListener.<Map<BlobFile, Long>>newForked(l1 -> {
                        if (endOffsetsToWarm == null) {
                            Map<BlobFile, Long> offsetsToWarmComputed = ConcurrentCollections.newConcurrentMap();
                            ObjectStoreService.readReferencedCompoundCommitsUsingCache(
                                commit.commitFiles(),
                                // do not pass in any previously read BCC, because we actually want to ensure that the
                                // referenced CCs (headers) in this BCC are also populated in the cache
                                null,
                                directory,
                                BlobCacheIndexInput.WARMING,
                                // cannot run on the {@link PREWARM_THREAD_POOL} because this triggers AND waits for cache population,
                                // which itself runs on the {@link PREWARM_THREAD_POOL}, potentially triggering a deadlock
                                throttledTaskRunner.asExecutor(),
                                referencedCompoundCommit -> {
                                    if (searchOfflineWarmingEnabled) {
                                        offsetsToWarmComputed.compute(
                                            referencedCompoundCommit.statelessCompoundCommitReference().bccBlobFile(),
                                            (blobFile, maxOffsetToWarm) -> {
                                                var offset = byteRangeToWarmForCC(referencedCompoundCommit).end();
                                                return maxOffsetToWarm == null ? offset : Math.max(maxOffsetToWarm, offset);
                                            }
                                        );
                                    }
                                },
                                l1.map(aVoid -> offsetsToWarmComputed)
                            );
                        } else {
                            l1.onResponse(endOffsetsToWarm);
                        }
                    }).<Void>andThen((l2, offsetsToWarmFinal) -> {
                        if (searchOfflineWarmingEnabled) {
                            warmBlobOffsets(indexShard, offsetsToWarmFinal, l2);
                        } else {
                            l2.onResponse(null);
                        }
                    }).addListener(listeners.acquire());
                }

                // regular shard recovery prewarming, which prefetches lucene files (headers & footers) required for shard recovery
                final var warmingRun = new WarmingRun(
                    type,
                    indexShard.shardId(),
                    "generation=" + commit.generation(),
                    Maps.copyMapWithAddedEntry(RecoveryMetricsCollector.commonMetricLabels(indexShard), "prewarming_type", type.name())
                );
                boolean preWarmForIdLookupRequested = preWarmForIdLookup && (type == Type.INDEXING_EARLY || type == Type.INDEXING);
                if (preWarmForIdLookupRequested) {
                    idLookupPrewarmReqsTotalMetric.incrementBy(1, Map.of("es_blob_cache_prewarming_type", type.name()));
                }
                try (
                    // warming up the latest commit upon recovery/unhollowing will fetch a few regions of every active
                    // segment (the first region of every segment is always fetched)
                    var warmer = new ShardWarmer(
                        warmingRun,
                        indexShard,
                        store::isClosing,
                        commit.commitFiles(),
                        directory,
                        preWarmForIdLookupRequested && prewarmIndexShardForIdLookupsEnabled,
                        listeners.acquire()
                    )
                ) {
                    warmer.run();
                }
            }
        }
    }

    /**
     * Search shard recovery warming for the internal replicated-files path: {@link #timeout()} drives the race in
     * {@link #searchRecoveryWarmingListener}; {@link TimeValue#ZERO} means do not await warming. Use {@link #awaitWarming()} to branch.
     */
    public record SearchRecoveryTimeout(TimeValue timeout, String timeoutContext) {

        public static SearchRecoveryTimeout skip() {
            return new SearchRecoveryTimeout(TimeValue.ZERO, "");
        }

        /** When {@code true}, recovery should use {@link #searchRecoveryWarmingListener} with {@link #timeout()} (which is then &gt; 0). */
        public boolean awaitWarming() {
            return timeout.millis() > 0;
        }
    }

    /**
     * When to await search recovery warming (internal replicated-files path only). Relocation targets use relocation-specific timeouts or
     * a computed share when the source is shutting down. Non-relocation: wait only if another active search shard copy exists and there
     * is no cluster shutdown metadata, using {@link #SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING}.
     */
    public SearchRecoveryTimeout searchRecoveryTimeout(ClusterState state, IndexShard indexShard) {
        final ShardRouting shardRouting = indexShard.routingEntry();
        assert shardRouting.isPromotableToPrimary() == false;
        if (isRelocationTarget(shardRouting)) {
            final String sourceNodeId = shardRouting.relocatingNodeId();
            assert sourceNodeId != null;
            if (state.metadata().nodeShutdowns().isNodeMarkedForRemoval(sourceNodeId)) {
                final TimeValue computed = computeRelocationSourceShutdownWarmingTimeout(state, sourceNodeId);
                return new SearchRecoveryTimeout(
                    computed,
                    "relocation source shutting down (equal share of remaining time to capped grace deadline)"
                );
            }
            if (hasActiveShutdownForRemovalNodes(state)) {
                return new SearchRecoveryTimeout(
                    searchRecoveryWarmingRelocationWithShutdownTimeout,
                    "relocation source not shutting down, cluster shutdown metadata present"
                );
            }
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationTimeout,
                "relocation source not shutting down, no cluster shutdown"
            );
        }
        if (hasAnotherActiveSearchShardCopy(state, indexShard) && hasActiveShutdownForRemovalNodes(state) == false) {
            return new SearchRecoveryTimeout(searchRecoveryWarmingNonRelocationTimeout, "not a relocation, another active shard copy");
        }
        return SearchRecoveryTimeout.skip();
    }

    /**
     * Completes {@code resumeRecoveryListener} when warming finishes, or when {@code timeout} elapses (whichever comes first).
     */
    public ActionListener<Void> searchRecoveryWarmingListener(
        TimeValue timeout,
        String timeoutContext,
        IndexShard indexShard,
        ActionListener<Void> resumeRecoveryListener
    ) {
        assert timeout.millis() > 0;
        final SubscribableListener<Void> race = new SubscribableListener<>();
        final var cancellable = threadPool.schedule(() -> {
            logger.warn(
                "Search shard recovery cache warming timed out after [{}] ({}) for {}",
                timeout,
                timeoutContext.isEmpty() ? "default" : timeoutContext,
                indexShard.shardId()
            );
            race.onResponse(null);
        }, timeout, threadPool.generic());
        race.addListener(
            ActionListener.runBefore(new ThreadedActionListener<>(threadPool.generic(), resumeRecoveryListener), cancellable::cancel)
        );
        return race;
    }

    private static boolean isRelocationTarget(ShardRouting self) {
        return self.initializing() && self.relocatingNodeId() != null;
    }

    /**
     * Whether the routing table has at least one active search routing for this logical shard
     */
    private static boolean hasAnotherActiveSearchShardCopy(ClusterState state, IndexShard indexShard) {
        final var projectId = state.metadata().projectFor(indexShard.shardId().getIndex()).id();
        final IndexShardRoutingTable shardTable = state.routingTable(projectId).shardRoutingTable(indexShard.shardId());
        return shardTable.getActiveSearchShardCount() > 0;
    }

    private static int countShardsOnNode(ClusterState clusterState, String nodeId) {
        var node = clusterState.getRoutingNodes().node(nodeId);
        return node == null ? 0 : node.size();
    }

    private static boolean hasActiveShutdownForRemovalNodes(ClusterState state) {
        for (Map.Entry<String, SingleNodeShutdownMetadata> entry : state.metadata().nodeShutdowns().getAll().entrySet()) {
            if (entry.getValue().getType().isRemovalType() && state.nodes().nodeExists(entry.getKey())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Per-shard timeout: {@code factor * (deadline - now) / shardsOnSource} with {@code deadline = start + min(metadata grace, cap)}.
     */
    private TimeValue computeRelocationSourceShutdownWarmingTimeout(ClusterState state, String sourceNodeId) {
        final var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        assert shutdown != null;
        TimeValue grace = shutdown.getGracePeriod();
        if (grace == null) {
            grace = searchRecoveryWarmingGracePeriodCap;
        }
        final long effectiveGraceMillis = Math.min(grace.getMillis(), searchRecoveryWarmingGracePeriodCap.millis());
        final long now = threadPool.absoluteTimeInMillis();
        final long deadline = shutdown.getStartedAtMillis() + effectiveGraceMillis;
        final long remaining = deadline - now;
        if (remaining <= 0) {
            return TimeValue.ZERO;
        }
        int shardsOnSource = countShardsOnNode(state, sourceNodeId);
        if (shardsOnSource <= 0) {
            shardsOnSource = 1;
        }
        final double timeoutMs = (remaining / (double) shardsOnSource) * searchRecoveryWarmingSourceShutdownShareFactor;
        return TimeValue.timeValueMillis(Math.round(timeoutMs));
    }

    public ByteRange byteRangeToWarmForCC(ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCC) {
        final double warmingRatio = calculateWarmingRatioFromCompoundCommit(referencedCC, threadPool.absoluteTimeInMillis());
        assert warmingRatio >= 0.0;
        if (warmingRatio <= 0) {
            return ByteRange.EMPTY;
        } else {
            final long startPosition = referencedCC.statelessCompoundCommitReference().headerOffsetInTheBccBlobFile();
            final long sizeInBytes = referencedCC.statelessCompoundCommitReference().compoundCommit().sizeInBytes();
            final long warmEndExclusive = startPosition + Math.round(sizeInBytes * warmingRatio);
            final long commitEndExclusive = startPosition + sizeInBytes;
            // Cap at the compound commit end. The previous region-floor heuristic could shrink the range below commitEndExclusive
            // when the warm end fell in the first half of a cache region, leaving the commit tail cold.
            return ByteRange.of(startPosition, Math.min(warmEndExclusive, commitEndExclusive));
        }
    }

    // protected for tests
    protected void warmBlobOffsets(IndexShard indexShard, Map<BlobFile, Long> offsetsToWarmPerBlobFile, ActionListener<Void> listener) {
        try (RefCountingListener listeners = new RefCountingListener(listener)) {
            for (var offsetsToWarm : offsetsToWarmPerBlobFile.entrySet()) {
                // Warm from the start of the blob through the computed end. We used to skip the first cache region, assuming
                // readReferencedCompoundCommitsUsingCache had fully populated it; header-sized reads can leave gaps in that region,
                // so searches can miss until the full range is forced into cache (see RecoveryWarmer#shouldSkipLocationWarming for SEARCH).
                if (offsetsToWarm.getValue() > 0) {
                    warmBlobByteRange(
                        Type.SEARCH,
                        indexShard,
                        offsetsToWarm.getKey(),
                        ByteRange.of(0, offsetsToWarm.getValue()),
                        listeners.acquire()
                    );
                }
            }
        }
    }

    private void warmBlobByteRange(
        Type type,
        IndexShard indexShard,
        BlobFile blobFile,
        ByteRange byteRangeToWarm,
        ActionListener<Void> listener
    ) {
        final Store store = indexShard.store();
        final ShardId shardId = indexShard.shardId();
        final var warmingRun = new WarmingRun(type, shardId, "prewarm", Map.of("prewarming_type", type.name()));
        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache [" + type + "] for " + shardId + ", store is closing"));
        } else {
            try (
                var warmer = new BlobByteRangeWarmer(
                    warmingRun,
                    blobFile,
                    byteRangeToWarm,
                    store::isClosing,
                    BlobStoreCacheDirectory.unwrapDirectory(store.directory()),
                    ActionListener.runAfter(listener, store::decRef)
                )
            ) {
                warmer.run();
            }
        }
    }

    private double calculateWarmingRatioFromCompoundCommit(
        ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCompoundCommit,
        long nowMillis
    ) {
        return warmingRatioProvider.getWarmingRatio(referencedCompoundCommit, nowMillis);
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> {
        assert ThreadPool.assertCurrentThreadPool(
            StatelessPlugin.PREWARM_THREAD_POOL,
            StatelessPlugin.UPLOAD_PREWARM_THREAD_POOL,
            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        );
        return ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE);
    });

    private record WarmingRun(Type type, ShardId shardId, String logIdentifier, Map<String, Object> labels) {}

    protected void scheduleWarmingTask(ActionListener<Releasable> warmTask) {
        throttledTaskRunner.enqueueTask(warmTask);
    }

    private class ShardWarmer extends AbstractWarmer {

        private final ConcurrentMap<BlobRegion, BlobRangesQueue> queues = new ConcurrentHashMap<>();
        private final IndexShard indexShard;
        private final Map<String, BlobLocation> filesToWarm;
        private final int segmentCount;
        protected final AtomicLong skippedTasksCount = new AtomicLong(0L);
        private final boolean preWarmForIdLookup;

        ShardWarmer(
            WarmingRun warmingRun,
            IndexShard indexShard,
            Supplier<Boolean> isStoreClosing,
            Map<String, BlobLocation> filesToWarm,
            BlobStoreCacheDirectory directory,
            boolean preWarmForIdLookup,
            ActionListener<Void> listener
        ) {
            super(warmingRun, isStoreClosing, directory, listener);
            this.indexShard = indexShard;
            this.filesToWarm = Collections.unmodifiableMap(filesToWarm);
            this.segmentCount = segmentCount(filesToWarm);
            this.preWarmForIdLookup = preWarmForIdLookup;
        }

        void run() {
            filesToWarm.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(new IndexingShardWarmingComparator()))
                .forEach(entry -> addFile(entry.getKey(), LuceneFilesExtensions.fromFile(entry.getKey()), entry.getValue()));
        }

        @Override
        protected boolean isCancelled() {
            return super.isCancelled()
                || (indexShard.state() != IndexShardState.RECOVERING
                    && warmingRun.type != Type.UNHOLLOWING
                    && warmingRun.type != Type.HOLLOWING);
        }

        @Override
        protected void onWarmingSuccess(long duration) {
            logger.log(
                duration >= 5000 ? Level.INFO : Level.DEBUG,
                "{} {} warming completed in {} ms ({} segments, {} files, {} tasks, {} skipped tasks, {} bytes)",
                warmingRun.shardId(),
                warmingRun.type(),
                duration,
                segmentCount,
                filesToWarm.size(),
                tasksCount.get(),
                skippedTasksCount.get(),
                totalBytesCopied.get()
            );
        }

        private static int segmentCount(Map<String, BlobLocation> filesToWarm) {
            return Math.toIntExact(
                filesToWarm.keySet().stream().filter(file -> LuceneFilesExtensions.fromFile(file) == LuceneFilesExtensions.SI).count()
            );
        }

        /**
         * Finds and scheduled the regions of the compound commit blob that must be warmed up for the given file.
         * <p>
         * The regions to warm are:
         * - the region containing the file header (but the size of the header is unknown so 1024 bytes will be requested)
         * - the region containing the file footer (usually 16 bytes)
         * If the file is a Lucene metadata file or is less than 1024 bytes then it is fully requested to compute the region(s).
         * Additionally this detects and warms the CFE entries
         * </p>
         * @param fileName      the name of the Lucene physical file (ie, for files embedded in .cfs segment this is the .cfs file name)
         * @param fileExtension the extension of the Lucene file (ie, for files embedded in .cfs segment this is the entry's file extension)
         * @param blobLocation  the blob location of the Lucene file
         */
        private void addFile(String fileName, @Nullable LuceneFilesExtensions fileExtension, BlobLocation blobLocation) {
            if (isCancelled()) {
                // stop warming
            } else if (fileExtension == LuceneFilesExtensions.CFE) {
                SubscribableListener
                    // warm entire CFE file
                    .<Void>newForked(listener -> addLocation(blobLocation, fileName, listener))
                    // parse it and schedule warming of corresponding parts of CFS file
                    .andThenAccept(ignored -> addCfe(fileName))
                    .addListener(listeners.acquire());
            } else if (shouldFullyWarmUp(fileName, fileExtension, preWarmForIdLookup) || blobLocation.fileLength() <= BUFFER_SIZE) {
                // warm entire file when it is small or required for id lookup
                addLocation(blobLocation, fileName, listeners.acquire());
            } else {
                // header
                final var length = getHeaderPreWarmSize(
                    fileExtension,
                    blobLocation.fileLength(),
                    preWarmForIdLookup ? idLookupPrewarmRatio : 0.0
                );
                addLocation(blobLocation, fileName, blobLocation.offset(), length, listeners.acquire());
                // footer
                addLocation(
                    blobLocation,
                    fileName,
                    blobLocation.offset() + blobLocation.fileLength() - CodecUtil.footerLength(),
                    CodecUtil.footerLength(),
                    listeners.acquire()
                );
            }
        }

        private static long getHeaderPreWarmSize(LuceneFilesExtensions fileExtension, long length, double idLookupPreWarmRatio) {
            if (fileExtension != LuceneFilesExtensions.TIM) {
                return BUFFER_SIZE;
            }
            final var value = Math.max((long) (idLookupPreWarmRatio * length), BUFFER_SIZE);
            return value;
        }

        private void addLocation(BlobLocation location, String fileName, ActionListener<Void> listener) {
            addLocation(location, fileName, location.offset(), location.fileLength(), listener);
        }

        private void addLocation(BlobLocation location, String fileName, long position, long length, ActionListener<Void> listener) {
            final long start = length <= Integer.MAX_VALUE ? directory.getPosition(fileName, position, (int) length) : position;
            final long end = start + length;
            final int regionSize = cacheService.getRegionSize();
            final int startRegion = cacheService.getRegion(start);
            final int endRegion = cacheService.getEndingRegion(end);

            if (startRegion == endRegion) {
                BlobRegion blobRegion = new BlobRegion(location.blobFile(), startRegion);
                enqueueLocation(blobRegion, location, start, length, listener);
            } else {
                try (var listeners = new RefCountingListener(listener)) {
                    for (int r = startRegion; r <= endRegion; r++) {
                        // adjust the position & length to the region
                        var range = ByteRange.of(Math.max(start, (long) r * regionSize), Math.min(end, (r + 1L) * regionSize));
                        BlobRegion blobRegion = new BlobRegion(location.blobFile(), r);
                        enqueueLocation(blobRegion, location, range.start(), range.length(), listeners.acquire());
                    }
                }
            }
        }

        private void addCfe(String fileName) {
            assert LuceneFilesExtensions.fromFile(fileName) == LuceneFilesExtensions.CFE : fileName;
            // We spawn to the generic pool here (via a throttled task runner), so that we have the following invocation path across
            // the thread pools: GENERIC (recovery/hollowing/unhollowing) -> FILL_VBCC_THREAD_POOL (if fetching from indexing node)
            // -> GENERIC.
            // We expect no blocking here since `addCfe` gets called AFTER warming the region.
            cfeThrottledTaskRunner.enqueueTask(listeners.acquire().map(ref -> {
                try (ref) {
                    if (isCancelled()) {
                        return null;
                    }
                    try (var in = directory.openInput(fileName, IOContext.READONCE)) {
                        var entries = Lucene90CompoundEntriesReader.readEntries(in);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Detected {} entries in {}: {}", entries.size(), fileName, entries);
                        }

                        var cfs = fileName.replace(".cfe", ".cfs");
                        var cfsLocation = filesToWarm.get(cfs);
                        assert cfsLocation != null : cfs;

                        entries.entrySet()
                            .stream()
                            .sorted(Map.Entry.comparingByKey(new IndexingShardWarmingComparator()))
                            .forEach(
                                entry -> addFile(
                                    cfs,
                                    LuceneFilesExtensions.fromFile(entry.getKey()),
                                    new BlobLocation(
                                        cfsLocation.blobFile(),
                                        cfsLocation.offset() + entry.getValue().offset(),
                                        entry.getValue().length()
                                    )
                                )
                            );
                        return null;
                    }
                }
            }));
        }

        private boolean shouldSkipLocationWarming(long position) {
            if (warmingRun.type.skipsWarmingForRegion0Locations == false) {
                return false;
            }
            // region 0 is already loaded by this point while resolving full set of commit files and safe to skip.
            // See org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService#readIndexingShardState
            // reads that span regions will be split into separate tasks, so checking the starting position suffices.
            return position / cacheService.getRegionSize() == 0;
        }

        // When using replicated ranges, the location should already be translated to its replicated counterpart
        private void enqueueLocation(
            BlobRegion blobRegion,
            BlobLocation blobLocation,
            long position,
            long length,
            ActionListener<Void> listener
        ) {
            if (shouldSkipLocationWarming(position)) {
                skippedTasksCount.incrementAndGet();
                listener.onResponse(null);
                return;
            }

            var blobRanges = queues.computeIfAbsent(blobRegion, BlobRangesQueue::new);
            if (blobRanges.add(blobLocation, position, length, listener)) {
                scheduleWarmingTask(new WarmingTask(blobRanges));
            }
        }

        private static boolean shouldFullyWarmUp(
            String fileName,
            @Nullable LuceneFilesExtensions fileExtension,
            boolean preWarmForIdLookup
        ) {
            return fileExtension == null // segments_N are fully warmed up in cache
                || fileExtension.isMetadata() // metadata files
                || StatelessCompoundCommit.isGenerationalFile(fileName) // generational files
                || (preWarmForIdLookup && fileExtension == LuceneFilesExtensions.TIP) // term index for id lookup
                || (preWarmForIdLookup && fileExtension == LuceneFilesExtensions.BFI); // bloom filter index for id lookup
        }
    }

    private class MergeWarmer extends AbstractWarmer {

        private final Collection<BlobLocation> locationsToWarm;
        private final BooleanSupplier mergeCancelled;
        private final int segmentCount;

        MergeWarmer(
            WarmingRun warmingRun,
            Supplier<Boolean> isStoreClosing,
            Map<String, BlobLocation> filesToWarm,
            int segmentCount,
            BooleanSupplier mergeCancelled,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            super(warmingRun, isStoreClosing, directory, listener);
            this.locationsToWarm = filesToWarm.values();
            this.mergeCancelled = mergeCancelled;
            this.segmentCount = segmentCount;
        }

        void run() {
            HashMap<BlobFile, Long> locations = new HashMap<>();
            for (BlobLocation location : locationsToWarm) {
                // compute the largest position in the blob that needs to be warmed
                locations.compute(location.blobFile(), (blobFile, existingLength) -> {
                    long embeddedEndOffset = location.offset() + location.fileLength();
                    if (existingLength == null) {
                        return embeddedEndOffset;
                    } else {
                        return Math.max(embeddedEndOffset, existingLength);
                    }
                });

            }

            locations.forEach(
                (blobFile, length) -> scheduleWarmingTask(
                    new WarmBlobLocationTask(new BlobLocation(blobFile, 0, length), listeners.acquire())
                )
            );
        }

        @Override
        protected boolean isCancelled() {
            return super.isCancelled() || mergeCancelled.getAsBoolean();
        }

        @Override
        protected void onWarmingSuccess(long duration) {
            logger.log(
                duration >= 5000 ? Level.INFO : Level.DEBUG,
                "{} {} warming completed in {} ms ({} segments, {} files, {} tasks, {} bytes)",
                warmingRun.shardId(),
                warmingRun.type(),
                duration,
                segmentCount,
                locationsToWarm.size(),
                tasksCount.get(),
                totalBytesCopied.get()
            );
        }
    }

    /**
     * Warms an arbitrary byte range from the blob file.
     * The caller must ensure that the {@link ByteRange} is inside the limits of the blob file.
     */
    private class BlobByteRangeWarmer extends SharedBlobCacheWarmingService.AbstractWarmer {
        private final BlobFile blobFile;
        private final ByteRange byteRangeToWarm;

        BlobByteRangeWarmer(
            SharedBlobCacheWarmingService.WarmingRun warmingRun,
            BlobFile blobFile,
            ByteRange byteRangeToWarm,
            Supplier<Boolean> isStoreClosing,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            super(warmingRun, isStoreClosing, directory, listener);
            this.blobFile = blobFile;
            this.byteRangeToWarm = byteRangeToWarm;
        }

        void run() {
            scheduleWarmingTask(new WarmBlobByteRangeTask(blobFile, byteRangeToWarm, listeners.acquire()));
        }

        @Override
        protected void onWarmingSuccess(long duration) {
            logger.log(
                duration >= 5000 ? Level.INFO : Level.DEBUG,
                "{} {} warming {} completed in {} ms ({}, {} tasks, {} bytes copied to cache)",
                warmingRun.shardId(),
                warmingRun.type(),
                blobFile.termAndGeneration(),
                duration,
                byteRangeToWarm,
                tasksCount.get(),
                totalBytesCopied.get()
            );
        }
    }

    // protected for tests
    protected abstract class AbstractWarmer implements Releasable {

        protected final WarmingRun warmingRun;
        protected final BlobStoreCacheDirectory directory;
        protected final Supplier<Boolean> isStoreClosing;
        protected final RefCountingListener listeners;
        protected final AtomicLong tasksCount = new AtomicLong(0L);
        protected final AtomicLong totalBytesCopied = new AtomicLong(0L);

        AbstractWarmer(
            WarmingRun warmingRun,
            Supplier<Boolean> isStoreClosing,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            this.warmingRun = warmingRun;
            this.isStoreClosing = isStoreClosing;
            this.directory = directory;
            this.listeners = new RefCountingListener(metering(logging(listener)));
        }

        private ActionListener<Void> logging(ActionListener<Void> target) {
            final long started = threadPool.rawRelativeTimeInMillis();
            logger.debug("{} {} warming, {}", warmingRun.shardId(), warmingRun.type(), warmingRun.logIdentifier());
            return ActionListener.runBefore(target, () -> {
                final long duration = threadPool.rawRelativeTimeInMillis() - started;
                onWarmingSuccess(duration);
            }).delegateResponse((l, e) -> {
                onWarmingFailed(e);
                l.onFailure(e);
            });
        }

        private ActionListener<Void> metering(ActionListener<Void> target) {
            return ActionListener.runAfter(
                target,
                () -> cacheWarmingPageAlignedBytesTotalMetric.incrementBy(totalBytesCopied.get(), warmingRun.labels())
            );
        }

        @Override
        public void close() {
            listeners.close();
        }

        protected abstract void onWarmingSuccess(long duration);

        protected void onWarmingFailed(Exception e) {
            Supplier<String> logMessage = () -> Strings.format("%s %s warming failed", warmingRun.shardId(), warmingRun.type());
            if (logger.isDebugEnabled()) {
                logger.debug(logMessage, e);
            } else {
                logger.info(logMessage);
            }
        }

        protected void scheduleWarmingTask(ActionListener<Releasable> warmTask) {
            SharedBlobCacheWarmingService.this.scheduleWarmingTask(warmTask);
            tasksCount.incrementAndGet();
        }

        protected class WarmBlobLocationTask implements ActionListener<Releasable> {

            private final BlobLocation blobLocation;
            private final BlobFile blobFile;
            private final ActionListener<Void> listener;

            WarmBlobLocationTask(BlobLocation blobLocation, ActionListener<Void> listener) {
                this.blobLocation = Objects.requireNonNull(blobLocation);
                this.blobFile = blobLocation.blobFile();
                this.listener = listener;
                logger.trace("{} {}: scheduled {}", warmingRun.shardId(), warmingRun.type(), blobLocation);
            }

            @Override
            public void onResponse(Releasable releasable) {
                var cacheKey = new FileCacheKey(warmingRun.shardId(), blobFile.primaryTerm(), blobFile.blobName());
                int endingRegion = cacheService.getEndingRegion(blobLocation.fileLength());

                // TODO: Evaluate reducing to fewer fetches in the future. For example, reading multiple fetches in a single read.
                try (RefCountingListener ref = new RefCountingListener(ActionListener.releaseAfter(listener, releasable))) {
                    for (int i = 0; i <= endingRegion; i++) {
                        long offset = (long) i * cacheService.getRegionSize();
                        cacheService.maybeFetchRegion(
                            cacheKey,
                            i,
                            cacheService.getRegionSize(),
                            new LazyRangeMissingHandler<>(
                                () -> new SequentialRangeMissingHandler(
                                    WarmBlobLocationTask.this,
                                    cacheKey.fileName(),
                                    ByteRange.of(offset, offset + cacheService.getRegionSize()),
                                    directory.getCacheBlobReaderForWarming(blobFile),
                                    () -> writeBuffer.get().clear(),
                                    totalBytesCopied::addAndGet,
                                    StatelessPlugin.PREWARM_THREAD_POOL
                                )
                            ),
                            fetchExecutor,
                            ref.acquire().map(b -> null)
                        );
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> format("%s %s failed to warm blob %s", warmingRun.shardId(), warmingRun.type(), blobLocation), e);
            }

            @Override
            public String toString() {
                return "WarmBlobLocationTask{blobLocation=" + blobLocation + "}";
            }
        }

        /**
         * Warms in cache all pending file locations of a given blob region.
         */
        protected class WarmingTask implements ActionListener<Releasable> {

            private final BlobRangesQueue queue;
            private final BlobRegion blobRegion;

            WarmingTask(BlobRangesQueue queue) {
                this.queue = Objects.requireNonNull(queue);
                this.blobRegion = queue.blobRegion;
                logger.trace("{} {}: scheduled {}", warmingRun.shardId(), warmingRun.type(), blobRegion);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try (RefCountingRunnable refs = new RefCountingRunnable(() -> Releasables.close(releasable))) {
                    var cacheKey = new FileCacheKey(warmingRun.shardId(), blobRegion.blob.primaryTerm(), blobRegion.blob.blobName());

                    var remaining = queue.counter.get();
                    assert 0 < remaining : remaining;

                    while (0 < remaining) {
                        for (int i = 0; i < remaining; i++) {
                            var item = queue.queue.poll();
                            assert item != null;

                            if (isCancelled()) {
                                item.listener().onResponse(null);
                                continue;
                            }

                            var blobLocation = item.blobLocation();
                            var cacheBlobReader = directory.getCacheBlobReaderForWarming(blobLocation.blobFile());
                            var itemListener = ActionListener.releaseAfter(item.listener(), Releasables.assertOnce(refs.acquire()));
                            maybeFetchBlobRange(item, cacheBlobReader, cacheKey, itemListener.delegateResponse((l, e) -> {
                                if (ExceptionsHelper.unwrap(e, ResourceAlreadyUploadedException.class) != null) {
                                    logger.debug(() -> "retrying " + blobLocation + " from object store", e);
                                    // retrying runs on {@link StatelessPlugin#FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL}
                                    // threads, but that is OK because fetchRange doesn't block or wait for anything.
                                    maybeFetchBlobRange(item, cacheBlobReader, cacheKey, l);
                                } else {
                                    l.onFailure(e);
                                }
                            }));
                        }

                        remaining = queue.counter.addAndGet(-remaining);
                        assert 0 <= remaining : remaining;
                    }
                }
            }

            private void maybeFetchBlobRange(
                BlobRange item,
                CacheBlobReader cacheBlobReader,
                FileCacheKey cacheKey,
                ActionListener<Void> listener
            ) {
                ActionListener.run(listener, (l) -> {
                    // compute the range to warm in cache
                    var range = maybeMinimizeRange(
                        cacheBlobReader.getRange(
                            item.position(),
                            Math.toIntExact(item.length()),
                            queue.maxBlobLength.get() - item.position()
                        ),
                        item
                    );

                    cacheService.maybeFetchRange(
                        cacheKey,
                        blobRegion.region,
                        range,
                        // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService
                        // to fully utilize each region. So we just pass it with a value that cover the current region.
                        (long) (blobRegion.region + 1) * cacheService.getRegionSize(),
                        // Can be executed on different thread pool depending whether we read from
                        // the SharedBlobCacheWarmingService (PREWARM_THREAD_POOL pool) or the IndexingShardCacheBlobReader (VBCC pool)
                        new SequentialRangeMissingHandler(
                            WarmingTask.this,
                            cacheKey.fileName(),
                            range,
                            cacheBlobReader,
                            () -> writeBuffer.get().clear(),
                            totalBytesCopied::addAndGet,
                            StatelessPlugin.PREWARM_THREAD_POOL,
                            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                        ),
                        fetchExecutor,
                        l.map(ignored -> null)
                    );
                });
            }

            private ByteRange maybeMinimizeRange(ByteRange range, BlobRange item) {
                // Step is equal to range size, effectively disable the step-sized prewarming
                if (prewarmingRangeMinimizationStep == cacheService.getRangeSize()) {
                    return range;
                }
                // This is a hack to minimize the amount of data we pre-warm
                if (range.length() != cacheService.getRangeSize()) {
                    // only change cache ranges when reading from the blob store
                    return range;
                }
                if (blobRegion.region == 0) {
                    // keep existing range as region 0 contains mostly metadata
                    return range;
                }
                // The rounding depends on the rangeSize to be integer multiples of the stepSize which is guaranteed in setting validation
                final long minimizedEnd = BlobCacheUtils.roundUpToAlignedSize(item.position + item.length, prewarmingRangeMinimizationStep);
                if (minimizedEnd < range.end()) {
                    assert assertCorrectMinimizedEnd(range, item, minimizedEnd);
                    return ByteRange.of(range.start(), minimizedEnd);
                } else {
                    return range;
                }
            }

            private boolean assertCorrectMinimizedEnd(ByteRange range, BlobRange item, long minimizedEnd) {
                assert minimizedEnd < range.end() : minimizedEnd + " >= " + range.end();
                assert minimizedEnd >= range.start() : minimizedEnd + "<" + range.start();
                assert minimizedEnd >= item.position + item.length : minimizedEnd + "<" + item.position + item.length;
                assert (minimizedEnd - range.start()) % prewarmingRangeMinimizationStep == 0
                    : minimizedEnd + "-" + range.start() + " vs " + prewarmingRangeMinimizationStep;
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> format("%s %s failed to warm region %s", warmingRun.shardId(), warmingRun.type(), blobRegion), e);
            }

            @Override
            public String toString() {
                return "WarmingTask{region=" + blobRegion + "}";
            }
        }

        // protected for tests
        protected class WarmBlobByteRangeTask implements ActionListener<Releasable> {
            // protected for tests
            protected final BlobFile blobFile;
            // protected for tests
            protected final ByteRange byteRangeToWarm;
            private final ActionListener<Void> listener;

            WarmBlobByteRangeTask(BlobFile blobFile, ByteRange byteRangeToWarm, ActionListener<Void> listener) {
                this.blobFile = Objects.requireNonNull(blobFile);
                this.byteRangeToWarm = byteRangeToWarm;
                this.listener = listener;
                logger.trace("{} {}: scheduled {} {}", warmingRun.shardId(), warmingRun.type(), blobFile, byteRangeToWarm);
            }

            @Override
            public void onResponse(Releasable releasable) {
                var cacheKey = new FileCacheKey(warmingRun.shardId(), blobFile.primaryTerm(), blobFile.blobName());
                var releasedListener = ActionListener.releaseAfter(listener, releasable);
                var cacheBlobReader = directory.getCacheBlobReaderForWarming(blobFile);
                fetchRange(cacheKey, cacheBlobReader, releasedListener.delegateResponse((l, e) -> {
                    if (ExceptionsHelper.unwrap(e, ResourceAlreadyUploadedException.class) != null) {
                        logger.debug(() -> "retrying " + blobFile + " " + byteRangeToWarm + " from object store", e);
                        // retrying runs on {@link StatelessPlugin#FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL}
                        // threads, but that is OK because fetchRange doesn't block or wait for anything.
                        fetchRange(cacheKey, cacheBlobReader, l);
                    } else {
                        l.onFailure(e);
                    }
                }));
            }

            private void fetchRange(FileCacheKey cacheKey, CacheBlobReader cacheBlobReader, ActionListener<Void> l) {
                cacheService.fetchRange(
                    cacheKey,
                    byteRangeToWarm,
                    cacheBlobReader,
                    WarmBlobByteRangeTask.this,
                    writeBuffer::get,
                    totalBytesCopied::addAndGet,
                    fetchExecutor,
                    true,
                    l
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> format("%s %s failed to warm blob %s %s", warmingRun.shardId(), warmingRun.type(), blobFile, byteRangeToWarm),
                    e
                );
            }
        }

        protected boolean isCancelled() {
            return isStoreClosing.get();
        }
    }
}
