/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.cache;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

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
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
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
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.metering.RecoveryMetricsCollector;
import org.elasticsearch.xpack.stateless.utils.IndexingShardRecoveryComparator;

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
        INDEXING_EARLY,
        INDEXING,
        INDEXING_MERGE,
        SEARCH,
        HOLLOWING,
        UNHOLLOWING
    }

    public static final String BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC = "es.blob_cache_warming.page_aligned_bytes.total";

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

    public static final String SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME = "stateless.search.offline_warming";
    public static final Setting<Boolean> SEARCH_OFFLINE_WARMING_ENABLED_SETTING = Setting.boolSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING = Setting.boolSetting(
        SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".prefetch_commits.enabled",
        true,
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
    private final long prewarmingRangeMinimizationStep;
    private volatile boolean prefetchCommitsForSearchShardRecovery;
    private volatile int minSearchPower;
    private volatile boolean searchOfflineWarmingEnabled;
    private volatile TimeValue boostWindow;

    public SharedBlobCacheWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        ClusterSettings clusterSettings
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
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
        this.prewarmingRangeMinimizationStep = clusterSettings.get(PREWARMING_RANGE_MINIMIZATION_STEP).getBytes();
        clusterSettings.initializeAndWatch(
            SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING,
            value -> this.prefetchCommitsForSearchShardRecovery = value
        );
        clusterSettings.initializeAndWatch(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING, value -> this.minSearchPower = value);
        clusterSettings.initializeAndWatch(ServerlessSharedSettings.BOOST_WINDOW_SETTING, value -> this.boostWindow = value);
        clusterSettings.initializeAndWatch(SEARCH_OFFLINE_WARMING_ENABLED_SETTING, value -> this.searchOfflineWarmingEnabled = value);
    }

    public void warmCacheBeforeUpload(VirtualBatchedCompoundCommit vbcc, ActionListener<Void> listener) {
        assert vbcc.isFrozen();
        long totalSizeInBytes = vbcc.getTotalSizeInBytes();
        cacheService.maybeFetchRegion(
            new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
            0,
            // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService to
            // fully utilize each region. So we just pass it with a value that cover the current region.
            totalSizeInBytes,
            (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> ActionListener.completeWith(
                completionListener,
                () -> {
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

                        // We don't override the flush method as we only want to do cache aligned flushes - when the buffer is full or on
                        // close.
                        private void doFlush(boolean closeFlush) throws IOException {
                            int position = byteBuffer.position();
                            var bytesCopied = SharedBytes.copyBufferToCacheFileAligned(channel, bytesFlushed + channelPos, byteBuffer);
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
                        vbcc.getBytesByRange(relativePos, Math.toIntExact(Math.min(len, totalSizeInBytes)), output);
                        return null;
                    }
                }
            ),
            uploadPrewarmFetchExecutor,
            listener.map(b -> null)
        );
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
     * Warms the cache to optimize cache hits during the recovery of an indexing or search shard. The warming happens concurrently
     * with the recovery and doesn't block it.
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
     * @param regionsToWarm up-to region idx to warm, in addition to regular warming, grouped by {@code BlobFile} (if known/computed)
     */
    public void warmCacheForShardRecovery(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Integer> regionsToWarm
    ) {
        warmCacheRecovery(type, indexShard, commit, directory, regionsToWarm, ActionListener.noop());
    }

    protected void warmCacheRecovery(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Integer> regionsToWarm,
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
                    SubscribableListener.<Map<BlobFile, Integer>>newForked(l1 -> {
                        if (regionsToWarm == null) {
                            Map<BlobFile, Integer> regionsToWarmComputed = ConcurrentCollections.newConcurrentMap();
                            ObjectStoreService.readReferencedCompoundCommitsUsingCache(
                                commit,
                                // do not pass in any previously read BCC, because we actually want to ensure that the
                                // referenced CCs (headers) in this BCC are also populated in the cache
                                null,
                                directory,
                                BlobCacheIndexInput.WARMING,
                                // cannot run on the {@link PREWARM_THREAD_POOL} because this triggers AND waits for cache population,
                                // which itself runs on the {@link PREWARM_THREAD_POOL}, potentially triggering a deadlock
                                throttledTaskRunner.asExecutor(),
                                referencedCompoundCommit -> {
                                    regionsToWarmComputed.compute(
                                        referencedCompoundCommit.statelessCompoundCommitReference().bccBlobFile(),
                                        (blobFile, maxRegionToWarm) -> {
                                            var regionIdx = regionsToWarmForCC(referencedCompoundCommit);
                                            return maxRegionToWarm == null ? regionIdx : Math.max(maxRegionToWarm, regionIdx);
                                        }
                                    );
                                },
                                l1.map(aVoid -> regionsToWarmComputed)
                            );
                        } else {
                            l1.onResponse(regionsToWarm);
                        }
                    }).<Void>andThen((l2, regionsToWarmFinal) -> {
                        if (searchOfflineWarmingEnabled) {
                            warmRegions(indexShard, regionsToWarmFinal, l2);
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
                try (
                    // warming up the latest commit upon recovery will fetch a few regions of every active
                    // segment (the first region of every segment is always fetched)
                    var warmer = new RecoveryWarmer(
                        warmingRun,
                        indexShard,
                        store::isClosing,
                        commit.commitFiles(),
                        directory,
                        listeners.acquire()
                    )
                ) {
                    warmer.run();
                }
            }
        }
    }

    public int regionsToWarmForCC(ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCC) {
        final double warmingRatio = calculateWarmingRatioFromCompoundCommit(referencedCC, threadPool.absoluteTimeInMillis());
        assert warmingRatio >= 0.0;
        if (warmingRatio <= 0) {
            return 0;
        } else {
            final long startPosition = referencedCC.statelessCompoundCommitReference().headerOffsetInTheBccBlobFile();
            final long endPosition = startPosition + Math.round(
                referencedCC.statelessCompoundCommitReference().compoundCommit().sizeInBytes() * warmingRatio
            );
            int endRegion = cacheService.getEndingRegion(endPosition);
            final long positionInEndRegion = endPosition % cacheService.getRegionSize();
            if (endRegion > 0 && positionInEndRegion > 0 && positionInEndRegion <= cacheService.getRegionSize() / 2) {
                // take the floor region if the end position is <= half of the region size
                endRegion = Math.max(endRegion - 1, cacheService.getRegion(startPosition));
            }
            return endRegion;
        }
    }

    private void warmRegions(IndexShard indexShard, Map<BlobFile, Integer> regionsToWarmPerBlobFile, ActionListener<Void> listener) {
        try (RefCountingListener listeners = new RefCountingListener(listener)) {
            for (var regionsToWarm : regionsToWarmPerBlobFile.entrySet()) {
                if (regionsToWarm.getValue() > 0) {
                    warmBlobRegions(Type.SEARCH, indexShard, regionsToWarm.getKey(), regionsToWarm.getValue(), listeners.acquire());
                }
            }
        }
    }

    private void warmBlobRegions(
        Type type,
        IndexShard indexShard,
        BlobFile blobFile,
        int endRegionToWarmTo,
        ActionListener<Void> listener
    ) {
        final Store store = indexShard.store();
        final ShardId shardId = indexShard.shardId();
        final var warmingRun = new WarmingRun(type, shardId, "prewarm", Map.of("prewarming_type", type.name()));

        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache [" + type + "] for " + shardId + ", store is closing"));
        } else {
            try (
                var warmer = new BlobRegionWarmer(
                    warmingRun,
                    blobFile,
                    endRegionToWarmTo,
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
        final var timestampFieldValueRange = referencedCompoundCommit.statelessCompoundCommitReference()
            .compoundCommit()
            .getTimestampFieldValueRange();
        long commitMillis;
        if (timestampFieldValueRange != null) {
            commitMillis = timestampFieldValueRange.minMillis() + (timestampFieldValueRange.maxMillis() - timestampFieldValueRange
                .minMillis()) / 2;
        } else {
            // if we don't have a timestamp range for the compound commit, use a timestamp in the middle of the boost window
            commitMillis = nowMillis - boostWindow.getMillis() / 2;
        }

        return calculateWarmingRatio(nowMillis, commitMillis, boostWindow.getMillis(), minSearchPower);
    }

    protected static double calculateWarmingRatio(long nowMillis, long timestampMillis, long boostWindowMillis, int searchPower) {
        final long delta = Math.max(timestampMillis - (nowMillis - boostWindowMillis), 0);
        final double ratio = ((double) delta / boostWindowMillis) * searchPower;
        return Math.min(ratio, 100) / 100;
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

    private class RecoveryWarmer extends AbstractWarmer {

        private final ConcurrentMap<BlobRegion, BlobRangesQueue> queues = new ConcurrentHashMap<>();
        private final IndexShard indexShard;
        private final Map<String, BlobLocation> filesToWarm;
        private final int segmentCount;
        protected final AtomicLong skippedTasksCount = new AtomicLong(0L);

        RecoveryWarmer(
            WarmingRun warmingRun,
            IndexShard indexShard,
            Supplier<Boolean> isStoreClosing,
            Map<String, BlobLocation> filesToWarm,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            super(warmingRun, isStoreClosing, directory, listener);
            this.indexShard = indexShard;
            this.filesToWarm = Collections.unmodifiableMap(filesToWarm);
            this.segmentCount = segmentCount(filesToWarm);
        }

        void run() {
            filesToWarm.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()))
                .forEach(entry -> addFile(entry.getKey(), LuceneFilesExtensions.fromFile(entry.getKey()), entry.getValue()));
        }

        @Override
        protected boolean isCancelled() {
            return super.isCancelled() || indexShard.state() != IndexShardState.RECOVERING;
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
                // stop warming if the shard is not recovering anymore
            } else if (fileExtension == LuceneFilesExtensions.CFE) {
                SubscribableListener
                    // warm entire CFE file
                    .<Void>newForked(listener -> addLocation(blobLocation, fileName, listener))
                    // parse it and schedule warming of corresponding parts of CFS file
                    .andThenAccept(ignored -> addCfe(fileName))
                    .addListener(listeners.acquire());
            } else if (shouldFullyWarmUp(fileName, fileExtension) || blobLocation.fileLength() <= BUFFER_SIZE) {
                // warm entire file when it is small
                addLocation(blobLocation, fileName, listeners.acquire());
            } else {
                // header
                addLocation(blobLocation, fileName, blobLocation.offset(), BUFFER_SIZE, listeners.acquire());
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

        private void addLocation(BlobLocation location, String fileName, ActionListener<Void> listener) {
            addLocation(location, fileName, location.offset(), location.fileLength(), listener);
        }

        private void addLocation(BlobLocation location, String fileName, long position, long length, ActionListener<Void> listener) {
            final long start = position;
            final long end = position + length;
            final int regionSize = cacheService.getRegionSize();
            final int startRegion = cacheService.getRegion(start);
            final int endRegion = cacheService.getEndingRegion(end);

            if (startRegion == endRegion) {
                BlobRegion blobRegion = new BlobRegion(location.blobFile(), startRegion);
                enqueueLocation(blobRegion, fileName, location, position, length, listener);
            } else {
                try (var listeners = new RefCountingListener(listener)) {
                    for (int r = startRegion; r <= endRegion; r++) {
                        // adjust the position & length to the region
                        var range = ByteRange.of(Math.max(start, (long) r * regionSize), Math.min(end, (r + 1L) * regionSize));
                        BlobRegion blobRegion = new BlobRegion(location.blobFile(), r);
                        enqueueLocation(blobRegion, fileName, location, range.start(), range.length(), listeners.acquire());
                    }
                }
            }
        }

        private void addCfe(String fileName) {
            assert LuceneFilesExtensions.fromFile(fileName) == LuceneFilesExtensions.CFE : fileName;
            // We spawn to the generic pool here (via a throttled task runner), so that we have the following invocation path across
            // the thread pools: GENERIC (recovery) -> FILL_VBCC_THREAD_POOL (if fetching from indexing node) -> GENERIC.
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
                            .sorted(Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()))
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

        private boolean canSkipLocation(String fileName, long position, long length) {
            if (warmingRun.type != Type.INDEXING
                && warmingRun.type != Type.INDEXING_EARLY
                && warmingRun.type != Type.HOLLOWING
                && warmingRun.type != Type.UNHOLLOWING) {
                return false;
            }
            if (length > Short.MAX_VALUE) {
                // length is too long to be contained in replicated section
                return false;
            }
            assert directory instanceof IndexBlobStoreCacheDirectory : directory.getClass() + " is not an IndexBlobStoreCacheDirectory";
            var dir = (IndexBlobStoreCacheDirectory) directory;
            int region = (int) (dir.getPosition(fileName, position, (int) length) / cacheService.getRegionSize());
            // region 0 is already loaded by this point while resolving full set of commit files and safe to skip.
            // See org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService#readIndexingShardState
            return region == 0;
        }

        private void enqueueLocation(
            BlobRegion blobRegion,
            String fileName,
            BlobLocation blobLocation,
            long position,
            long length,
            ActionListener<Void> listener
        ) {
            if (canSkipLocation(fileName, position, length)) {
                skippedTasksCount.incrementAndGet();
                listener.onResponse(null);
                return;
            }

            var blobRanges = queues.computeIfAbsent(blobRegion, BlobRangesQueue::new);
            if (blobRanges.add(blobLocation, position, length, listener)) {
                scheduleWarmingTask(new WarmingTask(blobRanges));
            }
        }

        private static boolean shouldFullyWarmUp(String fileName, @Nullable LuceneFilesExtensions fileExtension) {
            return fileExtension == null // segments_N are fully warmed up in cache
                || fileExtension.isMetadata() // metadata files
                || StatelessCompoundCommit.isGenerationalFile(fileName); // generational files
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

    private class BlobRegionWarmer extends SharedBlobCacheWarmingService.AbstractWarmer {
        private final BlobFile blobFile;
        private final int endRegionToWarmTo;

        BlobRegionWarmer(
            SharedBlobCacheWarmingService.WarmingRun warmingRun,
            BlobFile blobFile,
            int endRegionToWarmTo,
            Supplier<Boolean> isStoreClosing,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            super(warmingRun, isStoreClosing, directory, listener);
            this.blobFile = blobFile;
            this.endRegionToWarmTo = endRegionToWarmTo;
        }

        void run() {
            scheduleWarmingTask(new WarmBlobRegionsTask(blobFile, endRegionToWarmTo, listeners.acquire()));
        }

        @Override
        protected void onWarmingSuccess(long duration) {
            logger.log(
                duration >= 5000 ? Level.INFO : Level.DEBUG,
                "{} {} warming completed in {} ms ({} regions, {} tasks, {} bytes)",
                warmingRun.shardId(),
                warmingRun.type(),
                duration,
                endRegionToWarmTo,
                tasksCount.get(),
                totalBytesCopied.get()
            );
        }
    }

    private abstract class AbstractWarmer implements Releasable {

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
            throttledTaskRunner.enqueueTask(warmTask);
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

        protected class WarmBlobRegionsTask implements ActionListener<Releasable> {
            private final BlobFile blobFile;
            private final int endRegion;
            private final ActionListener<Void> listener;

            WarmBlobRegionsTask(BlobFile blobFile, int endRegion, ActionListener<Void> listener) {
                this.blobFile = Objects.requireNonNull(blobFile);
                this.endRegion = endRegion;
                this.listener = listener;
                logger.trace("{} {}: scheduled {}", warmingRun.shardId(), warmingRun.type(), blobFile);
            }

            @Override
            public void onResponse(Releasable releasable) {
                final var cacheKey = new FileCacheKey(warmingRun.shardId(), blobFile.primaryTerm(), blobFile.blobName());

                // TODO: Evaluate reducing to fewer fetches in the future. For example, reading multiple fetches in a single read.
                try (RefCountingListener ref = new RefCountingListener(ActionListener.releaseAfter(listener, releasable))) {
                    for (int region = 0; region <= endRegion; region++) {
                        final long offset = (long) region * cacheService.getRegionSize();
                        cacheService.fetchRegion(
                            cacheKey,
                            region,
                            cacheService.getRegionSize(),
                            new LazyRangeMissingHandler<>(
                                () -> new SequentialRangeMissingHandler(
                                    WarmBlobRegionsTask.this,
                                    cacheKey.fileName(),
                                    ByteRange.of(offset, offset + cacheService.getRegionSize()),
                                    directory.getCacheBlobReaderForWarming(blobFile),
                                    () -> writeBuffer.get().clear(),
                                    totalBytesCopied::addAndGet,
                                    StatelessPlugin.PREWARM_THREAD_POOL,
                                    StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                                )
                            ),
                            fetchExecutor,
                            // force is enabled during warming to ensure we always warm the specified region
                            true,
                            ref.acquire().map(b -> null)
                        );
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> format("%s %s failed to warm blob %s", warmingRun.shardId(), warmingRun.type(), blobFile), e);
            }
        }

        protected boolean isCancelled() {
            return isStoreClosing.get();
        }
    }
}
