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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.SequentialRangeMissingHandler;
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.engine.NewCommitNotification;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.stateless.Stateless.PREWARM_THREAD_POOL;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

/**
 * Prefetches segment files from {@link co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit}
 * to improve search performance.
 * <p>
 * Optimizes search latency by proactively fetching commit data into the shared blob cache
 * before it's needed. Tracks prefetched data to avoid redundant blob store calls, but in
 * most cases it is expected that the data would fit in the cache (dependent on the configured
 * search power).
 * </p>
 *
 * <p>
 * Triggered by new commit notifications to determine what data ranges need fetching based on
 * current pre-fetch state  {@link BCCPreFetchedOffset}.
 * </p>
 *
 */
public class SearchCommitPrefetcher {
    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    private static final boolean SEARCH_PREFETCHING_FEATURE_FLAG = new FeatureFlag("search_prefetching_enabled").isEnabled();

    private static final TimeValue DEFAULT_SEARCH_IDLE_TIME = TimeValue.timeValueMinutes(5);
    public static final Setting<Boolean> PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.prefetch_commits.enabled",
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> BACKGROUND_PREFETCH_ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.prefetch_commits.background_prefetch",
        true,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> PREFETCH_NON_UPLOADED_COMMITS_SETTING = Setting.boolSetting(
        "stateless.search.prefetch_commits.prefetch_non_uploaded_commits",
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> PREFETCH_SEARCH_IDLE_TIME_SETTING = Setting.timeSetting(
        "stateless.search.prefetch_commits.search_idle_time",
        DEFAULT_SEARCH_IDLE_TIME,
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING = Setting.byteSizeSetting(
        "stateless.search.prefetch_commits.request_size_limit_index_node",
        SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> FORCE_PREFETCH_SETTING = Setting.boolSetting(
        "stateless.search.prefetch_commits.force",
        false,
        Setting.Property.NodeScope
    );
    // TODO: add a setting to enable pre-fetching only for write indices?

    private final Logger logger = LogManager.getLogger(SearchCommitPrefetcher.class);

    private final ShardId shardId;
    private final StatelessSharedBlobCacheService cacheService;
    private final CacheBlobReaderSupplier cacheBlobReaderSupplier;
    private final Executor executor;
    private final ThreadPool threadPool;
    private final boolean prefetchEnabled;
    private final boolean prefetchInBackground;
    private final boolean prefetchNonUploadedCommits;
    private final long prefetchSearchIdleTimeInMillis;
    private final long maxBytesToFetchFromIndexingNode;
    private final boolean forcePrefetch;
    private final AtomicLong totalPrefetchedBytes = new AtomicLong();

    private final AtomicReference<BCCPreFetchedOffset> maxPrefetchedOffset = new AtomicReference<>(BCCPreFetchedOffset.ZERO);

    public SearchCommitPrefetcher(
        ShardId shardId,
        StatelessSharedBlobCacheService cacheService,
        CacheBlobReaderSupplier cacheBlobReaderSupplier,
        ThreadPool threadPool,
        Executor executor,
        ClusterSettings clusterSettings
    ) {
        this.shardId = shardId;
        this.cacheService = cacheService;
        this.cacheBlobReaderSupplier = cacheBlobReaderSupplier;
        this.executor = executor;
        this.threadPool = threadPool;
        this.prefetchEnabled = clusterSettings.get(PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING);
        this.prefetchInBackground = clusterSettings.get(BACKGROUND_PREFETCH_ENABLED_SETTING);
        this.prefetchNonUploadedCommits = clusterSettings.get(PREFETCH_NON_UPLOADED_COMMITS_SETTING);
        this.prefetchSearchIdleTimeInMillis = clusterSettings.get(PREFETCH_SEARCH_IDLE_TIME_SETTING).millis();
        this.maxBytesToFetchFromIndexingNode = clusterSettings.get(PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING).getBytes();
        this.forcePrefetch = clusterSettings.get(FORCE_PREFETCH_SETTING);
    }

    public void maybePrefetchLatestCommit(
        NewCommitNotification notification,
        long timeSinceLastSearcherWasAcquiredInMillis,
        ActionListener<Void> listener
    ) {
        if (prefetchEnabled == false
            // We only want to prefetch commits for shards actively serving searches so skip
            // if no searchers have been acquired for a while.
            || timeSinceLastSearcherWasAcquiredInMillis > prefetchSearchIdleTimeInMillis) {
            logger.debug(
                "{} Skipping prefetch commit notification enabled=[{}], "
                    + "timeSinceLastSearcherWasAcquiredInMillis=[{}], "
                    + "prefetchNonUploadedCommits=[{}], "
                    + "notification=[{}]",
                shardId,
                prefetchEnabled,
                timeSinceLastSearcherWasAcquiredInMillis,
                prefetchNonUploadedCommits,
                notification
            );
            listener.onResponse(null);
            return;
        }

        // This gets executed in a transport thread and if the cache needs to evict regions it acquires a lock,
        // just to be on the safe side, we dispatch it into a different thread pool.
        threadPool.generic().execute(ActionRunnable.wrap(listener, l -> {
            // If we prefetch in the background the new commit notification would go through and the refresh
            // will be executed concurrently with the prefetching. Otherwise, the refresh won't be executed
            // until the prefetching finishes.
            ActionListener<Void> prefetchListener = prefetchInBackground ? ActionListener.noop() : l;
            prefetchLatestCommit(notification, prefetchListener);

            if (prefetchInBackground) {
                l.onResponse(null);
            }
        }));
    }

    private void prefetchLatestCommit(NewCommitNotification notification, ActionListener<Void> listener) {
        var prefetchedBytes = new AtomicLong(0);
        final var currentMaxPrefetchedOffset = maxPrefetchedOffset.get();
        var prefetchingStarted = threadPool.relativeTimeInMillis();

        ActionListener<Void> prefetchListener = ActionListener.runBefore(listener, () -> {
            // TODO: add APM metrics?
            totalPrefetchedBytes.addAndGet(prefetchedBytes.get());
            logger.debug(
                "{} Prefetching for new commit missing blobs [{}] took [{}] and fetched [{} bytes] {}",
                shardId,
                notification,
                threadPool.relativeTimeInMillis() - prefetchingStarted,
                ByteSizeValue.ofBytes(prefetchedBytes.get()).getStringRep(),
                totalPrefetchedBytes.get()
            );
        });

        try (RefCountingListener refCountingListener = new RefCountingListener(prefetchListener)) {
            var compoundCommit = notification.compoundCommit();

            // New commits might be served from the indexing nodes, in such cases it might
            // be undesirable to fetch the entire commit from the indexing node as it adds
            // extra load to the indexing nodes.
            if (prefetchNonUploadedCommits == false && notification.latestUploadedBatchedCompoundCommitTermAndGen() == null) {
                return;
            }

            // New commit notifications can arrive out of order.
            // When prefetchNonUploadedCommits is false, we still want to trigger
            // prefetching for the latest uploaded BCC as soon as we receive any notification.
            // This ensures timely updates, even if notifications are out of order.
            // The underlying cache handles deduplication, so redundant prefetch requests are safe.
            var maxBCCGenerationToPrefetch = prefetchNonUploadedCommits
                ? notification.batchedCompoundCommitGeneration()
                : notification.latestUploadedBatchedCompoundCommitTermAndGen().generation();

            Map<BlobFile, ByteRange> bccRangesToPrefetch = getPendingRangesToPrefetch(
                currentMaxPrefetchedOffset,
                maxBCCGenerationToPrefetch,
                compoundCommit.commitFiles().values()
            );

            logger.debug("[{}] Missing ranges [{}] for new commit [{}]", shardId, bccRangesToPrefetch, notification);

            for (Map.Entry<BlobFile, ByteRange> bccRangeToPrefetch : bccRangesToPrefetch.entrySet()) {
                var blobFile = bccRangeToPrefetch.getKey();
                var rangeToPrefetch = bccRangeToPrefetch.getValue();

                // Skip pre-fetching from indexing nodes when requests exceed one region size.
                //
                // The TransportGetVirtualBatchedCompoundCommitChunkAction on indexing nodes serves
                // byte range requests without data size limits. This works fine for indexing nodes
                // because they use a fixed 4-thread pool (GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL),
                // capping memory usage at 4 * region_size.
                //
                // However, search nodes handle responses from TransportGetVirtualBatchedCompoundCommitChunkAction
                // via a scaling executor FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL.
                // When many concurrent pre-fetches occur while disk writes are slow, responses accumulate in memory and
                // can cause OOM errors on search nodes. This is not a problem for the regular read
                // path in the cache since in that case we fetch up to 128k bytes.
                long totalDataToPrefetchInBytes = rangeToPrefetch.length();
                if (blobFile.generation() == notification.batchedCompoundCommitGeneration()
                    && notification.isBatchedCompoundCommitUploaded() == false
                    && totalDataToPrefetchInBytes > maxBytesToFetchFromIndexingNode) {
                    logger.trace(
                        "Skipping prefetch commit [{}] missing ranges [{}] with size [{}] which is over the limit [{}]",
                        compoundCommit,
                        rangeToPrefetch,
                        totalDataToPrefetchInBytes,
                        maxBytesToFetchFromIndexingNode
                    );
                    continue;
                }

                var cacheKey = new FileCacheKey(shardId, blobFile.primaryTerm(), blobFile.blobName());

                // BlobLocation instances usually represents a single Lucene file embedded in a BlobFile.
                // In this case we are fetching a range that might contain multiple Lucene files and the
                // CacheBlobReader API expects a BlobLocation.
                var blobLocation = new BlobLocation(blobFile, 0, totalDataToPrefetchInBytes);
                var cacheBlobReader = cacheBlobReaderSupplier.getCacheBlobReaderForPreFetching(cacheKey.fileName(), blobLocation);

                // If fetching a range from the indexing node, adjust it for padding and alignment.
                // If fetching from the blob store, we try to fetch the entire region.
                var adjustedRangeToPrefetch = cacheBlobReader.getRange(
                    rangeToPrefetch.start(),
                    Math.toIntExact(totalDataToPrefetchInBytes),
                    totalDataToPrefetchInBytes
                );

                var startRegion = cacheService.getRegion(adjustedRangeToPrefetch.start());
                var endRegion = cacheService.getEndingRegion(adjustedRangeToPrefetch.end());
                for (int region = startRegion; region <= endRegion; region++) {
                    long maxPrefetchedOffsetInRegion = Math.min(cacheService.getRegionEnd(region), adjustedRangeToPrefetch.end());
                    // TODO: Implement force version that decays entries from level 1 if there's no room in the cache

                    // We cannot simply fetch the region directly because pre-fetching from index nodes introduces specific edge cases:
                    // 1. If the missing gaps are within or equal to the current VBCC size:
                    // - The get VBCC chunk action would return data up to the current VBCC length.
                    // - The tracker would incorrectly assume the region is fully populated.
                    // - If new commits are appended to the VBCC afterward, this assumption becomes invalid.
                    // 2. If the missing gaps extend beyond the current VBCC size:
                    // - The get VBCC chunk action would fail, as the request exceeds the current VBCC boundary.
                    cacheService.fetchRange(
                        cacheKey,
                        region,
                        adjustedRangeToPrefetch,
                        // This could grow over time (i.e. if we prefetch commits from a non-uploaded BCC),
                        // but the cache would size the region to the default region size anyway.
                        totalDataToPrefetchInBytes,
                        new SequentialRangeMissingHandler(
                            this,
                            cacheKey.fileName(),
                            adjustedRangeToPrefetch,
                            cacheBlobReader,
                            () -> writeBuffer.get().clear(),
                            prefetchedBytes::addAndGet,
                            // I/O for pre-warming uses DIRECT_EXECUTOR, meaning the pre-warm thread pool
                            // itself is responsible for fetching data from the blob store.
                            Stateless.PREWARM_THREAD_POOL,
                            // If the data is pre-fetched from the indexing node (for non-uploaded BCCs),
                            // these reads would be executed in the fill VBCC thread poll.
                            Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                        ),
                        executor,
                        forcePrefetch,
                        refCountingListener.acquire().map(populated -> {
                            if (populated) {
                                var offsetAfterPopulation = maxPrefetchedOffset.accumulateAndGet(
                                    new BCCPreFetchedOffset(blobFile.termAndGeneration(), maxPrefetchedOffsetInRegion),
                                    (original, candidate) -> original.compareTo(candidate) < 0 ? candidate : original
                                );
                                assert assertMaxPrefetchedOffsetMovesForward(currentMaxPrefetchedOffset, offsetAfterPopulation)
                                    : "maxPrefetchedOffset should move forward after a successful prefetching but it didn't: "
                                        + currentMaxPrefetchedOffset
                                        + " vs "
                                        + offsetAfterPopulation;
                            }
                            return null;
                        })
                    );
                }
            }
        }
    }

    private boolean assertMaxPrefetchedOffsetMovesForward(
        BCCPreFetchedOffset offsetBeforeCachePopulation,
        BCCPreFetchedOffset offsetAfterCachePopulation
    ) {
        if (cacheService.getRegionSize() == cacheService.getRangeSize()) {
            return offsetAfterCachePopulation.compareTo(offsetBeforeCachePopulation) > 0;
        }

        // In most cases region_size == range_size but sometimes in tests the
        // range_size > region_size, meaning that we might end up prefetching
        // the same region more than once (i.e. if the region was evicted after
        // being prefetched).
        assert cacheService.getRangeSize() > cacheService.getRegionSize();
        return offsetAfterCachePopulation.compareTo(offsetBeforeCachePopulation) >= 0;
    }

    // visible for testing
    public long getTotalPrefetchedBytes() {
        return totalPrefetchedBytes.get();
    }

    static Map<BlobFile, ByteRange> getPendingRangesToPrefetch(
        BCCPreFetchedOffset currentMaxPrefetchedOffset,
        long maxBCCTermAndGenToPrefetch,
        Collection<BlobLocation> commitFiles
    ) {
        Map<BlobFile, ByteRange> bccRangesToPrefetch = new HashMap<>();
        for (var blobLocation : commitFiles) {
            long blobLocationOffset = blobLocation.offset();
            var bccTermAndGen = blobLocation.getBatchedCompoundCommitTermAndGeneration();
            if (currentMaxPrefetchedOffset.precedesOrAt(bccTermAndGen, blobLocationOffset)
                && bccTermAndGen.generation() <= maxBCCTermAndGenToPrefetch) {
                var blobFile = blobLocation.blobFile();
                bccRangesToPrefetch.compute(blobFile, (unused, range) -> {
                    long blobLocationEnd = blobLocationOffset + blobLocation.fileLength();
                    if (range == null) {
                        boolean isNewBCC = blobLocation.getBatchedCompoundCommitTermAndGeneration()
                            .compareTo(currentMaxPrefetchedOffset.bccTermAndGen()) > 0;
                        var rangeStart = isNewBCC ? 0 : Math.min(currentMaxPrefetchedOffset.offset(), blobLocationOffset);
                        return ByteRange.of(rangeStart, blobLocationEnd);
                    }
                    var extendedRange = ByteRange.of(Math.min(range.start(), blobLocationOffset), Math.max(blobLocationEnd, range.end()));
                    assert range.isSubRangeOf(extendedRange);
                    return extendedRange;
                });
            }
        }
        return bccRangesToPrefetch;
    }

    /**
     * Tracks the maximum offset that has been pre-fetched within a Batched Compound Commit (BCC).
     * <p>
     * We need to track this to avoid re-fetching segment files that have already been prefetched.
     * </p>
     * <p>
     * When a new commit is added to a BCC with a higher generation, it may reference files
     * that haven't been prefetched yet. This record helps determine what additional data
     * needs to be fetched.
     * </p>
     *
     * @param bccTermAndGen the primary term and generation of the BCC
     * @param offset the byte offset indicating how far into the blob has been pre-fetched
     */
    record BCCPreFetchedOffset(PrimaryTermAndGeneration bccTermAndGen, long offset) implements Comparable<BCCPreFetchedOffset> {

        static final BCCPreFetchedOffset ZERO = new BCCPreFetchedOffset(PrimaryTermAndGeneration.ZERO, Long.MIN_VALUE);

        @Override
        public int compareTo(BCCPreFetchedOffset o) {
            var termGenCmp = bccTermAndGen.compareTo(o.bccTermAndGen);
            if (termGenCmp == 0) {
                return Long.compare(offset, o.offset);
            }
            return termGenCmp;
        }

        /**
         * Checks if this pre-fetched offset precedes the given BCC term/generation and offset.
         * This is useful to determine if a Lucene file stored in a determined BCC has been
         * prefetched already.
         *
         * @param bccTermAndGen the BCC term and generation to compare against
         * @param offset the offset to compare against
         * @return true if this offset precedes the given position, indicating more data needs to be fetched
         */
        boolean precedesOrAt(PrimaryTermAndGeneration bccTermAndGen, long offset) {
            var termAndGenCmp = this.bccTermAndGen.compareTo(bccTermAndGen);
            if (termAndGenCmp == 0) {
                return this.offset <= offset;
            }

            // If a new blob is detected (e.g., after recovery), its files should also be pre-fetched.
            return termAndGenCmp < 0;
        }
    }

    public interface CacheBlobReaderSupplier {
        CacheBlobReader getCacheBlobReaderForPreFetching(String fileName, BlobLocation blobLocation);
    }

    public static class PrefetchExecutor implements Executor {
        private final Logger logger = LogManager.getLogger(PrefetchExecutor.class);

        private final ThrottledTaskRunner throttledFetchExecutor;

        public PrefetchExecutor(ThreadPool threadPool) {
            this.throttledFetchExecutor = new ThrottledTaskRunner(
                PrefetchExecutor.class.getCanonicalName(),
                // Leave room for the recovery and online pre-warming to make progress
                threadPool.info(PREWARM_THREAD_POOL).getMax() / 2 + 1,
                threadPool.executor(Stateless.PREWARM_THREAD_POOL)
            );
        }

        @Override
        public void execute(Runnable command) {
            throttledFetchExecutor.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        command.run();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to execute search commit prefetch task", e);
                }

                @Override
                public String toString() {
                    return command.toString();
                }
            });
        }
    }
}
