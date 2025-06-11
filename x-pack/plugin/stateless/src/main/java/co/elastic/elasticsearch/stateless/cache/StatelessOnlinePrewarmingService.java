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
import co.elastic.elasticsearch.stateless.cache.reader.LazyRangeMissingHandler;
import co.elastic.elasticsearch.stateless.cache.reader.SequentialRangeMissingHandler;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL;
import static co.elastic.elasticsearch.stateless.Stateless.PREWARM_THREAD_POOL;
import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.INFO;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

/**
 * {@link OnlinePrewarmingService} implementation specific to our stateless search nodes. This aims to prewarm all the segments of the
 * provided {@link IndexShard} by reading the data from the blob store and writing it to the cache, unless the segments are already warm
 * in which case the prewarm operation is a no-op.
 *
 * We’re aiming to download the first region (currently 16MB) of every segment. A segment is composed of multiple files: fields,
 * fields data, fields index, term index, term dictionary etc.
 * Some of the small files that most searches will need are the term index file (tip) and points index (kdi). These small files reside at
 * the beginning of the blob (i.e. the first region) we store in the blob store.
 *
 * Note that this service is executing on the hot path for the query phase, sometimes on the transport_worker thread, so it needs to be
 * very fast and efficient. There is another service that does various cache warming operations in {@link SharedBlobCacheWarmingService}
 * however, due to that service being quite heavy already (both in terms of responsibility - currently handling three different types of
 * warming with different needs and different methods - and objects created) we decided to keep the search online prewarming separately
 * and contained.
 */
public class StatelessOnlinePrewarmingService implements OnlinePrewarmingService {

    public static final Setting<Boolean> STATELESS_ONLINE_PREWARMING_ENABLED = Setting.boolSetting(
        "stateless.online.prewarming.enabled",
        false,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(StatelessOnlinePrewarmingService.class);
    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> {
        assert ThreadPool.assertCurrentThreadPool(
            Stateless.PREWARM_THREAD_POOL,
            Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        ) : "writeBuffer should only be used in the prewarm or fill vbcc thread pool but used in " + Thread.currentThread().getName();
        return ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE);
    });
    public static final String SHARD_TOOK_DURATION_HISTOGRAM_NAME = "es.online_prewarming.shard_took_durations.histogram";
    public static final String SEGMENT_PREWARMING_EXECUTION_WAITING_TIME_HISTOGRAM_NAME =
        "es.online_prewarming.segment_execution_waiting_time.histogram";

    private final ThreadPool threadPool;

    private final boolean enabled;
    private final StatelessSharedBlobCacheService cacheService;
    private final ThrottledTaskRunner throttledTaskRunner;
    // histogram to record the duration of the prewarming for an entire shard
    private final LongHistogram shardTookDurationMillisHistogram;
    // histogram to record the time it took for a prewarming task for a segment to start executing
    private final LongHistogram segmentExecutionWaitTimeMillisHistogram;

    public StatelessOnlinePrewarmingService(
        Settings settings,
        ThreadPool threadPool,
        StatelessSharedBlobCacheService cacheService,
        MeterRegistry meterRegistry
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.enabled = STATELESS_ONLINE_PREWARMING_ENABLED.get(settings);
        // leave a few threads available for the blob cache warming that happens on shard recovery
        this.throttledTaskRunner = new ThrottledTaskRunner(
            "online-prewarming",
            threadPool.info(PREWARM_THREAD_POOL).getMax() / 2 + 1,
            threadPool.executor(PREWARM_THREAD_POOL)
        );
        shardTookDurationMillisHistogram = meterRegistry.registerLongHistogram(
            SHARD_TOOK_DURATION_HISTOGRAM_NAME,
            "The prewarming for an entire shard duration in milliseconds, expressed as a histogram",
            "millis"
        );

        segmentExecutionWaitTimeMillisHistogram = meterRegistry.registerLongHistogram(
            SEGMENT_PREWARMING_EXECUTION_WAITING_TIME_HISTOGRAM_NAME,
            "The time it took for a prewarming task for a segment to start executing in milliseconds, expressed as a histogram",
            "millis"
        );
    }

    @Override
    public void prewarm(IndexShard indexShard) {
        if (enabled == false) {
            logger.trace("online prewarming is disabled");
            return;
        }

        prewarm(indexShard, ActionListener.noop());
    }

    // visible for testing
    void prewarm(IndexShard indexShard, ActionListener<Void> listener) {
        Store store = indexShard.store();
        if (store.isClosing()) {
            return;
        }

        var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
        Collection<BlobFileRanges> highestSegmentInfoRanges = searchDirectory.getHighestOffsetSegmentInfos();

        if (highestSegmentInfoRanges.isEmpty()) {
            // exit early for empty indices or indices that didn't recover yet
            listener.onResponse(null);
            return;
        }

        final long started = threadPool.relativeTimeInMillis();
        AtomicInteger bytesCopiedForShard = new AtomicInteger(0);
        ActionListener<Void> prewarmShardListener = ActionListener.runBefore(listener, () -> {
            final long durationMillis = threadPool.relativeTimeInMillis() - started;
            shardTookDurationMillisHistogram.record(durationMillis);
            logger.log(
                durationMillis >= 5_000 ? INFO : DEBUG,
                "shard {} online prewarming completed in [{}]ms for [{}] bytes",
                indexShard.shardId(),
                durationMillis,
                bytesCopiedForShard.get()
            );
        }).delegateResponse((l, e) -> {
            Supplier<String> logMessage = () -> Strings.format(
                "shard %s online prewarming failed due to: %s",
                indexShard.shardId(),
                e.getMessage()
            );
            if (logger.isDebugEnabled()) {
                logger.debug(logMessage, e);
            } else {
                logger.info(logMessage);
            }
            l.onFailure(e);
        });

        try (var refs = new RefCountingListener(prewarmShardListener)) {
            for (BlobFileRanges siRange : highestSegmentInfoRanges) {
                // we're looking to warm the first region of every compound commit whenever we have an incoming search request
                // as part of the refresh cost optimization we currently group compound commits (segments) into one blob file if the
                // individual compound commits are smaller than 16MiB. If the compound commit is larger than 16MiB it goes to the blob store
                // in its own blob file.
                // Based on the batching of compound commits we derived the following heuristic:
                // - if there are *multiple compound commits* in one blob file we are dealing either with one blob file that's smaller than
                // 16MiB in which case we'll just download the entire file, or otherwise the blob file is larger than 16MiB. If the blob
                // file is larger than 16MiB we accumulated commits up to 15MiB and then batched them with a new commit that was larger than
                // 1MiB (this extra commit could've been in the GiB range) so we want to download the second region as well to make sure
                // the extra commit that pushed the blob file size over 16MiB is also prewarmed.
                // e.g. stateless_commit_3: <CC_1-15.9MiB><CC2-3GiB>
                // - if there is a single compound commit in the blob file we will just prewarm the first region of the blob file.
                final int endRegion = siRange.fileOffset() + siRange.fileLength() > cacheService.getRegionSize() ? 1 : 0;
                var cacheKey = new FileCacheKey(indexShard.shardId(), siRange.primaryTerm(), siRange.blobName());
                var cacheBlobReader = searchDirectory.getCacheBlobReaderForSearchOnlineWarming(cacheKey.fileName(), siRange.blobLocation());

                for (int i = 0; i <= endRegion; i++) {
                    if (store.isClosing() || store.tryIncRef() == false) {
                        return;
                    }
                    long offset = (long) i * cacheService.getRegionSize();
                    long blobLength = siRange.fileOffset() + siRange.fileLength();
                    // Determine the maximum bytes to read, limited by either the cache region size or the remaining
                    // blob content. This boundary check is specifically required by the IndexingShardCacheBlobReader
                    // implementation, which cannot read past the end of the non-uploaded blob when fetching from
                    // indexing nodes.
                    var lengthToRead = Math.min(cacheService.getRegionSize(), blobLength - offset);
                    var range = cacheBlobReader.getRange(offset, Math.toIntExact(lengthToRead), blobLength - offset);
                    logger.trace("online prewarming for key [{}] and region [{}] triggered", cacheKey, i);
                    long segmentWarmingTriggeredMillis = threadPool.relativeTimeInMillis();
                    cacheService.maybeFetchRange(
                        cacheKey,
                        i,
                        range,
                        blobLength,
                        new LazyRangeMissingHandler<>(
                            () -> new SequentialRangeMissingHandler(
                                this,
                                cacheKey.fileName(),
                                range,
                                cacheBlobReader,
                                () -> writeBuffer.get().clear(),
                                bytesCopiedForShard::addAndGet,
                                // Can be executed on different thread pool depending whether we read from
                                // the SharedBlobCacheWarmingService (PREWARM_THREAD_POOL pool) or the IndexingShardCacheBlobReader (VBCC
                                // pool)
                                PREWARM_THREAD_POOL,
                                FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                            )
                        ),
                        fetchRangeRunnable -> throttledTaskRunner.enqueueTask(new ActionListener<>() {
                            @Override
                            public void onResponse(Releasable releasable) {
                                try (releasable) {
                                    long segmentWarmingStartedMillis = threadPool.relativeTimeInMillis();
                                    fetchRangeRunnable.run();
                                    long segmentWarmingCompleteMillis = threadPool.relativeTimeInMillis();
                                    long segmentTaskWaitTimeMillis = segmentWarmingStartedMillis - segmentWarmingTriggeredMillis;
                                    segmentExecutionWaitTimeMillisHistogram.record(segmentTaskWaitTimeMillis);
                                    logger.trace(
                                        "online prewarming for key [{}], offset [{}], complete in [{}]ms.wait to process time was [{}]ms",
                                        cacheKey,
                                        offset,
                                        segmentWarmingCompleteMillis - segmentWarmingTriggeredMillis,
                                        segmentTaskWaitTimeMillis
                                    );
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.error(() -> format("%s failed to online prewarm cache key %s", indexShard.shardId(), cacheKey), e);
                            }
                        }),
                        ActionListener.runAfter(refs.acquire().map(b -> null), store::decRef)
                    );
                }
            }
        }
    }
}
