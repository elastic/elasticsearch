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
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.recovery.metering.RecoveryMetricsCollector;
import co.elastic.elasticsearch.stateless.utils.IndexingShardRecoveryComparator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.codecs.CodecUtil;
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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput.BUFFER_SIZE;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

public class SharedBlobCacheWarmingService {

    public enum Type {
        INDEXING_EARLY,
        INDEXING,
        SEARCH
    }

    public static final String BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC = "es.blob_cache_warming.page_aligned_bytes.total";

    /** Region of a blob **/
    private record BlobRegion(BlobFile blob, int region) {}

    /** Range of a blob to warm in cache, with a listener to complete once it is warmed **/
    private record BlobRange(String fileName, BlobLocation blobLocation, long position, long length, ActionListener<Void> listener)
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
         * @param fileName      the name of the file for which the range must be warmed up in cache.
         * @param blobLocation  the blob location of the file
         * @param position      the position in the blob where warming must start
         * @param length        the length of bytes to warm
         * @param listener      the listener to complete once the range is warmed
         * @return {@code true} if a warming task must be created to warm the range, {@code false} otherwise
         */
        private boolean add(String fileName, BlobLocation blobLocation, long position, long length, ActionListener<Void> listener) {
            maxBlobLength.accumulateAndGet(blobLocation.offset() + blobLocation.fileLength(), Math::max);
            queue.add(new BlobRange(fileName, blobLocation, position, length, listener));
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

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private final Executor fetchExecutor;
    private final ThrottledTaskRunner throttledTaskRunner;
    private final ThrottledTaskRunner cfeThrottledTaskRunner;
    private final LongCounter cacheWarmingPageAlignedBytesTotalMetric;
    private final long prewarmingRangeMinimizationStep;

    public SharedBlobCacheWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        Settings settings
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.fetchExecutor = threadPool.executor(Stateless.PREWARM_THREAD_POOL);

        // the PREWARM_THREAD_POOL does the actual work but we want to limit the number of prewarming tasks in flight at once so that each
        // one completes sooner, so we use a ThrottledTaskRunner. The throttle limit is a little more than the threadpool size just to avoid
        // having the PREWARM_THREAD_POOL stall while the next task is being queued up
        this.throttledTaskRunner = new ThrottledTaskRunner(
            "prewarming-cache",
            1 + threadPool.info(Stateless.PREWARM_THREAD_POOL).getMax(),
            threadPool.generic() // TODO should be DIRECT, forks to the fetch pool pretty much straight away, but see ES-8448
        );
        // We fork cfe prewarming to the generic pool to avoid blocking stateless_fill_vbcc_cache threads,
        // since their completion can also happen on that pool (and it is sized only for copying prefilled buffers to disk).
        // We have to throttle it, so we do not potentially overload the generic pool with I/O tasks.
        this.cfeThrottledTaskRunner = new ThrottledTaskRunner("cfe-prewarming-cache", 2, threadPool.generic());
        this.cacheWarmingPageAlignedBytesTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC, "Total bytes warmed in cache", "bytes");
        this.prewarmingRangeMinimizationStep = PREWARMING_RANGE_MINIMIZATION_STEP.get(settings).getBytes();
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
            fetchExecutor,
            listener.map(b -> null)
        );
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
     */
    public void warmCacheForShardRecovery(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory
    ) {
        warmCache(type, indexShard, commit, directory, ActionListener.noop());
    }

    protected void warmCache(
        Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        ActionListener<Void> listener
    ) {
        final Store store = indexShard.store();
        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache for " + indexShard + ", store is closing"));
            return;
        }
        try (var warmer = new Warmer(type, indexShard, commit, directory, ActionListener.runAfter(listener, store::decRef))) {
            warmer.run();
        }
    }

    private static boolean shouldFullyWarmUp(String fileName) {
        var extension = LuceneFilesExtensions.fromFile(fileName);
        return extension == null // segments_N are fully warmed up in cache
            || extension.isMetadata() // metadata files
            || StatelessCommitService.isGenerationalFile(fileName); // generational files
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> {
        assert ThreadPool.assertCurrentThreadPool(
            Stateless.PREWARM_THREAD_POOL,
            Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        );
        return ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE);
    });

    private class Warmer implements Releasable {

        private final Type type;
        private final IndexShard indexShard;
        private final StatelessCompoundCommit commit;
        private final BlobStoreCacheDirectory directory;
        private final ConcurrentMap<BlobRegion, BlobRangesQueue> queues;
        private final RefCountingListener listeners;

        private final AtomicLong tasksCount = new AtomicLong(0L);
        private final AtomicLong totalBytesCopied = new AtomicLong(0L);

        Warmer(
            Type type,
            IndexShard indexShard,
            StatelessCompoundCommit commit,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            this.type = type;
            this.indexShard = indexShard;
            this.commit = commit;
            this.directory = directory;
            this.queues = new ConcurrentHashMap<>();
            this.listeners = new RefCountingListener(metering(logging(listener)));
        }

        private ActionListener<Void> logging(ActionListener<Void> target) {
            final long started = threadPool.rawRelativeTimeInMillis();
            logger.debug("{} {} warming, generation={}", indexShard.shardId(), type, commit.generation());
            return ActionListener.runBefore(target, () -> {
                final long duration = threadPool.rawRelativeTimeInMillis() - started;
                logger.log(
                    duration >= 5000 ? Level.INFO : Level.DEBUG,
                    "{} {} warming completed in {} ms ({} segments, {} files, {} tasks, {} bytes)",
                    indexShard.shardId(),
                    type,
                    duration,
                    commit.commitFiles()
                        .keySet()
                        .stream()
                        .filter(file -> LuceneFilesExtensions.fromFile(file) == LuceneFilesExtensions.SI)
                        .count(),
                    commit.commitFiles().size(),
                    tasksCount.get(),
                    totalBytesCopied.get()
                );
            }).delegateResponse((l, e) -> {
                Supplier<String> logMessage = () -> Strings.format("%s %s warming failed", indexShard.shardId(), type);
                if (logger.isDebugEnabled()) {
                    logger.debug(logMessage, e);
                } else {
                    logger.info(logMessage);
                }
                l.onFailure(e);
            });
        }

        private ActionListener<Void> metering(ActionListener<Void> target) {
            return ActionListener.runAfter(
                target,
                () -> cacheWarmingPageAlignedBytesTotalMetric.incrementBy(
                    totalBytesCopied.get(),
                    RecoveryMetricsCollector.commonMetricLabels(indexShard)
                )
            );
        }

        void run() {
            commit.commitFiles()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()))
                .forEach(entry -> addFile(entry.getKey(), entry.getValue()));
        }

        @Override
        public void close() {
            listeners.close();
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
         * @param fileName the name of the Lucene file
         * @param blobLocation the blob location of the Lucene file
         */
        private void addFile(String fileName, BlobLocation blobLocation) {
            if (indexShard.store().isClosing()) {
                // skipping scheduling when store is closing
            } else if (LuceneFilesExtensions.fromFile(fileName) == LuceneFilesExtensions.CFE) {
                SubscribableListener
                    // warm entire CFE file
                    .<Void>newForked(listener -> addLocation(blobLocation, fileName, listener))
                    // parse it and schedule warming of corresponding parts of CFS file
                    .andThenAccept(ignored -> addCfe(fileName))
                    .addListener(listeners.acquire());
            } else if (shouldFullyWarmUp(fileName) || blobLocation.fileLength() <= BUFFER_SIZE) {
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
            final int startRegion = (int) (start / regionSize);
            final int endRegion = (int) ((end - (end % regionSize == 0 ? 1 : 0)) / regionSize);

            if (startRegion == endRegion) {
                enqueue(new BlobRegion(location.blobFile(), startRegion), fileName, location, position, length, listener);
            } else {
                try (var listeners = new RefCountingListener(listener)) {
                    for (int r = startRegion; r <= endRegion; r++) {
                        // adjust the position & length to the region
                        var range = ByteRange.of(Math.max(start, (long) r * regionSize), Math.min(end, (r + 1L) * regionSize));
                        enqueue(
                            new BlobRegion(location.blobFile(), r),
                            fileName,
                            location,
                            range.start(),
                            range.length(),
                            listeners.acquire()
                        );
                    }
                }
            }
        }

        private void addCfe(String fileName) {
            assert indexShard.store().hasReferences();// store.incRef() is held by toplevel warmCache until warming is complete
            // We spawn to the generic pool here (via a throtrled task runner), so that we have the following invocation path across
            // the thread pools: GENERIC (recovery) -> FILL_VBCC_THREAD_POOL (if fetching from indexing node) -> GENERIC.
            // We expect no blocking here since `addCfe` gets called AFTER warming the region.
            cfeThrottledTaskRunner.enqueueTask(listeners.acquire().map(ref -> {
                try (ref; var in = directory.openInput(fileName, IOContext.READONCE)) {
                    var entries = Lucene90CompoundEntriesReader.readEntries(in);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Detected {} entries in {}: {}", entries.size(), fileName, entries);
                    }

                    var cfs = fileName.replace(".cfe", ".cfs");
                    var cfsLocation = commit.commitFiles().get(cfs);

                    entries.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()))
                        .forEach(
                            entry -> addFile(
                                entry.getKey(),
                                new BlobLocation(
                                    cfsLocation.blobFile(),
                                    cfsLocation.offset() + entry.getValue().offset(),
                                    entry.getValue().length()
                                )
                            )
                        );
                    return null;
                }
            }));
        }

        private void enqueue(
            BlobRegion blobRegion,
            String fileName,
            BlobLocation blobLocation,
            long position,
            long length,
            ActionListener<Void> listener
        ) {
            var blobRanges = queues.computeIfAbsent(blobRegion, BlobRangesQueue::new);
            if (blobRanges.add(fileName, blobLocation, position, length, listener)) {
                createWarmingTask(blobRanges);
            }
        }

        private void createWarmingTask(BlobRangesQueue queue) {
            throttledTaskRunner.enqueueTask(new WarmingTask(queue));
            tasksCount.incrementAndGet();
        }

        private boolean isCancelled() {
            return indexShard.store().isClosing() || indexShard.state() != IndexShardState.RECOVERING;
        }

        /**
         * Warms in cache all pending file locations of a given blob region.
         */
        private class WarmingTask implements ActionListener<Releasable> {

            private final BlobRangesQueue queue;
            private final BlobRegion blobRegion;

            WarmingTask(BlobRangesQueue queue) {
                this.queue = Objects.requireNonNull(queue);
                this.blobRegion = queue.blobRegion;
                logger.trace("{} {}: scheduled {}", indexShard.shardId(), type, blobRegion);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try (RefCountingRunnable refs = new RefCountingRunnable(() -> Releasables.close(releasable))) {
                    var cacheKey = new FileCacheKey(indexShard.shardId(), blobRegion.blob.primaryTerm(), blobRegion.blob.blobName());

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
                            var cacheBlobReader = directory.getCacheBlobReaderForWarming(blobLocation);
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
                            Stateless.PREWARM_THREAD_POOL,
                            Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
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
                logger.error(() -> format("%s %s failed to warm region %s", indexShard.shardId(), type, blobRegion), e);
            }

            @Override
            public String toString() {
                return "WarmingTask{region=" + blobRegion + "}";
            }
        }
    }
}
