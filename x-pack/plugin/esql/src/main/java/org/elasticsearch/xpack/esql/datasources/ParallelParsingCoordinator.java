/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates parallel parsing of a single file by splitting it into byte-range
 * segments and dispatching each segment to a parser thread. Results are reassembled
 * in segment order so that row ordering is preserved.
 * <p>
 * Inspired by ClickHouse's {@code ParallelParsingInputFormat}. The approach:
 * <ol>
 *   <li>A segmentator divides the file into N byte-range segments at record boundaries</li>
 *   <li>Each segment is parsed independently on a separate executor thread</li>
 *   <li>The coordinator yields pages in segment order via a {@link CloseableIterator}</li>
 * </ol>
 * <p>
 * This coordinator only works with {@link SegmentableFormatReader} implementations
 * (line-oriented formats like CSV and NDJSON). Columnar formats have their own
 * row-group-level parallelism.
 */
public final class ParallelParsingCoordinator {

    private static final Logger logger = LogManager.getLogger(ParallelParsingCoordinator.class);

    /**
     * Fallback per-file cap on concurrently-open segment streams, used by overloads that don't resolve the
     * {@code max_concurrent_open_segments} pragma (tests and internal callers). Sourced from the single
     * source of truth {@link SourceOperatorContext#DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS}.
     */
    static final int DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS = SourceOperatorContext.DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS;

    private ParallelParsingCoordinator() {}

    /**
     * Creates a parallel-parsing iterator over a single storage object.
     * <p>
     * The file is divided into {@code parallelism} segments at record boundaries.
     * Each segment is parsed independently and results are yielded in order.
     * If the file is too small for meaningful parallelism (below the reader's
     * {@link SegmentableFormatReader#minimumSegmentSize()} per segment), falls
     * back to single-threaded reading.
     *
     * @param reader            the segmentable format reader
     * @param storageObject     the file to read
     * @param projectedColumns  columns to project
     * @param batchSize         rows per page
     * @param parallelism       number of parallel parser threads
     * @param executor          executor for parser threads
     * @return an iterator that yields pages in segment order
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor
    ) throws IOException {
        return parallelRead(reader, storageObject, projectedColumns, batchSize, parallelism, executor, null, false, true, null);
    }

    /**
     * Creates a parallel-parsing iterator with an explicit error policy.
     *
     * @param errorPolicy error handling policy for per-segment parsing, or {@code null} for defaults
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy
    ) throws IOException {
        return parallelRead(reader, storageObject, projectedColumns, batchSize, parallelism, executor, errorPolicy, false, true, null);
    }

    /**
     * Convenience overload that forwards {@code splitIncludesFileLeader=true}. This assumes the
     * storage object includes the file's leading bytes (header row). For non-leading macro-splits,
     * use the nine-argument overload with explicit {@code splitIncludesFileLeader=false}.
     *
     * @param splitStartsAtRecordBoundary when {@code true}, {@code storageObject} is a byte range that already begins
     *                                     on a record boundary (e.g. newline-aligned macro {@link FileSplit});
     *                                     single-threaded fallback reads must set {@link FormatReadContext#recordAligned()}.
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        boolean splitStartsAtRecordBoundary
    ) throws IOException {
        return parallelRead(
            reader,
            storageObject,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            errorPolicy,
            splitStartsAtRecordBoundary,
            true,
            null
        );
    }

    /**
     * @param splitStartsAtRecordBoundary when {@code true}, {@code storageObject} is a byte range that already begins
     *                                     on a record boundary (e.g. newline-aligned macro {@link FileSplit});
     *                                     single-threaded fallback reads must set {@link FormatReadContext#recordAligned()}.
     * @param splitIncludesFileLeader     whether this split contains the file-leading bytes (and therefore file header for
     *                                     header-bearing formats). For whole-file reads this is {@code true}; for
     *                                     non-leading macro-splits this is {@code false}.
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        boolean splitStartsAtRecordBoundary,
        boolean splitIncludesFileLeader
    ) throws IOException {
        return parallelRead(
            reader,
            storageObject,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            errorPolicy,
            splitStartsAtRecordBoundary,
            splitIncludesFileLeader,
            null
        );
    }

    /**
     * Forwards to the full-control overload with the {@link #DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS
     * default} open-segment cap and {@code captureSink=null} (text-format readers' close hooks
     * then publish into whatever {@link ExternalStatsCapture} sink — if any — happens to be bound
     * on the calling thread). Callers that resolve the {@code max_concurrent_open_segments} pragma
     * or care about cross-thread stats capture use the 12-arg overload.
     *
     * @param readSchema planner-bound read schema, or {@code null} for per-file inference
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        boolean splitStartsAtRecordBoundary,
        boolean splitIncludesFileLeader,
        List<Attribute> readSchema
    ) throws IOException {
        return parallelRead(
            reader,
            storageObject,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            errorPolicy,
            splitStartsAtRecordBoundary,
            splitIncludesFileLeader,
            readSchema,
            DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS,
            null
        );
    }

    /**
     * Full-control overload that takes both the {@code max_concurrent_open_segments} cap and an
     * explicit {@code captureSink} for per-chunk source-stats contributions.
     * <p>
     * {@code maxConcurrentOpenSegments} is the per-file limit on byte-range segments whose read
     * streams are open at once. Because the consumer drains segments in order, only the head
     * segments need be open; this caps the open-stream / buffer count independent of file count and
     * length. See {@link OrderedParallelIterator}.
     * <p>
     * Each segment is parsed on a worker thread; this coordinator binds {@code captureSink} on that
     * worker around the per-segment {@link CloseableIterator#close()} so text-format readers' close
     * hooks see the same sink the consumer-thread wrapper sees. Pass {@code null} when no capture
     * is desired (tests, benchmarks).
     *
     * @param maxConcurrentOpenSegments per-file cap on concurrently-open segment streams (>= 1)
     * @param captureSink               consumer-owned per-file stats sink to bind on each parser
     *                                  worker, or {@code null} to disable capture
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        boolean splitStartsAtRecordBoundary,
        boolean splitIncludesFileLeader,
        List<Attribute> readSchema,
        int maxConcurrentOpenSegments,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink
    ) throws IOException {
        return parallelRead(
            reader,
            storageObject,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            errorPolicy,
            splitStartsAtRecordBoundary,
            splitIncludesFileLeader,
            readSchema,
            maxConcurrentOpenSegments,
            captureSink,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );
    }

    /**
     * Full-control overload that also takes the {@code max_record_size} cap used by record splitters.
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        boolean splitStartsAtRecordBoundary,
        boolean splitIncludesFileLeader,
        List<Attribute> readSchema,
        int maxConcurrentOpenSegments,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
        int maxRecordBytes
    ) throws IOException {
        long fileLength = storageObject.length();
        long minSegment = reader.minimumSegmentSize();

        // COUNT(*) and similar: projectedColumns is empty while rows still need structural validation
        // against the file width. When this read includes the file-leading bytes (and therefore any
        // header), bind the full on-disk schema before segment workers run. For non-leading macro
        // splits, rebinding via metadata is unsafe because the split-local first row is data, not header.
        SegmentableFormatReader parallelReader = reader;
        if (projectedColumns != null && projectedColumns.isEmpty() && splitIncludesFileLeader) {
            var meta = parallelReader.metadata(storageObject);
            if (meta != null && meta.schema() != null && meta.schema().isEmpty() == false) {
                parallelReader = (SegmentableFormatReader) parallelReader.withSchema(meta.schema());
            }
        }

        ErrorPolicy effectivePolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.STRICT;
        // Empty list would read as "0-column schema"; pass null through.
        FormatReadContext baseCtx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .errorPolicy(effectivePolicy)
            .firstSplit(splitIncludesFileLeader)
            .recordAligned(splitStartsAtRecordBoundary)
            .readSchema(readSchema)
            .maxRecordBytes(maxRecordBytes)
            .build();
        if (parallelism <= 1 || fileLength < minSegment * 2) {
            return parallelReader.read(storageObject, baseCtx);
        }

        List<long[]> segments = computeSegments(parallelReader, storageObject, fileLength, parallelism, minSegment, maxRecordBytes);

        if (segments.size() <= 1) {
            return parallelReader.read(storageObject, baseCtx);
        }

        OrderedParallelIterator iterator = new OrderedParallelIterator(
            parallelReader,
            storageObject,
            projectedColumns,
            batchSize,
            segments,
            executor,
            parallelism,
            maxConcurrentOpenSegments,
            effectivePolicy,
            splitIncludesFileLeader,
            readSchema,
            captureSink,
            maxRecordBytes
        );
        // Fully constructed and published before any worker is dispatched — see OrderedParallelIterator#start.
        iterator.start();
        return iterator;
    }

    /**
     * Computes byte-range segments for the file by probing record boundaries.
     * Each segment is a {@code [offset, length]} pair. The first segment starts
     * at offset 0; subsequent segments start at the record boundary found after
     * the nominal split point.
     */
    public static List<long[]> computeSegments(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        long fileLength,
        int parallelism,
        long minSegment
    ) throws IOException {
        return computeSegments(
            reader,
            storageObject,
            fileLength,
            parallelism,
            minSegment,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );
    }

    /**
     * Computes byte-range segments using a splitter capped by {@code maxRecordBytes}.
     */
    public static List<long[]> computeSegments(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        long fileLength,
        int parallelism,
        long minSegment,
        int maxRecordBytes
    ) throws IOException {
        long nominalSize = fileLength / parallelism;
        if (nominalSize < minSegment) {
            nominalSize = minSegment;
        }

        RecordSplitter splitter = reader.recordSplitter(maxRecordBytes);
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);

        long pos = nominalSize;
        while (pos < fileLength) {
            long remaining = fileLength - pos;
            if (remaining < minSegment) {
                break;
            }
            InputStream stream = storageObject.newStream(pos, remaining);
            // Abort rather than close: findNextRecordBoundary reads only a prefix of the range
            // (fileLength - pos bytes), but close() on providers like S3 drains the remainder.
            try (Closeable abortOnExit = () -> storageObject.abortStream(stream)) {
                long skipped = splitter.findNextRecordBoundary(stream);
                if (skipped < 0) {
                    break;
                }
                long boundary = pos + skipped;
                if (boundary >= fileLength) {
                    break;
                }
                if (fileLength - boundary < minSegment) {
                    break;
                }
                boundaries.add(boundary);
            }
            pos = boundaries.get(boundaries.size() - 1) + nominalSize;
        }

        List<long[]> segments = new ArrayList<>(boundaries.size());
        for (int i = 0; i < boundaries.size(); i++) {
            long start = boundaries.get(i);
            long end = (i + 1 < boundaries.size()) ? boundaries.get(i + 1) : fileLength;
            segments.add(new long[] { start, end - start });
        }
        return segments;
    }

    /**
     * Iterator that dispatches segment parsing to an executor and yields pages
     * in strict segment order. Each segment's pages are fully consumed before
     * moving to the next segment.
     * <p>
     * Parser threads push pages into per-segment queues. The consumer thread
     * (the driver calling {@code next()}) drains queues in order.
     */
    private static final class OrderedParallelIterator implements CloseableIterator<Page> {

        private static final Page POISON = new Page(0);
        private static final long CLOSE_TIMEOUT_SECONDS = 60;

        private final SegmentableFormatReader reader;
        private final StorageObject storageObject;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final ErrorPolicy errorPolicy;
        private final boolean splitIncludesFileLeader;
        @Nullable
        private final List<Attribute> readSchema;
        /**
         * Consumer-owned per-file stats sink. Captured at construction so each segment worker can
         * bind it around {@code reader.read(...).close()} — the text-format readers' close hooks
         * publish per-chunk {@code _stats.*} contributions through {@link ExternalStatsCapture},
         * and {@code ACTIVE} is a plain {@link ThreadLocal} that does not propagate to executor
         * threads. {@code null} disables per-segment capture (e.g. tests, benchmarks).
         */
        @Nullable
        private final ConcurrentMap<String, List<Map<String, Object>>> captureSink;
        private final int maxRecordBytes;

        private final List<long[]> segments;
        private final Executor executor;
        private final int maxConcurrentSegments;
        private final List<BlockingQueue<Page>> segmentQueues;
        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        private final CountDownLatch allDone;

        private int currentSegment = 0;
        private Page buffered = null;
        private volatile boolean closed = false;

        OrderedParallelIterator(
            SegmentableFormatReader reader,
            StorageObject storageObject,
            List<String> projectedColumns,
            int batchSize,
            List<long[]> segments,
            Executor executor,
            int parallelism,
            int maxConcurrentOpenSegments,
            ErrorPolicy errorPolicy,
            boolean splitIncludesFileLeader,
            List<Attribute> readSchema,
            @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
            int maxRecordBytes
        ) {
            this.reader = reader;
            this.storageObject = storageObject;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.errorPolicy = errorPolicy;
            this.splitIncludesFileLeader = splitIncludesFileLeader;
            this.readSchema = readSchema;
            this.captureSink = captureSink;
            this.maxRecordBytes = maxRecordBytes;
            this.segments = segments;
            this.executor = executor;
            // Single clamp site for the effective window: the configured cap, never more than the parser
            // thread pool can run nor more segments than exist, floored at 1.
            this.maxConcurrentSegments = Math.max(1, Math.min(maxConcurrentOpenSegments, Math.min(parallelism, segments.size())));
            this.allDone = new CountDownLatch(segments.size());

            this.segmentQueues = new ArrayList<>(segments.size());
            for (int i = 0; i < segments.size(); i++) {
                segmentQueues.add(new ArrayBlockingQueue<>(16));
            }
            // Work is dispatched by start(), not here, so no parser thread can observe a partially
            // constructed instance — the constructor fully publishes before any worker runs.
        }

        /**
         * Begins the sliding-window dispatch: submit the first {@code maxConcurrentSegments} segments; each
         * segment, on completion, submits the one that many positions ahead (see parseSegment's finally).
         * This bounds open streams without stalling the in-order consumer, which runs on the driver thread,
         * so the head segment always progresses. Called once by {@link #parallelRead} after construction —
         * keeping it out of the constructor avoids leaking {@code this} to worker threads. A permit acquired
         * inside parseSegment would instead deadlock: a later segment could hold it while blocked on a full
         * queue, starving the head segment the consumer is waiting on.
         */
        void start() {
            // maxConcurrentSegments is already clamped to <= segments.size() in the constructor.
            for (int i = 0; i < maxConcurrentSegments; i++) {
                submitSegment(i);
            }
        }

        /**
         * Submits the segment at {@code startIndex}. On {@link RejectedExecutionException} (executor shutting
         * down) it cannot run, so we poison its queue, count it down, and cascade to the next in the
         * window-chain ({@code startIndex + maxConcurrentSegments}) so no latch is left dangling on teardown.
         */
        private void submitSegment(int startIndex) {
            int segIdx = startIndex;
            while (segIdx < segments.size()) {
                final int idx = segIdx;
                final long[] seg = segments.get(idx);
                try {
                    executor.execute(() -> parseSegment(idx, seg[0], seg[1]));
                    return;
                } catch (RejectedExecutionException e) {
                    firstError.compareAndSet(null, e);
                    enqueuePoison(segmentQueues.get(idx));
                    allDone.countDown();
                    segIdx += maxConcurrentSegments;
                }
            }
        }

        private void parseSegment(int segmentIndex, long offset, long length) {
            BlockingQueue<Page> queue = segmentQueues.get(segmentIndex);
            try {
                // Teardown or earlier failure: skip opening a stream; finally still poisons + cascades.
                if (closed || firstError.get() != null) {
                    return;
                }
                boolean lastSplit = segmentIndex == segmentQueues.size() - 1;
                StorageObject segObj = new RangeStorageObject(storageObject, offset, length);

                // Per-flag semantics:
                // - firstSplit: only segment 0 owns the file's leading bytes (and any header).
                // computeSegments probes the next record boundary so segments 1..N start on a
                // complete record, but for header-bearing formats (CSV) "first split" still means
                // "the segment that contains the header"; otherwise non-first segments would re-run
                // header inference on data rows.
                // - lastSplit: only the trailing segment runs to fileLength; non-final segments
                // end on a record-terminator byte and must NOT be marked lastSplit, so the
                // codec/reader can correctly handle the segment-boundary tail (see
                // ParallelParsingCoordinator's segmentation contract).
                // - recordAligned: every segment is guaranteed to start at a record boundary
                // (computeSegments probes the next record boundary), so line-oriented readers
                // can skip the "drop leading partial line" workaround used for byte-range
                // macro-splits where the leading bytes belong to a previous split. Setting this
                // also lets readers (e.g. NDJSON) skip the byte-by-byte trailing-partial-line
                // scan that the format would otherwise apply per chunk.
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(projectedColumns)
                    .batchSize(batchSize)
                    .errorPolicy(errorPolicy)
                    .firstSplit(splitIncludesFileLeader && segmentIndex == 0)
                    .lastSplit(lastSplit)
                    .recordAligned(true)
                    .readSchema(readSchema)
                    .maxRecordBytes(maxRecordBytes)
                    .build();
                // Bind the consumer-owned sink on this worker so the reader's close hook (which
                // publishes the chunk's _stats.* contribution via ExternalStatsCapture.record) reaches
                // the same map the consumer-thread StatsCapturingIterator binds. The pages iterator
                // is opened *inside* the bound's try-with-resources so a failing reader.read still
                // restores the previous ThreadLocal binding — worker threads are reused across
                // queries by the shared executor, a leaked binding would poison subsequent tasks.
                // Inner closes first, so the close hook's record() call happens with the sink
                // still bound, then the handle restores the previous binding.
                ExternalStatsCapture.Handle bound = captureSink != null ? ExternalStatsCapture.bind(captureSink) : () -> {};
                try (bound) {
                    try (CloseableIterator<Page> pages = reader.read(segObj, ctx)) {
                        while (pages.hasNext()) {
                            if (firstError.get() != null || closed) {
                                break;
                            }
                            Page page = pages.next();
                            enqueueOrRelease(queue, page);
                        }
                    }
                }
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
            } finally {
                enqueuePoison(queue);
                allDone.countDown();
                // Slide the window: this stream is now closed, so the segment maxConcurrentSegments ahead may open.
                int next = segmentIndex + maxConcurrentSegments;
                if (next < segments.size()) {
                    submitSegment(next);
                }
            }
        }

        private void enqueueOrRelease(BlockingQueue<Page> queue, Page page) throws InterruptedException {
            while (true) {
                if (closed || firstError.get() != null) {
                    if (page.getPositionCount() > 0) {
                        page.releaseBlocks();
                    }
                    return;
                }
                if (queue.offer(page, 500, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        }

        private static void enqueuePoison(BlockingQueue<Page> queue) {
            boolean poisoned = false;
            while (poisoned == false) {
                try {
                    queue.put(POISON);
                    poisoned = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (buffered != null) {
                return true;
            }
            try {
                buffered = takeNextPage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for parallel parse results", e);
            }
            return buffered != null;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new java.util.NoSuchElementException();
            }
            Page result = buffered;
            buffered = null;
            return result;
        }

        private Page takeNextPage() throws InterruptedException {
            while (currentSegment < segmentQueues.size()) {
                checkError();
                BlockingQueue<Page> queue = segmentQueues.get(currentSegment);
                Page page = queue.take();
                if (page == POISON) {
                    currentSegment++;
                    continue;
                }
                return page;
            }
            checkError();
            return null;
        }

        private void checkError() {
            Throwable t = firstError.get();
            if (t != null) {
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException("Parallel parsing failed", t);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            // Decide whether to publish a finalize marker before flipping closed=true: the marker
            // means "every segment thread finished cleanly and the per-chunk partials shipped to
            // ExternalStatsCapture represent the complete file." closing=true short-circuits the
            // segment threads' enqueue loops, which we only want AFTER they've drained naturally.
            boolean cleanCompletion = firstError.get() == null && currentSegment >= segmentQueues.size();
            closed = true;
            drainAllQueues();
            try {
                if (allDone.await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                    logger.warn("Timed out waiting for parallel parsing threads to finish after [{}]s", CLOSE_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainAllQueues();
            if (cleanCompletion && firstError.get() == null) {
                publishFinalizeMarker();
            }
        }

        /**
         * Signals the coordinator-side reconciler that the per-chunk partials shipped via
         * {@link ExternalStatsCapture#record} cover
         * the entire file cleanly. Without this marker, partial contributions are discarded.
         */
        private void publishFinalizeMarker() {
            try {
                Instant lastMod = storageObject.lastModified();
                long mtimeMillis = lastMod != null ? lastMod.toEpochMilli() : -1L;
                if (mtimeMillis < 0) {
                    return;
                }
                Map<String, Object> marker = new HashMap<>();
                marker.put(ExternalStats.MTIME_MILLIS_KEY, mtimeMillis);
                marker.put(ExternalStats.FINALIZE_CHUNKS_KEY, Boolean.TRUE);
                ExternalStatsCapture.record(storageObject.path().toString(), marker);
            } catch (IOException e) {
                logger.debug(
                    () -> "ParallelParsingCoordinator: skipping finalize marker for ["
                        + storageObject.path()
                        + "] — lastModified() unavailable",
                    e
                );
            }
        }

        private void drainAllQueues() {
            for (BlockingQueue<Page> queue : segmentQueues) {
                Page p;
                while ((p = queue.poll()) != null) {
                    if (p != POISON && p.getPositionCount() > 0) {
                        p.releaseBlocks();
                    }
                }
            }
        }
    }
}
