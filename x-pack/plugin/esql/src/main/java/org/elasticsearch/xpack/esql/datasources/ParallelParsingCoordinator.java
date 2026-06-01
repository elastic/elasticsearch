/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates parallel parsing of a single file by splitting it into byte-range
 * segments and dispatching each segment to a parser thread. Pages are emitted to the
 * consumer in completion (as-ready) order, not segment order: any segment whose pages
 * are ready drains immediately, so a fully-parsed later segment never waits behind a
 * slower earlier one.
 * <p>
 * Inspired by ClickHouse's {@code ParallelParsingInputFormat}. The approach:
 * <ol>
 *   <li>A segmentator divides the file into N byte-range segments at record boundaries</li>
 *   <li>Each segment is parsed independently on a separate executor thread</li>
 *   <li>The coordinator yields pages as they become ready via a {@link CloseableIterator}</li>
 * </ol>
 * <p>
 * <b>Row ordering.</b> Cross-segment row order is intentionally <em>not</em> preserved:
 * an external scan has no row-order guarantee absent an explicit {@code SORT}, and the
 * read schema is bound up-front (see {@link #parallelRead}) so segment 0 has no obligation
 * to emit first. Holding pages back to reconstruct segment order is what created the bug
 * this design fixes: a parsed-but-not-yet-emitted segment kept its object-store socket open
 * and idle until the in-order cursor reached it; on S3 that idle socket exceeds the server
 * idle timeout and is reset, surfacing as an HTTP 500. Emitting as-ready lets each segment's
 * socket close as soon as its pages are consumed.
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
     * Each segment is parsed independently and pages are yielded as they become ready
     * (completion order, not segment order). If the file is too small for meaningful
     * parallelism (below the reader's {@link SegmentableFormatReader#minimumSegmentSize()}
     * per segment), falls back to single-threaded reading.
     *
     * @param reader            the segmentable format reader
     * @param storageObject     the file to read
     * @param projectedColumns  columns to project
     * @param batchSize         rows per page
     * @param parallelism       number of parallel parser threads
     * @param executor          executor for parser threads
     * @return an iterator that yields pages as they become ready (cross-segment row order not preserved)
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
     * Full-control overload that propagates the planner-resolved {@code readSchema} (so multi-file
     * headerless reads do not drift per file). Pass {@code null} to fall back to per-file inference.
     * Uses the {@link #DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS default} open-segment cap; callers that
     * resolve the {@code max_concurrent_open_segments} pragma use the {@code maxConcurrentOpenSegments}
     * overload.
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
            DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS
        );
    }

    /**
     * Full-control overload that also takes the {@code max_concurrent_open_segments} cap — the per-file
     * limit on byte-range segments whose read streams are open at once. A sliding window dispatches at
     * most {@code maxConcurrentOpenSegments} segments at a time; each completion opens the next. This caps
     * the open-stream / buffer count independent of file count and length. See {@link AsReadyParallelIterator}.
     *
     * @param maxConcurrentOpenSegments per-file cap on concurrently-open segment streams (>= 1)
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
        int maxConcurrentOpenSegments
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

        AsReadyParallelIterator iterator = new AsReadyParallelIterator(
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
            maxRecordBytes
        );
        // Fully constructed and published before any worker is dispatched — see AsReadyParallelIterator#start.
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
     * Iterator that dispatches segment parsing to an executor and yields pages to the consumer
     * as they become ready — in completion order, not segment order. Cross-segment row order is
     * intentionally not preserved (external scans carry no order guarantee absent an explicit
     * {@code SORT}); see the class-level Javadoc on {@link ParallelParsingCoordinator}.
     * <p>
     * Parser threads push pages into a single shared bounded queue. The consumer thread (the driver
     * calling {@code next()}) drains that one queue. A segment's object-store stream therefore closes
     * as soon as its pages are consumed, rather than waiting for an in-order cursor to reach it — which
     * is what let an idle socket sit open long enough to be reset by the server.
     * <p>
     * A sliding window still bounds the number of segment streams open at once
     * ({@code maxConcurrentSegments}): {@link #start()} dispatches the first window and each
     * completing segment, in {@link #parseSegment}'s finally, submits the one {@code maxConcurrentSegments}
     * positions ahead.
     */
    private static final class AsReadyParallelIterator implements CloseableIterator<Page> {

        private static final long CLOSE_TIMEOUT_SECONDS = 60;
        /**
         * Per-segment cap on bytes of buffered pages before this rewrite used a 16-deep per-segment queue.
         * The shared queue keeps the same per-segment budget so total buffered pages stay roughly
         * {@code maxConcurrentSegments * PAGES_PER_OPEN_SEGMENT}, preserving backpressure pressure.
         */
        private static final int PAGES_PER_OPEN_SEGMENT = 16;
        /**
         * How many times a worker re-opens its segment after a connection-reset-class failure before
         * giving up and propagating. Bounded so a persistently-resetting endpoint fails fast rather than
         * looping forever.
         */
        private static final int MAX_STREAM_RESET_RETRIES = 3;
        /**
         * Fixed delay between a connection-reset re-open and the retry, giving a momentarily-unhealthy
         * endpoint a beat to recover rather than hammering it back-to-back.
         */
        private static final long RESET_RETRY_BACKOFF_MILLIS = 100;

        private final SegmentableFormatReader reader;
        private final StorageObject storageObject;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final ErrorPolicy errorPolicy;
        private final boolean splitIncludesFileLeader;
        @org.elasticsearch.core.Nullable
        private final List<Attribute> readSchema;
        private final int maxRecordBytes;

        private final List<long[]> segments;
        private final Executor executor;
        private final int maxConcurrentSegments;
        /**
         * Single bounded queue shared by every segment worker. {@code offer} with a timeout provides
         * backpressure; the consumer drains in as-ready order. The consumer never blocks indefinitely on
         * it: {@link #takeNextPage()} polls with a 200ms timeout and re-checks the completion counter on
         * every wake-up, so termination depends only on that poll, not on any wake-up signal from the
         * workers.
         */
        private final BlockingQueue<Page> sharedQueue;
        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        /** Counts segments still running. Reaches 0 when every worker has finished (success or failure). */
        private final AtomicInteger remainingSegments;
        private final CountDownLatch allDone;

        private Page buffered = null;
        private volatile boolean closed = false;

        AsReadyParallelIterator(
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
            int maxRecordBytes
        ) {
            this.reader = reader;
            this.storageObject = storageObject;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.errorPolicy = errorPolicy;
            this.splitIncludesFileLeader = splitIncludesFileLeader;
            this.readSchema = readSchema;
            this.maxRecordBytes = maxRecordBytes;
            this.segments = segments;
            this.executor = executor;
            // Single clamp site for the effective window: the configured cap, never more than the parser
            // thread pool can run nor more segments than exist, floored at 1.
            this.maxConcurrentSegments = Math.max(1, Math.min(maxConcurrentOpenSegments, Math.min(parallelism, segments.size())));
            this.remainingSegments = new AtomicInteger(segments.size());
            this.allDone = new CountDownLatch(segments.size());
            // Bound total buffered pages to the open-segment window times the per-segment page budget, so
            // backpressure scales with how many streams may be open rather than with the segment count.
            this.sharedQueue = new LinkedBlockingQueue<>(Math.max(1, maxConcurrentSegments * PAGES_PER_OPEN_SEGMENT));
            // Work is dispatched by start(), not here, so no parser thread can observe a partially
            // constructed instance — the constructor fully publishes before any worker runs.
        }

        /**
         * Begins the sliding-window dispatch: submit the first {@code maxConcurrentSegments} segments; each
         * segment, on completion, submits the one that many positions ahead (see parseSegment's finally).
         * This bounds open streams while pages drain in as-ready order, so no segment is gated on an earlier
         * one finishing. Called once by {@link #parallelRead} after construction — keeping it out of the
         * constructor avoids leaking {@code this} to worker threads.
         */
        void start() {
            // maxConcurrentSegments is already clamped to <= segments.size() in the constructor.
            for (int i = 0; i < maxConcurrentSegments; i++) {
                submitSegment(i);
            }
        }

        /**
         * Submits the segment at {@code startIndex}. On {@link RejectedExecutionException} (executor shutting
         * down) it cannot run, so we record the error, mark it finished, and cascade to the next in the
         * window-chain ({@code startIndex + maxConcurrentSegments}) so the completion counter is never left
         * dangling on teardown.
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
                    finishSegment();
                    segIdx += maxConcurrentSegments;
                }
            }
        }

        private void parseSegment(int segmentIndex, long offset, long length) {
            try {
                // Teardown or earlier failure: skip opening a stream; finally still finishes + cascades.
                if (closed || firstError.get() != null) {
                    return;
                }
                readSegmentWithResetRetries(segmentIndex, offset, length);
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
            } finally {
                finishSegment();
                // Slide the window: this stream is now closed, so the segment maxConcurrentSegments ahead may open.
                int next = segmentIndex + maxConcurrentSegments;
                if (next < segments.size()) {
                    submitSegment(next);
                }
            }
        }

        /**
         * Reads one segment, re-opening and resuming on connection-reset-class failures.
         * <p>
         * Segments are {@code recordAligned} and the underlying read is deterministic, so re-opening from the
         * segment start reproduces the same page sequence. On a reset we re-read from the start and skip the
         * rows this worker already enqueued (tracked by {@code deliveredRows}), so each row reaches the shared
         * queue exactly once. {@link InterruptedException} and genuine data/parse errors are not reset-class
         * and propagate immediately; retries are bounded by {@link #MAX_STREAM_RESET_RETRIES}.
         */
        private void readSegmentWithResetRetries(int segmentIndex, long offset, long length) throws Exception {
            boolean lastSplit = segmentIndex == segments.size() - 1;
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

            // Running count of rows from this segment already delivered to the shared queue. Held in a
            // single-element array so the count survives a mid-read throw: readSegmentOnce updates it as it
            // enqueues, so on a reset-resume we re-read from the segment start and skip exactly this many
            // rows, never delivering any row twice.
            long[] deliveredRows = { 0 };
            int attempt = 0;
            while (true) {
                try {
                    readSegmentOnce(segObj, ctx, deliveredRows);
                    return;
                } catch (InterruptedException ie) {
                    // Preserve the interrupt for callers up the stack; a blocking op may have cleared it.
                    Thread.currentThread().interrupt();
                    throw ie;
                } catch (Exception e) {
                    boolean canRetry = attempt < MAX_STREAM_RESET_RETRIES
                        && closed == false
                        && firstError.get() == null
                        && isConnectionReset(e);
                    if (canRetry == false) {
                        throw e;
                    }
                    attempt++;
                    logger.debug(
                        "segment [{}] hit a connection reset after [{}] delivered rows; re-opening (attempt [{}]/[{}])",
                        segmentIndex,
                        deliveredRows[0],
                        attempt,
                        MAX_STREAM_RESET_RETRIES
                    );
                    // Brief backoff so a momentarily-unhealthy endpoint gets a beat to recover rather than
                    // being hammered with an immediate re-open. Interrupt-aware: an interrupt during the
                    // wait aborts the read promptly.
                    try {
                        Thread.sleep(RESET_RETRY_BACKOFF_MILLIS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    }
                }
            }
        }

        /**
         * Reads the segment from its start, skipping the first {@code deliveredRows[0]} rows (already delivered
         * on a prior attempt), and enqueues the remaining pages onto the shared queue. {@code deliveredRows[0]}
         * is advanced only as pages are actually enqueued, in place, so it reflects truly-delivered progress
         * even if this method throws partway through — a subsequent reset-resume then knows exactly where to
         * pick up.
         * <p>
         * The skip is performed at the <em>row</em> level, not the page level. A {@link SegmentableFormatReader}
         * is only required to reproduce the same row sequence on a re-read, not the same pagination (a reader
         * that fills pages by buffer or timing legitimately repaginates). So when the skip lands inside a page,
         * the already-delivered prefix of that page is dropped and only the remaining
         * {@code pageRows - toSkip} rows are enqueued; whole earlier pages are dropped outright.
         */
        private void readSegmentOnce(StorageObject segObj, FormatReadContext ctx, long[] deliveredRows) throws Exception {
            long toSkip = deliveredRows[0];
            CloseableIterator<Page> pages = reader.read(segObj, ctx);
            try (pages) {
                while (pages.hasNext()) {
                    if (firstError.get() != null || closed) {
                        break;
                    }
                    Page page = pages.next();
                    int pageRows = page.getPositionCount();
                    if (toSkip > 0) {
                        // Replay path: this page (or a prefix of it) was already delivered before the reset.
                        if (toSkip >= pageRows) {
                            // Entire page already delivered: drop it and move on.
                            toSkip -= pageRows;
                            page.releaseBlocks();
                            continue;
                        }
                        // The skip lands inside this page: the first `toSkip` rows were already delivered, the
                        // remaining `pageRows - toSkip` rows have not. Slice out the undelivered tail and
                        // enqueue only that. Page#slice builds independent blocks, so the original page is
                        // released here regardless of whether the slice is enqueued.
                        int skipInPage = (int) toSkip;
                        Page remainder = page.slice(skipInPage, pageRows);
                        page.releaseBlocks();
                        toSkip = 0;
                        int remainderRows = remainder.getPositionCount();
                        if (enqueueOrRelease(remainder)) {
                            deliveredRows[0] += remainderRows;
                        }
                        continue;
                    }
                    if (enqueueOrRelease(page)) {
                        deliveredRows[0] += pageRows;
                    }
                }
            }
        }

        /**
         * True if {@code t} (or any cause in its chain) is a transient transport fault whose segment stream
         * can be safely re-opened and resumed, as opposed to a genuine data or parse error (which must
         * propagate without retry).
         * <p>
         * <b>Type first.</b> Classification keys off the exception <em>type</em>: the JDK transport types
         * {@link SocketException} (covers "Connection reset" / "Connection reset by peer" / "Broken pipe"),
         * {@link SocketTimeoutException}, and {@link InterruptedIOException} (a read timeout surfaces as one of
         * these from the object-store HTTP client). The s3 datasource ({@code esql-datasource-s3}) wraps its
         * transport failures: {@code S3StorageObject} rethrows them as {@link IOException} ("Range request
         * failed", "Failed to read object"), and the AWS SDK's own
         * {@code software.amazon.awssdk.core.exception.SdkClientException} / {@code AbortedException} carry the
         * underlying {@code SocketException} as a cause — both of which are caught by the cause-chain walk
         * above. (The esql plugin does not depend on the AWS SDK, so those concrete types cannot be referenced
         * here by class; they are matched through their JDK transport cause.)
         * <p>
         * <b>Message fallback is narrow and documented.</b> Some HTTP clients surface a connection drop as an
         * {@link IOException} with no transport-typed cause — either an outright reset/close, or a mid-read
         * truncation where fewer body bytes arrived than the Content-Length promised (Apache HttpClient's
         * {@code ConnectionClosedException} "Premature end of Content-Length delimited message body", or
         * "unexpected end of stream" elsewhere). Only for those cases do we fall back to a tight substring
         * check on the message. A throwable that cannot be type-identified as transport and does not match
         * those narrow phrases is treated as a real error and is <em>not</em> retried — a genuine data/parse
         * error never gets retried as transient, and a genuinely truncated object simply re-trips and fails
         * cleanly within the bounded retry budget.
         */
        static boolean isConnectionReset(Throwable t) {
            for (Throwable c = t; c != null; c = c.getCause()) {
                // Type-based detection: JDK transport exception types are the authoritative signal.
                if (c instanceof SocketException || c instanceof SocketTimeoutException || c instanceof InterruptedIOException) {
                    return true;
                }
                // Narrow, documented message fallback: only an IOException whose message is a connection-drop
                // phrase. Covers two shapes of the same transport failure — an outright reset/close, and a
                // mid-read truncation where the HTTP client received fewer body bytes than the Content-Length
                // promised (an idle/connection drop on an already-open object stream surfaces from Apache
                // HttpClient as ConnectionClosedException "Premature end of Content-Length delimited message
                // body", and from other clients as "unexpected end of stream"). Deliberately does NOT match
                // arbitrary throwables — a data or parse error that merely mentions the phrase must not be
                // retried; a re-read of a genuinely truncated object simply re-trips and fails cleanly within
                // the bounded retry budget.
                if (c instanceof IOException) {
                    String msg = c.getMessage();
                    if (msg != null) {
                        String lower = msg.toLowerCase(Locale.ROOT);
                        if (lower.contains("connection reset")
                            || lower.contains("connection closed")
                            || lower.contains("premature end of")
                            || lower.contains("unexpected end of stream")) {
                            return true;
                        }
                    }
                }
                if (c.getCause() == c) {
                    break;
                }
            }
            return false;
        }

        /**
         * Offers {@code page} to the shared queue, blocking with a timeout for backpressure. Returns
         * {@code true} if the page was enqueued (and is now owned by the consumer), {@code false} if it was
         * released here because the iterator was closed or an error flipped before it could be enqueued. The
         * caller uses the return value to advance {@code deliveredRows} only on a genuine enqueue, so the
         * resume cursor reflects truly-delivered rows.
         */
        private boolean enqueueOrRelease(Page page) throws InterruptedException {
            while (true) {
                if (closed || firstError.get() != null) {
                    page.releaseBlocks();
                    return false;
                }
                if (sharedQueue.offer(page, 500, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
        }

        /**
         * Marks one segment finished. The consumer does not need an explicit wake-up: {@link #takeNextPage()}
         * polls the shared queue with a 200ms timeout and re-checks the completion counter on every wake-up,
         * so it observes the final segment finishing within one poll interval.
         */
        private void finishSegment() {
            allDone.countDown();
            remainingSegments.decrementAndGet();
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
                throw new NoSuchElementException();
            }
            Page result = buffered;
            buffered = null;
            return result;
        }

        /**
         * Drains the shared queue in as-ready order. Returns the next data page, or {@code null} once every
         * segment has finished and the queue is empty. Polls with a timeout (rather than blocking forever) so
         * the loop re-checks the completion counter and any error within one poll interval — termination does
         * not depend on any wake-up signal from the workers.
         */
        private Page takeNextPage() throws InterruptedException {
            while (true) {
                checkError();
                Page page = sharedQueue.poll(200, TimeUnit.MILLISECONDS);
                if (page != null) {
                    return page;
                }
                // No page available: if all segments are done and the queue is now empty, we're finished.
                if (remainingSegments.get() == 0 && sharedQueue.isEmpty()) {
                    checkError();
                    return null;
                }
            }
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
            closed = true;
            drainQueue();
            try {
                if (allDone.await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                    logger.warn("Timed out waiting for parallel parsing threads to finish after [{}]s", CLOSE_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainQueue();
        }

        private void drainQueue() {
            Page p;
            while ((p = sharedQueue.poll()) != null) {
                p.releaseBlocks();
            }
        }
    }
}
