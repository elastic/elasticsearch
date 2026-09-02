/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
     * {@code external_max_concurrent_open_segments} pragma (tests and internal callers). Sourced from the single
     * source of truth {@link SourceOperatorContext#DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS}.
     */
    static final int DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS = SourceOperatorContext.DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS;

    private ParallelParsingCoordinator() {}

    private static SegmentableFormatReader createSegmentableReader(
        FormatReaderFactory factory,
        Settings settings,
        @Nullable BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        FormatReadContext.Binding binding
    ) {
        if (factory.create(settings, blockFactory, config, binding) instanceof SegmentableFormatReader segmentable) {
            return segmentable;
        }
        throw new IllegalStateException("format factory [" + factory.formatName() + "] is not segmentable");
    }

    /** Convenience overload for tests and internal callers using default execution settings. */
    public static CloseableIterator<Page> parallelRead(
        FormatReaderFactory factory,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor
    ) throws IOException {
        return parallelRead(
            factory,
            Settings.EMPTY,
            BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("parallel-parse")).build(),
            null,
            FormatReadContext.Binding.empty(),
            storageObject,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            null,
            false,
            true,
            null,
            0L,
            DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            -1L,
            StripeColumnScope.PROJECTED,
            false,
            ExternalSourceMetrics.NOOP,
            null
        );
    }

    /**
     * Creates a parallel-parsing iterator over a single storage object.
     * <p>
     * The file is divided into {@code parallelism} segments at record boundaries.
     * Each segment is parsed independently and pages are yielded as they become ready
     * (completion order, not segment order). If the file is too small for meaningful
     * parallelism (below the reader's {@link SegmentableFormatReader#minimumSegmentSize()}
     * per segment), falls back to single-threaded reading.
     *
     * @param factory           the format factory; each worker creates and closes its own reader
     * @param storageObject     the file to read
     * @param projectedColumns  columns to project
     * @param batchSize         rows per page
     * @param parallelism       number of parallel parser threads
     * @param executor          executor for parser threads
     * @return an iterator that yields pages as they become ready (cross-segment row order not preserved)
     */
    public static CloseableIterator<Page> parallelRead(
        FormatReaderFactory factory,
        Settings settings,
        @Nullable BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        FormatReadContext.Binding binding,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        boolean splitStartsAtRecordBoundary,
        boolean splitIncludesFileLeader,
        List<Attribute> readSchema,
        long baseFileOffset,
        int maxConcurrentOpenSegments,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
        int maxRecordBytes,
        long statsStripeSize,
        StripeColumnScope statsColumnScope,
        boolean splitIsFileFinal,
        ExternalSourceMetrics metrics,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        long fileLength = storageObject.length();
        long minSegment = factory.minimumSegmentSize(config);
        FormatReadContext.Binding workerBinding = binding != null ? binding : FormatReadContext.Binding.empty();
        if (projectedColumns != null && projectedColumns.isEmpty() && splitIncludesFileLeader) {
            SegmentableFormatReader metadataReader = createSegmentableReader(factory, settings, blockFactory, config, workerBinding);
            try {
                var metadata = metadataReader.metadata(storageObject);
                if (metadata != null && metadata.schema() != null && metadata.schema().isEmpty() == false) {
                    workerBinding = workerBinding.withBoundSchema(metadata.schema());
                }
            } finally {
                ReaderReleasingIterator.closeReader(metadataReader);
            }
        }

        ErrorPolicy effectivePolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.STRICT;
        FormatReadContext baseCtx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .errorPolicy(effectivePolicy)
            .firstSplit(splitIncludesFileLeader)
            .recordAligned(splitStartsAtRecordBoundary)
            .readSchema(readSchema)
            .splitStartByte(baseFileOffset)
            .maxRecordBytes(maxRecordBytes)
            .stats(baseFileOffset, statsStripeSize, splitIsFileFinal)
            .statsColumnScope(statsColumnScope)
            .informationalWarningSink(warningSink)
            .build();
        if (parallelism <= 1 || fileLength < minSegment * 2) {
            SegmentableFormatReader reader = createSegmentableReader(factory, settings, blockFactory, config, workerBinding);
            boolean handedOff = false;
            try {
                CloseableIterator<Page> pages = ReaderReleasingIterator.wrap(reader.read(storageObject, baseCtx), reader);
                handedOff = true;
                return pages;
            } finally {
                if (handedOff == false) {
                    ReaderReleasingIterator.closeReader(reader);
                }
            }
        }

        List<long[]> segments = computeSegments(factory, config, storageObject, fileLength, parallelism, minSegment, maxRecordBytes);
        if (segments.size() <= 1) {
            SegmentableFormatReader reader = createSegmentableReader(factory, settings, blockFactory, config, workerBinding);
            boolean handedOff = false;
            try {
                CloseableIterator<Page> pages = ReaderReleasingIterator.wrap(reader.read(storageObject, baseCtx), reader);
                handedOff = true;
                return pages;
            } finally {
                if (handedOff == false) {
                    ReaderReleasingIterator.closeReader(reader);
                }
            }
        }

        AsReadyParallelIterator iterator = new AsReadyParallelIterator(
            factory,
            settings,
            blockFactory,
            config,
            workerBinding,
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
            baseFileOffset,
            captureSink,
            maxRecordBytes,
            statsStripeSize,
            statsColumnScope,
            splitIsFileFinal,
            metrics,
            warningSink
        );
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
        FormatReaderFactory factory,
        StorageObject storageObject,
        long fileLength,
        int parallelism,
        long minSegment
    ) throws IOException {
        return computeSegments(
            factory,
            null,
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
        FormatReaderFactory factory,
        StorageObject storageObject,
        long fileLength,
        int parallelism,
        long minSegment,
        int maxRecordBytes
    ) throws IOException {
        return computeSegments(factory, null, storageObject, fileLength, parallelism, minSegment, maxRecordBytes);
    }

    /**
     * Computes byte-range segments using a splitter capped by {@code maxRecordBytes}.
     */
    public static List<long[]> computeSegments(
        FormatReaderFactory factory,
        @Nullable Map<String, Object> config,
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

        RecordSplitter splitter = factory.recordSplitter(config, maxRecordBytes);
        boolean strided = splitter.supportsStridedProbing();
        boolean proven = splitter.supportsProvenProbing();
        // Strided segmentation probes record boundaries at arbitrary mid-file offsets, which is only correct
        // when a raw newline is unambiguously a record terminator. A non-strided splitter (quoted/escaped
        // CSV/TSV) can still be segmented when it supports proven probing: a macro-split range starts at a proven
        // record boundary, so exactCursor seeds at the range start (offset 0). A splitter that is neither must
        // have been routed to the whole-file sequential path upstream; if one reaches here the routing is broken,
        // so fail loud rather than produce wrong segments.
        if (strided == false && proven == false) {
            throw new IllegalStateException(
                "record splitter ["
                    + splitter.getClass().getName()
                    + "] supports neither strided nor proven probing and cannot be segmented"
            );
        }
        // Both walks are sequential, so either resolves every boundary in one call. Segmentation runs on the
        // thread that already carries the read's StorageRetryCancellation scope. The walks read that scope
        // through StorageRetryCancellation::isCancelled; they do not install a nested one, which would
        // replace the read's live signal. A probe parked in retry/throttle backoff still sees the same
        // signal through the ambient scope.
        List<Long> boundaries;
        if (strided) {
            // The offsets of a nominal-size grid, resumed from each boundary rather than walked blind. Both
            // walks read the split at most once, but a blind grid buys that by capping every window at the
            // stride, which is also a ceiling on the records it can resolve: a file whose records outgrow a
            // segment would be cut into far fewer pieces than its offsets asked for, and one whose record width
            // divides the stride would not be cut at all. Resuming gets the same bound from the offsets being
            // monotonic, so its probes can open the record cap instead. The concurrency a blind grid's
            // independent offsets allow is no loss here: these probes run on the calling thread either way.
            boundaries = RecordBoundaryProbe.advancingBoundaries(
                splitter,
                storageObject,
                fileLength,
                nominalSize,
                minSegment,
                maxRecordBytes,
                StorageRetryCancellation::isCancelled
            );
        } else {
            boundaries = RecordBoundaryProbe.provenBoundaries(
                splitter,
                storageObject,
                fileLength,
                nominalSize,
                minSegment,
                StorageRetryCancellation::isCancelled
            ).boundaries();
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
         * Per-open-segment page budget. Before this rewrite each open segment had its own 16-deep page queue;
         * the single shared queue keeps the same per-segment depth so the total buffered pages stay roughly
         * {@code maxConcurrentSegments * PAGES_PER_OPEN_SEGMENT}, preserving the same backpressure.
         */
        private static final int PAGES_PER_OPEN_SEGMENT = 16;
        private final FormatReaderFactory factory;
        private final Settings settings;
        @Nullable
        private final BlockFactory blockFactory;
        @Nullable
        private final Map<String, Object> config;
        private final FormatReadContext.Binding binding;
        private final StorageObject storageObject;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final ErrorPolicy errorPolicy;
        private final boolean splitIncludesFileLeader;
        @Nullable
        private final List<Attribute> readSchema;
        private final long baseFileOffset;
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
        /** Canonical-stripe grid for per-stripe stats attribution ({@code <= 0} disables). Pure stats overlay. */
        private final long statsStripeSize;
        /** How much per-stripe statistics each segment harvests (row count only / + projected / + all / nothing). */
        private final StripeColumnScope statsColumnScope;
        /**
         * Whether this storage object is the file's final split. Only then may the trailing segment mark its
         * last stripe file-final ({@code eof}); a mid-file macro-split's trailing segment ends at the range
         * boundary, and a later split supplies the terminal stripe — so it must never be marked file-final.
         */
        private final boolean splitIsFileFinal;
        /** Node telemetry sink for the {@code reader.pool.rejected} event; {@link ExternalSourceMetrics#NOOP} when unwired. */
        private final ExternalSourceMetrics metrics;
        /**
         * Relay for client-visible {@link SkipWarnings} messages raised while parsing a segment; see
         * {@link #parallelRead}'s {@code warningSink} parameter. {@code null} falls back to a direct
         * {@link org.elasticsearch.common.logging.HeaderWarning} call on the segment worker thread.
         */
        @Nullable
        private final Consumer<String> warningSink;

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
        private final AtomicBoolean statsPoisoned = new AtomicBoolean(false);
        /** Counts segments still running. Reaches 0 when every worker has finished (success or failure). */
        private final AtomicInteger remainingSegments;
        private final CountDownLatch allDone;

        // Volatile so close() — which may run on a different thread than the consumer that drove hasNext() —
        // reads the parked page rather than a stale value, so its blocks are released rather than leaked.
        private volatile Page buffered = null;
        private volatile boolean closed = false;

        /**
         * Async-ready signal, mirroring {@code StreamingParallelIterator}. {@code null} when no consumer is
         * parked. When {@link #waitForReady()} can't satisfy synchronously it installs a fresh listener here;
         * the parser workers fire it on every event that can transition the iterator to a ready state (a page
         * enqueued on the shared queue, a segment finishing, an error recorded, or close). Single-shot: after
         * firing it is cleared and lazily replaced by the next {@code waitForReady}. Without it the default
         * immediately-ready signal drops {@code drainHotPath} straight into {@link #hasNext()}, whose
         * {@link #takeNextPage()} then blocks a scarce consumer-pool thread for the whole segment read.
         */
        private final AtomicReference<SubscribableListener<Void>> pendingReady = new AtomicReference<>();

        AsReadyParallelIterator(
            FormatReaderFactory factory,
            Settings settings,
            @Nullable BlockFactory blockFactory,
            @Nullable Map<String, Object> config,
            FormatReadContext.Binding binding,
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
            long baseFileOffset,
            @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
            int maxRecordBytes,
            long statsStripeSize,
            StripeColumnScope statsColumnScope,
            boolean splitIsFileFinal,
            ExternalSourceMetrics metrics,
            @Nullable Consumer<String> warningSink
        ) {
            this.factory = factory;
            this.settings = settings;
            this.blockFactory = blockFactory;
            this.config = config;
            this.binding = binding != null ? binding : FormatReadContext.Binding.empty();
            this.storageObject = storageObject;
            this.splitIsFileFinal = splitIsFileFinal;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.errorPolicy = errorPolicy;
            this.splitIncludesFileLeader = splitIncludesFileLeader;
            this.readSchema = readSchema;
            this.baseFileOffset = baseFileOffset;
            this.captureSink = captureSink;
            this.maxRecordBytes = maxRecordBytes;
            this.statsStripeSize = statsStripeSize;
            this.statsColumnScope = statsColumnScope != null ? statsColumnScope : StripeColumnScope.PROJECTED;
            this.metrics = metrics == null ? ExternalSourceMetrics.NOOP : metrics;
            this.warningSink = warningSink;
            this.segments = segments;
            this.executor = executor;
            this.maxConcurrentSegments = Math.max(1, Math.min(maxConcurrentOpenSegments, Math.min(parallelism, segments.size())));
            this.remainingSegments = new AtomicInteger(segments.size());
            this.allDone = new CountDownLatch(segments.size());
            this.sharedQueue = new LinkedBlockingQueue<>(Math.max(1, maxConcurrentSegments * PAGES_PER_OPEN_SEGMENT));
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
                    // Best-effort telemetry: the parser pool refused this segment (saturated / shutting down). The
                    // record method self-guards, so no inner try/catch is needed here.
                    metrics.recordPoolRejected();
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
                readSegment(segmentIndex, offset, length);
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
         * Reads one segment and emits its pages as they become ready. A transient transport fault during the
         * read (connection reset, premature end of body) is recovered <em>beneath</em> this read by the
         * self-healing storage stream, which re-opens the byte range and resumes; so this simply reads
         * complete pages. A genuine data/parse error propagates and fails the query.
         */
        private void readSegment(int segmentIndex, long offset, long length) throws Exception {
            boolean lastSplit = segmentIndex == segments.size() - 1;
            StorageObject segObj = new RangeStorageObject(storageObject, offset, length);
            // Absolute file offset of this segment's first byte: segment offsets are relative to this
            // (possibly macro-split) storage object, so add its base file offset. The reader uses it to
            // attribute each record to its canonical stripe — a pure stats overlay; this seekable path
            // gets stripe-addressed stats for free, with no change to how segments are computed or read.
            // This ONE value feeds both splitStartByte and the stats attribution base — they are the same
            // quantity, so derive it once. (Deriving the stats base separately from RangeStorageObject.offset()
            // is equal today but would silently make CSV and NDJSON attribute the same records to different
            // grids if a future caller's baseFileOffset ever diverged from the range offset.)
            long segmentFileOffset = baseFileOffset + offset;
            // statsFileFinal: the trailing segment reaches the file's true end only when (a) it is this storage
            // object's last segment AND (b) this storage object is the file's final split (splitIsFileFinal).
            // A mid-file macro-split's trailing segment ends at the range boundary, so a later macro-split
            // supplies the terminal stripe — never mark it file-final (it would mark a mid-file stripe complete
            // and silently undercount). The file's final macro-split DOES reach EOF, so its trailing segment is
            // file-final and the byte-range cover can close the last stripe.
            boolean statsFileFinal = lastSplit && splitIsFileFinal;

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
                .splitStartByte(segmentFileOffset)
                .maxRecordBytes(maxRecordBytes)
                .stats(segmentFileOffset, statsStripeSize, statsFileFinal)
                .statsColumnScope(statsColumnScope)
                .informationalWarningSink(warningSink)
                .build();

            // Bind the consumer-owned sink on this worker so the reader's close hook (which publishes its
            // per-stripe _stats.* contributions via ExternalStatsCapture.record) reaches the same map the
            // consumer-thread StatsCapturingIterator binds — ExternalStatsCapture.ACTIVE is a plain
            // ThreadLocal that does not propagate to executor threads. The pages iterator is opened *inside*
            // the bound's try-with-resources so a failing reader.read still restores the previous binding —
            // worker threads are reused across queries by the shared executor, and a leaked binding would
            // poison subsequent tasks. The inner stream closes first, so the close hook's record() call runs
            // with the sink still bound; then the handle restores the previous binding. The reader stamps
            // stripe addressing itself, so the sink no longer carries a coverage.
            ExternalStatsCapture.Handle bound = captureSink != null ? ExternalStatsCapture.bind(captureSink) : () -> {};
            SegmentableFormatReader workerReader = createSegmentableReader(factory, settings, blockFactory, config, binding);
            try (bound) {
                try (CloseableIterator<Page> pages = workerReader.read(segObj, ctx)) {
                    while (pages.hasNext()) {
                        if (firstError.get() != null || closed) {
                            break;
                        }
                        enqueueOrRelease(pages.next());
                    }
                }
            } finally {
                ReaderReleasingIterator.closeReader(workerReader);
            }
        }

        /**
         * Hands {@code page} to the consumer: offers it to the shared queue, blocking with a timeout for
         * backpressure until it is accepted, or releases it (and returns) if the iterator was closed or an
         * error flipped before it could be enqueued. Either way the page is accounted for — enqueued and
         * owned by the consumer, or released here — so the caller never has to release it.
         */
        private void enqueueOrRelease(Page page) throws InterruptedException {
            while (true) {
                if (closed || firstError.get() != null) {
                    page.releaseBlocks();
                    return;
                }
                if (sharedQueue.offer(page, 500, TimeUnit.MILLISECONDS)) {
                    // Wake any consumer parked on waitForReady(): a page is now available at the queue head.
                    signalReady();
                    return;
                }
            }
        }

        /**
         * Marks one segment finished. The consumer does not need an explicit wake-up: {@link #takeNextPage()}
         * polls the shared queue with a 200ms timeout and re-checks the completion counter on every wake-up,
         * so it observes the final segment finishing within one poll interval.
         */
        private void finishSegment() {
            int remaining = remainingSegments.decrementAndGet();
            if (remaining == 0 && closed) {
                drainQueue();
            }
            // Publish completion only after the final worker has performed close-time queue cleanup.
            // Otherwise close() can return from await() while the last queued page still owns breaker bytes.
            allDone.countDown();
            // Only wake a parked consumer when this completion actually flips isReadyNow() to true: an error
            // was recorded, or this was the last segment and the queue is now drained (terminal EOF). A
            // mid-parse segment finishing with pages still to come leaves isReadyNow() false, so signalling
            // here would spuriously complete the listener and drop drainHotPath into a blocking hasNext() —
            // exactly the starvation this override removes. Enqueued pages are woken by enqueueOrRelease's
            // own signalReady(), so no wake-up is lost. Mirrors StreamingParallelIterator's task-exit gate.
            if (firstError.get() != null || (remaining == 0 && sharedQueue.isEmpty())) {
                signalReady();
            }
        }

        /**
         * Returns {@code true} when {@link #hasNext()} can run without blocking on a segment read: a page is
         * already buffered or sitting at the head of the shared queue, an error was recorded, the iterator is
         * closed, or every segment has finished and the queue is drained (terminal EOF). Otherwise segments
         * are still parsing with nothing yet enqueued, so {@link #hasNext()} would block in
         * {@link #takeNextPage()} — the consumer should park on {@link #waitForReady()} instead.
         */
        private boolean isReadyNow() {
            return buffered != null
                || closed
                || firstError.get() != null
                || sharedQueue.peek() != null
                || (remainingSegments.get() == 0 && sharedQueue.isEmpty());
        }

        @Override
        public SubscribableListener<Void> waitForReady() {
            if (isReadyNow()) {
                return SubscribableListener.newSucceeded(null);
            }
            // Install a listener for the next state-change event (page enqueued, segment finished, EOF, error,
            // close). Re-check after the CAS to close the gap where state flipped to ready between the first
            // isReadyNow() call and the install — mirrors StreamingParallelIterator.waitForReady().
            SubscribableListener<Void> existing = pendingReady.get();
            if (existing != null) {
                return existing;
            }
            SubscribableListener<Void> fresh = new SubscribableListener<>();
            if (pendingReady.compareAndSet(null, fresh) == false) {
                return pendingReady.get();
            }
            if (isReadyNow()) {
                signalReady();
                return SubscribableListener.newSucceeded(null);
            }
            return fresh;
        }

        /**
         * Fires the pending readiness listener (if any). Parser workers call this from every state-change site
         * so a consumer parked on {@link #waitForReady()} resumes promptly.
         */
        private void signalReady() {
            SubscribableListener<Void> listener = pendingReady.getAndSet(null);
            if (listener != null) {
                listener.onResponse(null);
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
                throw new NoSuchElementException();
            }
            Page result = buffered;
            buffered = null;
            return result;
        }

        @Override
        public Page tryAdvance() {
            if (closed) {
                return null;
            }
            if (buffered != null) {
                Page result = buffered;
                buffered = null;
                return result;
            }
            checkError();
            return sharedQueue.poll();
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
                // EOF-first: once every segment has finished and the queue is drained no page will ever
                // arrive, so return without burning a residual 200ms poll. This is the same terminal
                // condition the post-poll branch below checks; hoisting it only removes the final idle wait.
                // The poll below remains the termination guarantee for the racing case where the last
                // segment finishes just after this check but before a page we still need to drain. No
                // checkError() here: the top-of-loop call just ran and nothing yields in between.
                if (remainingSegments.get() == 0 && sharedQueue.isEmpty()) {
                    return null;
                }
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
                throw ExternalFailures.surface(t, "Parallel parsing failed");
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            // Decide clean completion before flipping closed: no error, every segment finished
            // (remainingSegments == 0), and the consumer drained the shared queue with no page parked.
            // An early close (LIMIT, cancellation) leaves a segment cut off mid-parse — a partial row
            // count under that segment's full byte range — which the coverage tiling could otherwise
            // accept as complete and cache as an under-count. So a non-clean scan poisons the file.
            boolean cleanCompletion = firstError.get() == null && remainingSegments.get() == 0 && sharedQueue.isEmpty() && buffered == null;
            closed = true;
            // Wake any consumer parked on waitForReady(); isReadyNow() now returns true on closed.
            signalReady();
            // Release the page parked by a hasNext() with no following next(); drainQueue() only sees the shared
            // queue, so without this its Blocks leak against the breaker on every early close.
            if (buffered != null) {
                buffered.releaseBlocks();
                buffered = null;
            }
            drainQueue();
            try {
                if (allDone.await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                    logger.warn("Timed out waiting for parallel parsing threads to finish after [{}]s", CLOSE_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainQueue();
            // Coverage tiling at the reconciler establishes completeness for a clean scan; an early/error
            // close can leave a segment with a partial count under a full range, so discard the file's
            // contributions when the scan did not drain cleanly.
            if (cleanCompletion == false && captureSink != null) {
                if (statsPoisoned.compareAndSet(false, true)) {
                    Map<String, Object> poison = new HashMap<>();
                    poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
                    ExternalStatsCapture.record(captureSink, storageObject.path().toString(), poison);
                }
            }
        }

        private void drainQueue() {
            Page p;
            while ((p = sharedQueue.poll()) != null) {
                p.releaseBlocks();
            }
        }
    }
}
