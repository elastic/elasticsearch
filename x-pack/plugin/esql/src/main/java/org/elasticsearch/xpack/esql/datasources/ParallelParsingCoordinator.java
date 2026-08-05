/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
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
        return parallelRead(reader, storageObject, projectedColumns, batchSize, parallelism, executor, null, false, true, null, 0L);
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
        return parallelRead(reader, storageObject, projectedColumns, batchSize, parallelism, executor, errorPolicy, false, true, null, 0L);
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
            null,
            0L
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
            null,
            0L
        );
    }

    /**
     * Forwards to the full-control overload with the {@link #DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS
     * default} open-segment cap and {@code captureSink=null} (text-format readers' close hooks
     * then publish into whatever {@link ExternalStatsCapture} sink — if any — happens to be bound
     * on the calling thread). Callers that resolve the {@code max_concurrent_open_segments} pragma
     * or care about cross-thread stats capture use the 12-arg overload.
     *
     * @param readSchema     planner-bound read schema, or {@code null} for per-file inference
     * @param baseFileOffset file-global byte offset of {@code storageObject}'s first byte (i.e. the macro
     *                       split's {@code FileSplit.offset()}). {@code 0} for whole-file reads. Added to each
     *                       segment's split-relative offset so readers emit file-global, split-invariant
     *                       record positions on the {@code _rowPosition} channel.
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
        long baseFileOffset
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
            baseFileOffset,
            DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            -1L,
            StripeColumnScope.PROJECTED,
            // No stripe addressing on this convenience path (stripeSize == -1), so file-final is moot;
            // conservatively false preserves the prior never-file-final-for-macro-splits behavior.
            false
        );
    }

    /**
     * Convenience overload that adds the {@code max_concurrent_open_segments} cap without per-chunk
     * source-stats capture (equivalent to passing {@code captureSink == null}).
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
            null
        );
    }

    /**
     * Full-control overload that takes both the {@code max_concurrent_open_segments} cap and an
     * explicit {@code captureSink} for per-chunk source-stats contributions.
     * <p>
     * {@code maxConcurrentOpenSegments} is the per-file limit on byte-range segments whose read
     * streams are open at once. A sliding window dispatches at most {@code maxConcurrentOpenSegments}
     * segments at a time; each completion opens the next. This caps the open-stream / buffer count
     * independent of file count and length. See {@link AsReadyParallelIterator}.
     * <p>
     * Each segment is parsed on a worker thread; the worker binds {@code captureSink} around the
     * per-segment {@link CloseableIterator#close()} so text-format readers' close hooks publish their
     * {@code _stats.*} contribution into the same sink the consumer-thread wrapper sees. Pass
     * {@code null} when no capture is desired (tests, benchmarks).
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
            0L,
            maxConcurrentOpenSegments,
            captureSink,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            -1L,
            StripeColumnScope.PROJECTED,
            // No stripe addressing on this convenience path (stripeSize == -1), so file-final is moot.
            false
        );
    }

    /**
     * Full-control overload that also takes the {@code max_record_size} cap used by record splitters,
     * the file-global byte base offset, and the canonical-stripe grid for per-stripe stats attribution
     * ({@code <= 0} disables; pure overlay).
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
        long baseFileOffset,
        int maxConcurrentOpenSegments,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
        int maxRecordBytes,
        long statsStripeSize,
        StripeColumnScope statsColumnScope,
        boolean splitIsFileFinal
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
            baseFileOffset,
            maxConcurrentOpenSegments,
            captureSink,
            maxRecordBytes,
            statsStripeSize,
            statsColumnScope,
            splitIsFileFinal,
            ExternalSourceMetrics.NOOP
        );
    }

    /**
     * As the {@code captureSink}+{@code maxRecordBytes} overload, plus a node telemetry sink used to record a
     * {@code reader.pool.rejected} event when a parser-segment submission is rejected (executor saturated /
     * shutting down). Pass {@link ExternalSourceMetrics#NOOP} to disable (all narrower overloads do).
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
        long baseFileOffset,
        int maxConcurrentOpenSegments,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
        int maxRecordBytes,
        long statsStripeSize,
        StripeColumnScope statsColumnScope,
        boolean splitIsFileFinal,
        ExternalSourceMetrics metrics
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
            baseFileOffset,
            maxConcurrentOpenSegments,
            captureSink,
            maxRecordBytes,
            statsStripeSize,
            statsColumnScope,
            splitIsFileFinal,
            metrics,
            null
        );
    }

    /**
     * As the {@code metrics} overload, plus a relay for client-visible lenient-policy warnings raised
     * while parsing a segment (see {@link SkipWarnings}). Segment parsing runs on {@code executor}
     * worker threads, so a direct {@link org.elasticsearch.common.logging.HeaderWarning} call there
     * would attach to the wrong thread's response headers and be dropped; production passes
     * {@code AsyncExternalSourceBuffer::recordInformationalWarning} so the operator can re-emit it on
     * the driver thread without flipping the response's {@code is_partial} flag (these are per-record
     * skip/null-fill warnings, not a truncation of the whole read — see
     * {@code AsyncExternalSourceBuffer#recordWarning} for that case). Pass {@code null} to fall back to
     * a direct {@code HeaderWarning} call on the parsing thread (tests, benchmarks).
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
            // lastSplit is deliberately left to the builder default (true) and not set from splitIsFileFinal.
            // This context is used two ways: as the single-threaded fallback below, which reads the whole
            // storage object and so genuinely owns its trailing bytes, and as the base for per-segment
            // contexts, which each set lastSplit themselves from their segment index. Setting it here from
            // splitIsFileFinal breaks the first use, because the convenience overloads that assume a
            // whole-file read (splitIncludesFileLeader=true) do not make the matching claim about the
            // file's end. Reconciling those defaults is worth doing on its own, not as a side effect.
            .recordAligned(splitStartsAtRecordBoundary)
            .readSchema(readSchema)
            .splitStartByte(baseFileOffset)
            .maxRecordBytes(maxRecordBytes)
            // Single-segment fallback reads this whole storage object in one shot, so its trailing stripe is
            // file-final exactly when this macro-split is the file's final split (splitIsFileFinal). A mid-file
            // macro-split is never file-final — a later split supplies the terminal stripe.
            .stats(baseFileOffset, statsStripeSize, splitIsFileFinal)
            .statsColumnScope(statsColumnScope)
            .informationalWarningSink(warningSink)
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
            baseFileOffset,
            captureSink,
            maxRecordBytes,
            statsStripeSize,
            statsColumnScope,
            splitIsFileFinal,
            metrics,
            warningSink
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
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);

        // The last proven record start (range-relative), the base the exact walk streams from on AMBIGUOUS.
        long exactCursor = 0L;
        long pos = nominalSize;
        while (pos < fileLength) {
            long remaining = fileLength - pos;
            if (remaining < minSegment) {
                break;
            }
            long boundary;
            if (strided) {
                InputStream stream = storageObject.newStream(pos, remaining);
                // Abort rather than close: findNextRecordBoundary reads only a prefix of the range
                // (fileLength - pos bytes), but close() on providers like S3 drains the remainder.
                try (Closeable abortOnExit = () -> storageObject.abortStream(stream)) {
                    long skipped = splitter.findNextRecordBoundary(stream);
                    if (skipped < 0) {
                        break;
                    }
                    boundary = pos + skipped;
                }
            } else {
                long probed;
                InputStream probeStream = storageObject.newStream(pos, remaining);
                try (Closeable abortOnExit = () -> storageObject.abortStream(probeStream)) {
                    probed = splitter.findProvenRecordBoundary(probeStream);
                }
                if (probed >= 0) {
                    boundary = pos + probed;
                } else if (probed == RecordSplitter.AMBIGUOUS) {
                    // Exact walk from the last proven boundary. This path is range-bounded and read at planning
                    // time, so it relies on the existing read-time cancellation rather than a new supplier.
                    long walkRemaining = fileLength - exactCursor;
                    InputStream walkStream = storageObject.newStream(exactCursor, walkRemaining);
                    long start;
                    try (Closeable abortOnExit = () -> storageObject.abortStream(walkStream)) {
                        start = splitter.findRecordStartAtOrAfter(walkStream, pos - exactCursor, () -> false);
                    }
                    if (start == RecordSplitter.RECORD_TOO_LARGE || start < 0) {
                        break;
                    }
                    boundary = exactCursor + start;
                } else {
                    // findProvenRecordBoundary only ever returns a boundary (>= 0) or AMBIGUOUS.
                    assert false : "findProvenRecordBoundary returned an unexpected sentinel: " + probed;
                    break;
                }
            }
            if (boundary >= fileLength) {
                break;
            }
            if (fileLength - boundary < minSegment) {
                break;
            }
            assert boundary > boundaries.get(boundaries.size() - 1) : "macro-split boundary must be strictly increasing";
            boundaries.add(boundary);
            exactCursor = boundary;
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
         * Per-open-segment page budget. Before this rewrite each open segment had its own 16-deep page queue;
         * the single shared queue keeps the same per-segment depth so the total buffered pages stay roughly
         * {@code maxConcurrentSegments * PAGES_PER_OPEN_SEGMENT}, preserving the same backpressure.
         */
        private static final int PAGES_PER_OPEN_SEGMENT = 16;
        private final SegmentableFormatReader reader;
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
            long baseFileOffset,
            @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
            int maxRecordBytes,
            long statsStripeSize,
            StripeColumnScope statsColumnScope,
            boolean splitIsFileFinal,
            ExternalSourceMetrics metrics,
            @Nullable Consumer<String> warningSink
        ) {
            this.reader = reader;
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
            try (bound) {
                try (CloseableIterator<Page> pages = reader.read(segObj, ctx)) {
                    while (pages.hasNext()) {
                        if (firstError.get() != null || closed) {
                            break;
                        }
                        enqueueOrRelease(pages.next());
                    }
                }
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
            allDone.countDown();
            remainingSegments.decrementAndGet();
            // Only wake a parked consumer when this completion actually flips isReadyNow() to true: an error
            // was recorded, or this was the last segment and the queue is now drained (terminal EOF). A
            // mid-parse segment finishing with pages still to come leaves isReadyNow() false, so signalling
            // here would spuriously complete the listener and drop drainHotPath into a blocking hasNext() —
            // exactly the starvation this override removes. Enqueued pages are woken by enqueueOrRelease's
            // own signalReady(), so no wake-up is lost. Mirrors StreamingParallelIterator's task-exit gate.
            if (firstError.get() != null || (remainingSegments.get() == 0 && sharedQueue.isEmpty())) {
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
                Map<String, Object> poison = new HashMap<>();
                poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
                ExternalStatsCapture.record(storageObject.path().toString(), poison);
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
