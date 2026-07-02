/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.logging.HeaderWarning;
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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Coordinates parallel parsing of a sequential (non-seekable) decompressed stream
 * by chunking it at record boundaries and dispatching each chunk to a parser thread.
 * <p>
 * This is the streaming counterpart to {@link ParallelParsingCoordinator}, designed for
 * stream-only compression codecs (gzip, zstd without seekable frames) where the
 * decompressed data is only available as a sequential {@link InputStream}.
 * <p>
 * Architecture:
 * <ol>
 *   <li>A segmentator thread reads the decompressed stream into byte buffers from a pool</li>
 *   <li>Each buffer is split at the last record boundary to form a complete chunk</li>
 *   <li>Chunks are dispatched to parser threads via a bounded queue</li>
 *   <li>Parser threads parse each chunk independently using the format reader</li>
 *   <li>Results are yielded in chunk order via per-slot page queues</li>
 * </ol>
 * <p>
 * Shutdown blocking sites: when the iterator's {@code close()} is invoked the segmentator may
 * be parked on (a) the upstream {@link InputStream#read(byte[], int, int)}, (b) {@code bufferPool.take()},
 * (c) {@code chunkQueue.put()}, or (d) {@code dispatchPermits.acquire()}. Close sets
 * {@code closed=true}, releases one permit on {@code dispatchPermits} (covers (d)), and drains
 * both the chunk queue and page queues (covers (b) and (c) by freeing slots so {@code put}/{@code take}
 * either succeeds or completes after the post-acquire {@code closed} re-check). Case (a) is the
 * responsibility of the upstream stream wrapper — most stream-only codecs return on close; if the
 * upstream blocks indefinitely on read, close will time out after the iterator's close-timeout and
 * log a warning.
 */
public final class StreamingParallelParsingCoordinator {

    private static final Logger logger = LogManager.getLogger(StreamingParallelParsingCoordinator.class);

    private StreamingParallelParsingCoordinator() {}

    /**
     * Creates a parallel-parsing iterator over a sequential decompressed stream.
     *
     * @param reader              the segmentable format reader (provides record boundary semantics)
     * @param decompressedStream  the sequential decompressed input stream
     * @param projectedColumns    columns to project
     * @param batchSize           rows per page
     * @param parallelism         number of parallel parser threads
     * @param executor            executor for segmentator and parser threads
     * @param errorPolicy         error handling policy
     * @return an iterator that yields pages in chunk order
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        InputStream decompressedStream,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy
    ) throws IOException {
        return parallelRead(
            reader,
            decompressedStream,
            null,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            errorPolicy,
            null,
            0L,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            null,
            null
        );
    }

    /**
     * Variant that propagates the planner-resolved {@code readSchema}. Mirrors the same parameter on
     * {@link ParallelParsingCoordinator#parallelRead}; the streaming path must thread it so multi-file
     * globs over gzip/zstd/bz2 inputs honor the planner's typing instead of re-inferring per file.
     * Pass {@code null} when no read schema is bound.
     *
     * @param baseFileOffset file-global byte offset added to each chunk's decompressed start byte before it
     *                       is handed to the reader as {@link FormatReadContext#splitStartByte()}. Stream-only
     *                       compressed inputs are not macro-split, so this is {@code 0}; the decompressed
     *                       cumulative offset is the logical file-global offset on its own.
     * <p>
     * Full-control overload that takes both the {@code max_record_size} grow-loop bound and an
     * explicit consumer-owned {@code captureSink} for per-chunk source-stats contributions. Each
     * chunk is parsed on a worker thread; this coordinator binds {@code captureSink} on that worker
     * around the per-chunk {@link CloseableIterator#close()} so text-format readers' close hooks
     * publish into the same map the consumer-thread wrapper sees. Pass {@code null} for the sink
     * when no capture is desired (tests, benchmarks). When {@code storageObject} is non-null its path
     * is stamped on every chunk and each chunk carries its decompressed-stream coverage range, so the
     * coordinator reconciler can union the chunks under the real file key (see
     * {@link ParallelParsingCoordinator}).
     * <p>
     * {@code partialResultsWarningSink} receives a single client-visible message if a non-strict
     * {@link ErrorPolicy} truncates the read at a {@code max_record_size} cap-hit. Production passes
     * {@link AsyncExternalSourceBuffer#recordWarning} so the operator can re-emit it on the driver
     * thread (the segmentator runs on a forked worker whose response headers never reach the client —
     * see #835). Pass {@code null} to fall back to a direct {@link HeaderWarning} on the current thread
     * (tests, benchmarks).
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        InputStream decompressedStream,
        @Nullable StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy,
        @Nullable List<Attribute> readSchema,
        long baseFileOffset,
        int maxRecordBytes,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
        @Nullable Consumer<String> partialResultsWarningSink
    ) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "streaming parallelRead: readSchema={}, parallelism={}, projection={}",
                readSchema == null ? "null" : "present(" + readSchema.size() + ")",
                parallelism,
                projectedColumns == null ? "null" : projectedColumns.size()
            );
        }
        ErrorPolicy effectivePolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.STRICT;

        if (parallelism <= 1) {
            FormatReadContext ctx = FormatReadContext.builder()
                .projectedColumns(projectedColumns)
                .batchSize(batchSize)
                .errorPolicy(effectivePolicy)
                .readSchema(readSchema)
                .splitStartByte(baseFileOffset)
                .maxRecordBytes(maxRecordBytes)
                .build();
            return reader.read(new InputStreamStorageObject(decompressedStream), ctx);
        }

        return new StreamingParallelIterator(
            reader,
            decompressedStream,
            storageObject,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            effectivePolicy,
            readSchema,
            baseFileOffset,
            maxRecordBytes,
            captureSink,
            partialResultsWarningSink
        );
    }

    // Package-private so close-path tests can assert on segmentator wait state via
    // isSegmentatorParkedOnDispatchPermits(); production callers see only CloseableIterator<Page>.
    static final class StreamingParallelIterator implements CloseableIterator<Page> {

        private static final Page POISON = new Page(0);
        private static final long CLOSE_TIMEOUT_SECONDS = 60;

        /**
         * The reader used by parser threads. Initially the caller-supplied reader; the segmentator
         * thread replaces it with a schema-bound variant after inferring the schema from the first
         * chunk (see {@link #runSegmentator}). The replacement happens-before the first chunk is
         * dispatched (via the {@link ArrayBlockingQueue} synchronization on {@link #chunkQueue}),
         * so parser threads always observe the schema-bound reader by the time they pick up work.
         */
        private volatile SegmentableFormatReader reader;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final ErrorPolicy errorPolicy;
        /** See {@link FormatReadContext#readSchema()}. {@code null} = per-file inference. */
        @Nullable
        private final List<Attribute> readSchema;
        /** Added to each chunk's decompressed start byte; see {@link #parallelRead}'s {@code baseFileOffset}. */
        private final long baseFileOffset;
        /**
         * Consumer-owned per-file stats sink. Captured at construction so each chunk's parser worker
         * can bind it around {@code reader.read(...).close()} — see {@link ExternalStatsCapture} for
         * why this can't piggyback on the thread-local in production.
         */
        @Nullable
        private final ConcurrentMap<String, List<Map<String, Object>>> captureSink;
        /**
         * Receives the truncation warning when a non-strict policy converts a {@code max_record_size}
         * cap-hit into a graceful stop. Production wires {@link AsyncExternalSourceBuffer#recordWarning}
         * so the operator re-emits it on the driver thread; {@code null} falls back to a direct
         * {@link HeaderWarning} on the segmentator thread (tests / benchmarks). See {@link #emitTruncationWarning}.
         */
        @Nullable
        private final Consumer<String> partialResultsWarningSink;
        /** Compressed file being decompressed; {@code null} in tests that only supply a stream. */
        @Nullable
        private final StorageObject storageObject;

        private final ArrayBlockingQueue<byte[]> bufferPool;
        private final ArrayBlockingQueue<Chunk> chunkQueue;
        /** Capacity of {@link #bufferPool} (one pooled buffer per possible in-flight chunk-sized slice). */
        private final int bufferPoolSize;
        /** Length of {@link #pageQueues}; must match {@link #bufferPoolSize} so chunk index modulo never collides. */
        private final int pageQueueRingSize;
        private final int chunkSize;
        /**
         * Grow-loop bound. In production it comes from the {@code max_record_size} pragma (default
         * {@link SegmentableFormatReader#DEFAULT_MAX_RECORD_BYTES}); overridable for tests.
         */
        private final int maxRecordBytes;
        private final ArrayBlockingQueue<Page>[] pageQueues;
        private volatile SegmentableFormatReader recordSplitterReader;
        private volatile RecordSplitter recordSplitter;

        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        /**
         * Total outstanding tasks for this iterator: the segmentator (1, started at construction) plus
         * one per chunk it dispatches (incremented in {@link #dispatchChunk}, decremented in the parser
         * task's {@code finally}). EOF: reaches 0 only after the segmentator has exited AND every
         * dispatched chunk's parser task has exited (page emit + POISON enqueue). Replaces the previous
         * {@code CountDownLatch(1 + parallelism)} which assumed a fixed pre-spawned parser pool — the
         * pool that pinned executor slots and caused the multi-file-gzip deadlock.
         */
        private final AtomicInteger tasksOutstanding;
        /** Executor used to spawn one parser task per dispatched chunk. */
        private final Executor executor;
        private final AtomicInteger chunksDispatched = new AtomicInteger();
        /**
         * Bounds how far ahead of the consumer the segmentator may dispatch. {@link #pageQueues} is
         * indexed by {@code chunk.index % pageQueueRingSize}; without this semaphore a fast parser
         * could recycle a buffer (and the segmentator could dispatch a new chunk) before the consumer
         * drained the previous chunk's slot, interleaving pages from two generations into the same queue.
         * Acquired by the segmentator in {@link #dispatchChunk}; released by the consumer when a chunk's
         * POISON has been observed.
         */
        private final Semaphore dispatchPermits;

        private int currentChunk = 0;
        private Page buffered = null;
        private volatile boolean closed = false;
        /**
         * Set when a non-strict {@link ErrorPolicy} converts a {@code max_record_size} cap-hit into a
         * graceful stop instead of a hard failure (see {@link #runSegmentator}). A truncated read is
         * <em>not</em> a clean completion: the records emitted so far are a partial prefix and any
         * captured stats are an under-count, so {@link #close()} must poison them rather than cache
         * them as the file's full contribution.
         */
        private volatile boolean truncated = false;
        /**
         * Async-ready signal. {@code null} when no consumer is waiting. When the consumer's
         * {@link #waitForReady()} can't satisfy synchronously it installs a fresh listener here;
         * the producers (segmentator, parser, error-path) fire it on every event that can transition
         * the iterator to a ready state (chunk dispatched, page emitted, POISON enqueued, EOF, error).
         * Single-shot: after firing, cleared and replaced lazily by the next {@code waitForReady}.
         */
        private final AtomicReference<SubscribableListener<Void>> pendingReady = new AtomicReference<>();

        StreamingParallelIterator(
            SegmentableFormatReader reader,
            InputStream decompressedStream,
            @Nullable StorageObject storageObject,
            List<String> projectedColumns,
            int batchSize,
            int parallelism,
            Executor executor,
            ErrorPolicy errorPolicy,
            @Nullable List<Attribute> readSchema,
            long baseFileOffset,
            int maxRecordBytes,
            @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
            @Nullable Consumer<String> partialResultsWarningSink
        ) {
            this.reader = reader;
            this.storageObject = storageObject;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.errorPolicy = errorPolicy;
            this.readSchema = readSchema;
            this.baseFileOffset = baseFileOffset;
            this.maxRecordBytes = maxRecordBytes;
            this.captureSink = captureSink;
            this.partialResultsWarningSink = partialResultsWarningSink;
            this.bufferPoolSize = parallelism + 1;
            this.pageQueueRingSize = parallelism + 1;

            this.chunkSize = Math.toIntExact(reader.minimumSegmentSize());

            this.bufferPool = new ArrayBlockingQueue<>(bufferPoolSize);
            for (int i = 0; i < bufferPoolSize; i++) {
                bufferPool.add(new byte[chunkSize]);
            }

            this.chunkQueue = new ArrayBlockingQueue<>(parallelism);
            this.dispatchPermits = new Semaphore(pageQueueRingSize);
            this.executor = executor;

            @SuppressWarnings("unchecked")
            ArrayBlockingQueue<Page>[] queues = (ArrayBlockingQueue<Page>[]) new ArrayBlockingQueue<?>[pageQueueRingSize];
            this.pageQueues = queues;
            for (int i = 0; i < pageQueueRingSize; i++) {
                pageQueues[i] = new ArrayBlockingQueue<>(16);
            }

            // One-task-per-chunk model: the segmentator counts as one outstanding task, plus one
            // additional task per chunk it dispatches (incremented in {@link #dispatchChunk}, decremented
            // in {@link #runParserOnce}'s finally). The previous design pre-spawned {@code parallelism}
            // long-lived parser threads that parked on {@code chunkQueue.take()} indefinitely — that
            // pinning held an executor slot per parker, deadlocking the pool on multi-file gzip globs
            // where producer-loop drivers and sub-tasks of other iterators competed for the same slots.
            this.tasksOutstanding = new AtomicInteger(1);

            try {
                executor.execute(() -> runSegmentator(decompressedStream, this.chunkSize));
            } catch (RejectedExecutionException e) {
                firstError.compareAndSet(null, e);
                if (tasksOutstanding.decrementAndGet() == 0) {
                    signalReady();
                }
            }
        }

        private RecordSplitter recordSplitter() {
            SegmentableFormatReader currentReader = reader;
            RecordSplitter currentSplitter = recordSplitter;
            if (currentSplitter == null || recordSplitterReader != currentReader) {
                currentSplitter = currentReader.recordSplitter(maxRecordBytes);
                recordSplitterReader = currentReader;
                recordSplitter = currentSplitter;
            }
            return currentSplitter;
        }

        private RecordTooLargeException recordTooLargeException(int scannedBytes) {
            String hint = switch (reader.formatName()) {
                case "csv", "tsv" -> "; possible unclosed quote or bracket cell";
                default -> "";
            };
            return new RecordTooLargeException(
                "record exceeded max_record_size ["
                    + maxRecordBytes
                    + "] after scanning ["
                    + scannedBytes
                    + "] bytes for format ["
                    + reader.formatName()
                    + "]"
                    + hint
            );
        }

        /**
         * Raised by the segmentator when a single record exceeds {@code max_record_size} before a
         * boundary is found. Carried as a distinct type so {@link #runSegmentator} can branch on the
         * read policy without catching unrelated I/O errors: a strict policy rethrows it (hard fail),
         * a non-strict policy truncates the read at this point and surfaces a partial-results warning.
         * Extends {@link IOException} so that under a strict policy {@link ExternalFailures#surface}
         * still classifies it as client-class bad input (HTTP 400), exactly as before this change.
         */
        private static final class RecordTooLargeException extends IOException {
            RecordTooLargeException(String message) {
                super(message);
            }
        }

        /**
         * Surfaces a single client-visible {@code Warning} announcing that the read was truncated
         * because an undelimitable record exceeded {@code max_record_size}, returning only the records
         * parsed before it.
         * <p>
         * {@code recordStartByte} is the decompressed byte offset where the oversized record began —
         * i.e. the point at which good data ended — not where scanning gave up; it advances only after
         * a successful dispatch, so it is a stable "results truncated here" marker.
         * <p>
         * The message is routed through {@link #partialResultsWarningSink} when present so the operator
         * can re-emit it on the driver thread and the header actually reaches the client (the segmentator
         * runs on a forked worker whose response headers are never merged back — see #835). Callers
         * without a sink (tests, benchmarks) fall back to a direct {@link HeaderWarning} on the current
         * thread. A single self-contained line is emitted (rather than the
         * {@link org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings} summary+detail pair) because
         * this is a one-shot truncation event, not a per-row skip stream.
         */
        private void emitTruncationWarning(long recordStartByte, String causeMessage) {
            String warning = "External read truncated at byte ["
                + recordStartByte
                + "] (start of an oversized record); results are partial (error_mode="
                + errorPolicy.modeName()
                + "): "
                + causeMessage;
            if (partialResultsWarningSink != null) {
                partialResultsWarningSink.accept(warning);
            } else {
                // No varargs: HeaderWarning treats the message as a plain string so a '{' or '}' in the
                // format-specific hint is never reinterpreted as a placeholder pattern.
                HeaderWarning.addWarning(warning);
            }
            // INFO, not WARN: a truncation under an explicit non-strict error_mode is an expected,
            // opted-into degradation (the client gets the prominent Warning above), not an actionable
            // server-side problem an operator must investigate.
            logger.info("Streaming external read truncated at byte [{}] (non-strict policy): {}", recordStartByte, causeMessage);
        }

        private void runSegmentator(InputStream stream, int chunkSize) {
            byte[] carry = null;
            int carryLen = 0;
            int chunkIndex = 0;
            // Running offset of the next chunk in the decompressed stream. Each dispatched chunk
            // covers [coverageStart, coverageStart + dispatchedLength); the sequential segmentator
            // makes these tile [0, decompressedLength) deterministically for a given (file, config),
            // so the reconciler can union them by range and dedup a sibling scan's identical chunks.
            long coverageStart = 0;

            try {
                while (closed == false && firstError.get() == null) {
                    byte[] buf;
                    try {
                        buf = bufferPool.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    int offset = 0;
                    if (carry != null) {
                        System.arraycopy(carry, 0, buf, 0, carryLen);
                        offset = carryLen;
                        carry = null;
                        carryLen = 0;
                    }

                    int bytesRead = readFromStream(stream, buf, offset, buf.length - offset);
                    if (bytesRead <= 0 && offset == 0) {
                        recycleBuffer(buf);
                        break;
                    }

                    int totalBytes = offset + Math.max(bytesRead, 0);
                    boolean isEof = bytesRead < 0 || totalBytes < buf.length;

                    int lastNewline = recordSplitter().findLastRecordBoundary(buf, 0, totalBytes);
                    if (lastNewline == RecordSplitter.RECORD_TOO_LARGE) {
                        // Return the pool buffer before unwinding: under a non-strict policy the segmentator
                        // stops here but the iterator keeps draining already-dispatched chunks, so the pool
                        // must not permanently lose a slot.
                        recycleBuffer(buf);
                        throw recordTooLargeException(totalBytes);
                    }

                    if (lastNewline < 0) {
                        if (isEof) {
                            if (chunkIndex == 0) {
                                bindSchemaFromFirstChunk(buf, totalBytes);
                            }
                            if (dispatchChunk(chunkIndex, coverageStart, buf, totalBytes, true)) {
                                chunkIndex++;
                                coverageStart += totalBytes;
                            } else {
                                recycleBuffer(buf);
                                break;
                            }
                        } else {
                            // Single record larger than chunk size — grow a temporary buffer.
                            // {@link #growUntilRecordBoundary} only copies from {@code buf}; the original pool
                            // buffer is independent of {@code grown}, so recycle it in a finally. The grow loop
                            // can throw RecordTooLargeException (cap-hit) — the dominant path for an oversized
                            // record — and under a non-strict policy the segmentator stops there while the
                            // iterator keeps draining already-dispatched chunks, so the pool must not lose a slot.
                            GrowResult result;
                            try {
                                result = growUntilRecordBoundary(stream, buf, totalBytes, chunkSize);
                            } finally {
                                recycleBuffer(buf);
                            }
                            byte[] grown = result.buffer();
                            int grownNewline = result.boundary();
                            if (grownNewline < 0) {
                                if (chunkIndex == 0) {
                                    bindSchemaFromFirstChunk(grown, grown.length);
                                }
                                if (dispatchChunk(chunkIndex, coverageStart, grown, grown.length, true)) {
                                    chunkIndex++;
                                    coverageStart += grown.length;
                                } else {
                                    recycleBuffer(grown);
                                    break;
                                }
                            } else {
                                int validLen = grownNewline + 1;
                                carryLen = grown.length - validLen;
                                carry = new byte[carryLen];
                                System.arraycopy(grown, validLen, carry, 0, carryLen);
                                if (chunkIndex == 0) {
                                    bindSchemaFromFirstChunk(grown, validLen);
                                }
                                if (dispatchChunk(chunkIndex, coverageStart, grown, validLen, false)) {
                                    chunkIndex++;
                                    coverageStart += validLen;
                                } else {
                                    recycleBuffer(grown);
                                    break;
                                }
                            }
                        }
                        if (isEof) break;
                        continue;
                    }

                    int validLen = isEof ? totalBytes : lastNewline + 1;

                    if (isEof == false && validLen < totalBytes) {
                        carryLen = totalBytes - validLen;
                        carry = new byte[carryLen];
                        System.arraycopy(buf, validLen, carry, 0, carryLen);
                    }

                    if (chunkIndex == 0) {
                        bindSchemaFromFirstChunk(buf, validLen);
                    }
                    if (dispatchChunk(chunkIndex, coverageStart, buf, validLen, isEof)) {
                        chunkIndex++;
                        coverageStart += validLen;
                        if (isEof) break;
                    } else {
                        recycleBuffer(buf);
                        break;
                    }
                }
            } catch (RecordTooLargeException e) {
                // A single record exceeded max_record_size before any boundary was found. Under a strict
                // policy this stays a hard failure (the historical behavior); under a non-strict policy we
                // honor the lenient read intent by truncating the read here: stop dispatching, keep the
                // records parsed so far, and surface a client-visible partial-results warning. An
                // undelimitable record has no resumption point, so we truncate at the failure rather than
                // skip-and-continue. coverageStart is the decompressed byte offset where the oversized
                // record began (it advances only after a successful dispatch).
                if (errorPolicy.isStrict()) {
                    firstError.compareAndSet(null, e);
                    signalReady();
                } else {
                    truncated = true;
                    emitTruncationWarning(coverageStart, e.getMessage());
                    // Fall through to finally: already-dispatched chunks drain and the consumer reaches EOF.
                }
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
                signalReady();
            } finally {
                try {
                    stream.close();
                } catch (IOException ignored) {}
                // No POISON-to-parkers fan-out anymore: parser tasks are one-shot (one per chunk)
                // and exit on their own after processing. Segmentator's done; decrement and signal
                // so the consumer wakes if it's the last task standing (EOF condition is
                // currentChunk >= chunksDispatched && tasksOutstanding == 0).
                if (tasksOutstanding.decrementAndGet() == 0) {
                    signalReady();
                }
            }
        }

        /**
         * Infers the schema from the first chunk and swaps {@link #reader} for the schema-bound
         * variant returned by {@link FormatReader#withSchema(List)}; parser threads thereafter
         * skip per-chunk inference. Same approach as ClickHouse / DuckDB / Spark.
         */
        private void bindSchemaFromFirstChunk(byte[] buffer, int length) throws IOException {
            ByteArrayStorageObject firstChunkObj = chunkStorageObject(0, buffer, 0, length);
            SourceMetadata metadata = reader.metadata(firstChunkObj);
            List<Attribute> schema = metadata == null ? null : metadata.schema();
            if (schema == null) {
                return;
            }
            FormatReader bound = reader.withSchema(schema);
            if (bound == reader) {
                return;
            }
            if (bound instanceof SegmentableFormatReader segBound) {
                this.reader = segBound;
            } else {
                throw new IllegalStateException(
                    "FormatReader#withSchema returned a non-SegmentableFormatReader: " + bound.getClass().getName()
                );
            }
        }

        /**
         * Waits for {@link #dispatchPermits}, then enqueues a chunk for parsers unless the coordinator
         * is closed or the calling thread is interrupted.
         *
         * @return {@code true} if the chunk was queued; {@code false} if dispatch aborted. On {@code false},
         *         {@link #chunksDispatched} is unchanged and the caller must {@link #recycleBuffer(byte[])}
         *         when {@code buffer} is pool-sized (oversized temporary buffers are simply dropped).
         */
        private boolean dispatchChunk(int index, long coverageStart, byte[] buffer, int length, boolean last) {
            try {
                dispatchPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                firstError.compareAndSet(null, e);
                return false;
            }
            if (closed) {
                dispatchPermits.release();
                return false;
            }
            try {
                chunkQueue.put(new Chunk(index, coverageStart, buffer, length, last));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                firstError.compareAndSet(null, e);
                dispatchPermits.release();
                return false;
            }
            chunksDispatched.incrementAndGet();
            // Wake any consumer that was previously parked at currentChunk == chunksDispatched.
            signalReady();
            // Spawn one parser task for this chunk. The task is one-shot (process + exit) so its
            // executor slot is released as soon as the chunk is parsed — this is the change that
            // prevents pool-exhaustion deadlocks on multi-file gzip globs. Pre-fix, persistent
            // parser threads parked on {@code chunkQueue.take()} held slots for the iterator's
            // lifetime, pinning the pool when many file readers were active concurrently.
            tasksOutstanding.incrementAndGet();
            try {
                executor.execute(this::runParserOnce);
            } catch (RejectedExecutionException e) {
                firstError.compareAndSet(null, e);
                if (tasksOutstanding.decrementAndGet() == 0) {
                    signalReady();
                }
                return false;
            }
            return true;
        }

        /**
         * One-shot parser: takes the single chunk the segmentator just queued, parses it, emits its
         * pages and POISON, and exits — releasing its executor slot for the next task. Per-task
         * lifetime keeps the pool footprint proportional to in-flight chunks (bounded by
         * {@link #dispatchPermits}) rather than to {@code parallelism × concurrent_file_readers}
         * (the pre-fix pool footprint that caused the multi-file-gzip deadlock).
         */
        private void runParserOnce() {
            Chunk chunk = null;
            ArrayBlockingQueue<Page> queue = null;
            try {
                if (closed || firstError.get() != null) {
                    return;
                }
                // chunkQueue is FIFO and the segmentator put exactly one chunk before submitting this
                // task; the poll returns it immediately. If a concurrent close drained the queue
                // (drainAllQueues), poll returns null and we exit cleanly via finally.
                chunk = chunkQueue.poll();
                if (chunk == null) {
                    return;
                }
                int queueSlot = chunk.index % pageQueueRingSize;
                queue = pageQueues[queueSlot];
                ByteArrayStorageObject chunkObj = chunkStorageObject(chunk.index, chunk.buffer, 0, chunk.length);
                // - firstSplit: only chunk 0 carries the file's leading bytes (header for CSV).
                // - lastSplit: every chunk is aligned to a record boundary by the segmentator, so
                // line-oriented readers (NDJSON) can skip TrimLastPartialLineInputStream.
                // - recordAligned: chunks always start on a record boundary, so readers skip the
                // "drop leading partial line" workaround used for byte-range macro-splits.
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(projectedColumns)
                    .batchSize(batchSize)
                    .errorPolicy(errorPolicy)
                    .firstSplit(chunk.index == 0)
                    .lastSplit(true)
                    .recordAligned(true)
                    .readSchema(readSchema)
                    .splitStartByte(baseFileOffset + chunk.coverageStart())
                    .maxRecordBytes(maxRecordBytes)
                    .build();
                // Bind the consumer-owned sink on this worker so the reader's close hook reaches
                // the same map the consumer-thread StatsCapturingIterator binds. The pages iterator
                // is opened inside the bound's try-with-resources so a failing reader.read still
                // restores the previous ThreadLocal binding — workers in this executor are reused
                // across queries, a leaked binding would route subsequent tasks' record() calls
                // into the prior query's sink. Inner closes first, so record() runs with the sink
                // still bound, then the handle restores the previous binding.
                ExternalStatsCapture.Handle bound = captureSink != null
                    ? ExternalStatsCapture.bind(
                        captureSink,
                        new ExternalStatsCapture.Coverage(chunk.coverageStart(), chunk.coverageStart() + chunk.length(), chunk.last())
                    )
                    : () -> {};
                try (bound) {
                    try (CloseableIterator<Page> pages = reader.read(chunkObj, ctx)) {
                        while (pages.hasNext()) {
                            if (firstError.get() != null || closed) {
                                break;
                            }
                            putPageAndSignal(queue, pages.next());
                        }
                    }
                }
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
                signalReady();
            } finally {
                if (chunk != null) {
                    recycleBuffer(chunk.buffer);
                    if (queue != null) {
                        putPoisonAndSignal(queue);
                    }
                }
                if (tasksOutstanding.decrementAndGet() == 0) {
                    signalReady();
                }
            }
        }

        private void recycleBuffer(byte[] buf) {
            if (buf.length <= chunkSize) {
                bufferPool.offer(buf);
            }
        }

        private static int readFromStream(InputStream stream, byte[] buf, int offset, int length) throws IOException {
            int totalRead = 0;
            while (totalRead < length) {
                int n = stream.read(buf, offset + totalRead, length - totalRead);
                if (n < 0) {
                    return totalRead == 0 ? -1 : totalRead;
                }
                totalRead += n;
            }
            return totalRead;
        }

        private static byte[] growUntilNewline(InputStream stream, byte[] existing, int existingLen, int growBy) throws IOException {
            byte[] grown = new byte[existingLen + growBy];
            System.arraycopy(existing, 0, grown, 0, existingLen);

            int offset = existingLen;
            while (true) {
                int n = stream.read(grown, offset, grown.length - offset);
                if (n < 0) {
                    if (offset == existingLen) {
                        return Arrays.copyOf(existing, existingLen);
                    }
                    return Arrays.copyOf(grown, offset);
                }
                for (int i = offset; i < offset + n; i++) {
                    if (grown[i] == '\n') {
                        return Arrays.copyOf(grown, offset + n);
                    }
                }
                offset += n;
                if (offset >= grown.length) {
                    grown = Arrays.copyOf(grown, grown.length + growBy);
                }
            }
        }

        private record GrowResult(byte[] buffer, int boundary) {}

        /**
         * Like {@link #growUntilNewline} but keeps growing until the accumulated buffer contains at
         * least one record boundary (as determined by {@link RecordSplitter#findLastRecordBoundary}).
         * Multi-line quoted fields may contain {@code \n} bytes that are not record boundaries; this
         * method avoids splitting in the middle of such a field.
         * <p>
         * The inner loop uses {@link #growUntilNewline} (raw {@code \n} scan) intentionally: a
         * record boundary always coincides with a {@code \n}, so growing to the next raw
         * {@code \n} is the minimum I/O needed before re-checking with the quote-aware SPI method.
         * For quoted fields with embedded {@code \n}, the raw scan stops too early and the
         * boundary check returns {@code -1}, causing another growth iteration — correct, just
         * not single-pass.
         *
         * @return a {@link GrowResult} carrying both the grown buffer and the pre-computed boundary
         *         index, so callers can avoid a redundant boundary rescan
         *
         * <p><strong>Note on memory:</strong> {@code maxRecordBytes} bounds record <em>acceptance</em>,
         * not transient allocation. The {@link #growUntilNewline} step below reads to the next raw
         * {@code \n} (or EOF) before the cap pre-check fires, so a record with no newline for N bytes can
         * buffer up to ~N bytes (one {@code growBy} chunk past the cap) before being rejected. Bounding
         * peak buffer growth to {@code maxRecordBytes} itself would be a separate follow-up (#835).
         */
        private GrowResult growUntilRecordBoundary(InputStream stream, byte[] existing, int existingLen, int growBy) throws IOException {
            byte[] buf = existing;
            int len = existingLen;
            // Bound the grow loop: a record past maxRecordBytes means the scanner won't find a boundary
            // (a format/quoting mismatch), so fail rather than read the input without bound.
            while (true) {
                if (len + growBy > maxRecordBytes) {
                    throw recordTooLargeException(len);
                }
                byte[] grown = growUntilNewline(stream, buf, len, growBy);
                if (grown.length == len) {
                    return new GrowResult(grown, -1);
                }
                // Rescans the whole grown buffer each iteration; total work is O(n^2), bounded by maxRecordBytes.
                int boundary = recordSplitter().findLastRecordBoundary(grown, 0, grown.length);
                if (boundary == RecordSplitter.RECORD_TOO_LARGE) {
                    throw recordTooLargeException(grown.length);
                }
                if (boundary >= 0) {
                    return new GrowResult(grown, boundary);
                }
                buf = grown;
                len = grown.length;
            }
        }

        /**
         * Puts {@code page} on {@code queue} and wakes any consumer parked on {@link #waitForReady()}.
         * <p>
         * <strong>The {@link #signalReady()} call here is load-bearing — do not remove it.</strong>
         * The consumer registers a listener with {@link #waitForReady()} when {@code pageQueues[slot]}
         * is empty AND the chunk for {@code slot} hasn't finished producing. The signal here is what
         * transitions the listener from pending to fired — without it the consumer would park
         * indefinitely while pages accumulate in the queue. The post-publish signal is also necessary
         * for the case where the parser fills {@code pageQueues[slot]} to its capacity (16 pages):
         * {@code queue.put} would block, and if the consumer hasn't been woken to start draining there
         * is no one to free a slot. Signaling on every page put (one cheap {@code AtomicReference.getAndSet})
         * is the simplest correct choice; consolidating to "signal only on 0→1 transitions" is a valid
         * optimization but requires careful coordination with consumer-side state, so we don't bother —
         * the overhead is negligible compared to parse cost.
         */
        private void putPageAndSignal(ArrayBlockingQueue<Page> queue, Page page) throws InterruptedException {
            queue.put(page);
            signalReady();
        }

        /**
         * Enqueues the POISON sentinel on {@code queue} and wakes any consumer parked on
         * {@link #waitForReady()}. The {@link #signalReady()} call here is load-bearing:
         * POISON is the consumer's signal that a chunk's pages are fully drained; if the consumer
         * is parked on {@code waitForReady} when the parser exits normally (typical case for tiny
         * chunks emitting zero pages), the POISON signal is what unblocks the consumer to advance
         * {@code currentChunk} and release a dispatchPermit. Without this signal a healthy parse can
         * stall after the last chunk in a slot.
         */
        private void putPoisonAndSignal(ArrayBlockingQueue<Page> queue) {
            boolean poisoned = false;
            while (poisoned == false) {
                try {
                    queue.put(POISON);
                    poisoned = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            signalReady();
        }

        /**
         * Blocks until a page is available or EOF, preserving the standard {@link java.util.Iterator} contract
         * for synchronous consumers (test code, generic CloseableIterator users). Async consumers
         * (the producer-loop in {@code AsyncExternalSourceOperatorFactory}) bypass this block by
         * calling {@link #waitForReady()} first, then either drain via this method (now non-spinning
         * because the page is already there) or yield their executor thread when the listener isn't
         * yet done. Internally implemented via the same {@link #waitForReady()} signal so the wait
         * is event-driven (no busy-poll on the thread), and {@link #takeNextPage()} is non-blocking
         * (single peek, advance through one POISON if encountered, return).
         */
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (buffered != null) {
                return true;
            }
            while (true) {
                buffered = takeNextPage();
                if (buffered != null) {
                    return true;
                }
                // takeNextPage returned null. Could be: (a) EOF, (b) "no page yet, more coming", or
                // (c) just advanced past POISON into an empty slot whose chunk is still parsing.
                // We must NOT conclude EOF based solely on waitForReady being done — that listener
                // could have fired because a POISON landed (transitioning peek != null → true), and
                // takeNextPage just consumed that POISON. Only conclude EOF when waitForReady is
                // done AND the EOF condition itself holds (no more chunks, all tasks finished).
                SubscribableListener<Void> ready = waitForReady();
                if (ready.isDone()) {
                    buffered = takeNextPage();
                    if (buffered != null) {
                        return true;
                    }
                    if (currentChunk >= chunksDispatched.get() && tasksOutstanding.get() == 0) {
                        return false;
                    }
                    // Not EOF — page may have advanced past POISON; loop to re-register a listener
                    // for the next state transition.
                    continue;
                }
                CountDownLatch latch = new CountDownLatch(1);
                ready.addListener(ActionListener.running(latch::countDown));
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for streaming parallel parse results", e);
                }
                if (closed) {
                    return false;
                }
                // Loop: re-poll takeNextPage. The signal that fired ready may have been for a page
                // that's now in the current slot, or for a transition we still need to drain past.
            }
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
         * Returns {@code true} when {@link #hasNext()} can run without blocking on upstream
         * production: a page is already buffered, the current slot has a page or POISON, EOF has
         * been reached, an error has been recorded, or the iterator is closed.
         */
        private boolean isReadyNow() {
            if (closed || buffered != null || firstError.get() != null) {
                return true;
            }
            int slot = currentChunk % pageQueueRingSize;
            if (pageQueues[slot].peek() != null) {
                return true;
            }
            // EOF: consumer has drained every dispatched chunk AND every producer has exited.
            return currentChunk >= chunksDispatched.get() && tasksOutstanding.get() == 0;
        }

        @Override
        public SubscribableListener<Void> waitForReady() {
            if (isReadyNow()) {
                return SubscribableListener.newSucceeded(null);
            }
            // Install a listener for the next state-change event (page enqueued, POISON enqueued,
            // chunk dispatched, EOF, or error). Re-check after CAS to close the gap where state
            // flipped to ready between the first {@link #isReadyNow} call and the install.
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
         * Fires the pending readiness listener (if any). Producers call this from any state-change
         * site so that a consumer parked on {@link #waitForReady()} resumes promptly.
         */
        private void signalReady() {
            SubscribableListener<Void> listener = pendingReady.getAndSet(null);
            if (listener != null) {
                listener.onResponse(null);
            }
        }

        /**
         * Single-shot non-blocking page retrieval. Never spins, never holds the calling thread
         * waiting for upstream production — returns {@code null} as soon as no page is available
         * in the current slot. Callers must use {@link #waitForReady()} to know when to retry;
         * a {@code null} return value paired with an immediately-done {@link #waitForReady()}
         * indicates terminal EOF (no further chunks will ever be dispatched), while {@code null}
         * paired with a non-done {@link #waitForReady()} indicates the iterator is still
         * producing but no page is in the current slot at this instant. This separation lets the
         * producer-loop driver yield its executor slot back to the pool during the gap between
         * draining one chunk's POISON and the segmenter+parser delivering the next chunk's pages —
         * which is the deadlock-on-multi-file-gzip scenario {@code waitForReady} was built to fix.
         * The previous spinning implementation held the consumer thread across that gap, blocking
         * sub-tasks (segmenter, parsers) queued behind it on the same executor.
         */
        private Page takeNextPage() {
            // Bounded loop — at most one POISON-advance plus one peek per call. Each POISON observed
            // advances currentChunk and releases a dispatchPermit so the segmentator can resume;
            // immediately after, we either find a page in the new slot (return it) or return null.
            // Never blocks.
            while (true) {
                checkError();
                if (currentChunk >= chunksDispatched.get()) {
                    // No new chunk yet — either EOF (allDone fired) or we're between dispatches.
                    // {@link #waitForReady} will return immediately-done in the EOF case (covered by
                    // its EOF branch) and a non-done listener otherwise. Either way the caller knows
                    // what to do next without us holding the thread.
                    return null;
                }
                int slot = currentChunk % pageQueueRingSize;
                Page page = pageQueues[slot].poll();
                if (page == null) {
                    // Chunk has been dispatched but its parser hasn't published a page yet.
                    // Bail without spinning; waitForReady will fire when the parser puts a page.
                    return null;
                }
                if (page == POISON) {
                    currentChunk++;
                    dispatchPermits.release();
                    // Retry once on the new slot. If the next chunk's parser has already published
                    // a page, we return it now (avoids an unnecessary executor round-trip); if not,
                    // the next iteration's poll returns null and we exit to let the caller yield.
                    continue;
                }
                return page;
            }
        }

        private void checkError() {
            Throwable t = firstError.get();
            if (t != null) {
                throw ExternalFailures.surface(t, "Streaming parallel parsing failed");
            }
        }

        /**
         * Test-only accessor: returns {@code true} when at least one thread is parked on
         * {@link #dispatchPermits}. Used by close-path tests to deterministically wait for the
         * segmentator to reach the permit-acquire blocking site instead of relying on wall-clock sleeps.
         */
        boolean isSegmentatorParkedOnDispatchPermits() {
            return dispatchPermits.hasQueuedThreads();
        }

        /**
         * Two-phase shutdown sequenced to drain pages a parser task may publish after the first drain
         * but before all outstanding tasks finish.
         * <p>
         * Phase 1: set {@code closed=true} (causes any in-flight parser task to bail on its
         * {@code firstError || closed} check and the segmentator to skip its next iteration), wake any
         * segmentator parked on {@link #dispatchPermits}, and drain whatever is already queued.
         * <p>
         * Phase 2: poll {@link #tasksOutstanding} for up to {@link #CLOSE_TIMEOUT_SECONDS} so all
         * one-shot parser tasks (and the segmentator) have a chance to exit cleanly, then drain again
         * to catch pages a parser may have queued between the first drain and its own exit.
         * <p>
         * <strong>Timeout contract:</strong> if {@code tasksOutstanding} does not reach 0 within the
         * deadline we log a warning and run the second drain anyway, but parser tasks may still be
         * alive at return time. Any pages they publish after that — and any blocks those pages
         * reference — are leaked. We do not interrupt the parser executor: it is shared and an
         * interrupt could disrupt unrelated tasks. The timeout is intentionally generous (60s) to make
         * the leak window an exceptional condition; production workloads should not hit it.
         */
        private ByteArrayStorageObject chunkStorageObject(int chunkIndex, byte[] buffer, int offset, int length) {
            StoragePath path = storageObject != null ? storageObject.path() : StoragePath.of("mem://chunk-" + chunkIndex);
            Instant mtime = Instant.EPOCH;
            if (storageObject != null) {
                try {
                    Instant fileMtime = storageObject.lastModified();
                    if (fileMtime != null) {
                        mtime = fileMtime;
                    }
                } catch (IOException e) {
                    // Fall back to EPOCH — chunk stats may be uncacheable without a pinned mtime.
                }
            }
            return new ByteArrayStorageObject(path, buffer, offset, length, mtime);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            // Flip closed first: a segmentator already past the if(closed) check in dispatchChunk can
            // still enqueue one more chunk and spawn its parser. cleanCompletion is therefore evaluated
            // *after* the drain loop below — where chunksDispatched is final and no parser is in flight —
            // not here, where reading chunksDispatched could miss that late dispatch and falsely conclude
            // the scan drained cleanly (skipping the poison and caching an under-count).
            closed = true;
            // Wake any consumer parked on {@link #waitForReady()}; isReadyNow now returns true on closed.
            signalReady();
            // Wake the segmentator if parked on dispatchPermits.acquire(); after a successful acquire it
            // re-checks {@code closed} and exits {@link #dispatchChunk} without enqueueing.
            dispatchPermits.release();
            drainAllQueues();
            // Poll tasksOutstanding instead of waiting on a fixed CountDownLatch — the number of
            // outstanding tasks is dynamic (one per dispatched chunk) so we can't precompute it.
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLOSE_TIMEOUT_SECONDS);
            try {
                while (tasksOutstanding.get() > 0 && System.nanoTime() < deadlineNanos) {
                    Thread.sleep(50);
                    drainAllQueues();
                }
                if (tasksOutstanding.get() > 0) {
                    logger.warn(
                        "Timed out waiting for streaming parallel parsing tasks to finish after [{}]s; "
                            + "any pages published by still-running parsers after this point will leak their blocks",
                        CLOSE_TIMEOUT_SECONDS
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainAllQueues();
            // Now safe to evaluate: chunksDispatched is final (the drain loop waited for every spawned
            // parser, including any dispatched in the close race window) and the consumer's currentChunk
            // is fixed. Clean = no error, the read was not truncated, and the consumer drained every
            // dispatched chunk. An early close (LIMIT, cancellation) leaves chunks unconsumed — and a
            // parser cut off mid-chunk records a partial row count under that chunk's full byte range,
            // which the coverage tiling would otherwise accept as complete and cache as an under-count.
            // A non-strict truncation (max_record_size cap-hit) likewise emits only a prefix of the
            // file's records, so it must not be cached as the file's full contribution. Either way a
            // non-clean scan poisons the file's contributions: the reconciler discards them.
            boolean cleanCompletion = firstError.get() == null && truncated == false && currentChunk >= chunksDispatched.get();
            if (cleanCompletion == false && captureSink != null && storageObject != null) {
                poisonCapturedStats(storageObject.path().toString());
            }
        }

        private void poisonCapturedStats(String path) {
            Map<String, Object> poison = new HashMap<>();
            poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);
            ExternalStatsCapture.record(path, poison);
        }

        private void drainAllQueues() {
            // Drain chunk queue
            Chunk chunk;
            while ((chunk = chunkQueue.poll()) != null) {
                recycleBuffer(chunk.buffer);
            }
            // Drain page queues
            for (ArrayBlockingQueue<Page> queue : pageQueues) {
                Page p;
                while ((p = queue.poll()) != null) {
                    if (p != POISON && p.getPositionCount() > 0) {
                        p.releaseBlocks();
                    }
                }
            }
        }
    }

    private record Chunk(int index, long coverageStart, byte[] buffer, int length, boolean last) {}

    /**
     * Minimal StorageObject wrapping an InputStream for the parallelism=1 fallback path.
     * <p>
     * <strong>Single-use:</strong> the wrapped stream is sequential and cannot be re-opened, so
     * {@link #newStream()} hands out the same underlying stream exactly once. Callers that need
     * to consume the stream twice (e.g. metadata inference followed by a separate read pass)
     * must buffer the data themselves; the production path here calls {@code newStream()} only
     * once via {@code reader.read(...)}.
     */
    private static final class InputStreamStorageObject implements StorageObject {
        private final InputStream stream;
        private final AtomicBoolean handedOut = new AtomicBoolean(false);

        InputStreamStorageObject(InputStream stream) {
            this.stream = stream;
        }

        @Override
        public InputStream newStream() {
            if (handedOut.compareAndSet(false, true) == false) {
                throw new IllegalStateException(
                    "InputStreamStorageObject is single-use; the wrapped sequential stream cannot be re-opened"
                );
            }
            return stream;
        }

        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException("Streaming storage object does not support random access");
        }

        @Override
        public long length() {
            throw new UnsupportedOperationException("Streaming storage object has unknown length");
        }

        @Override
        public java.time.Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.spi.StoragePath path() {
            return StoragePath.of("stream://decompressed");
        }
    }
}
