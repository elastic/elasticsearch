/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BooleanSupplier;

/**
 * Finds format-specific record boundaries for splittable row-oriented external data sources.
 * <p>
 * Implementations are used in two different contexts: stream-based planning code that skips
 * forward to the next complete record, and byte-array chunking code that slices an already-read
 * buffer at the last complete record.
 */
public interface RecordSplitter {

    /**
     * Returned by {@link #findNextRecordBoundary(InputStream)} when a scanner exceeds
     * {@link #maxRecordBytes()} before finding a record boundary. Distinct from EOF ({@code -1}).
     */
    long RECORD_TOO_LARGE = -2L;

    /**
     * Returned <em>only</em> by {@link #findProvenRecordBoundary(InputStream)} when a bounded probe cannot
     * <em>prove</em> a record start within its convergence window (e.g. a quote-free stretch where the
     * in-quote hypothesis never closes, or a lookahead that would need a byte past the window). Distinct
     * from EOF ({@code -1}) and {@link #RECORD_TOO_LARGE} ({@code -2}): it means "undecided here, retry
     * with the exact walk", not "no boundary" or "record too large". The exact walk
     * ({@link #findRecordStartAtOrAfter(InputStream, long, BooleanSupplier)}), not the probe, owns the
     * {@link #RECORD_TOO_LARGE} verdict because it always starts from a known record start.
     */
    long AMBIGUOUS = -3L;

    /**
     * Scans forward from the current stream position until the next complete record boundary.
     *
     * @return the number of bytes consumed, {@code -1} if EOF is reached before a boundary,
     *         or {@link #RECORD_TOO_LARGE} if the next record exceeds {@link #maxRecordBytes()}
     */
    long findNextRecordBoundary(InputStream stream) throws IOException;

    /**
     * Returns the index of the byte that terminates the last complete record within
     * {@code buf[offset..offset + length)}, or {@code -1} if no complete record terminates inside
     * that range.
     * <p>
     * When the range ends inside an open record, implementations must return the previous complete
     * record boundary, not a byte inside the open tail. Implementations that enforce
     * {@link #maxRecordBytes()} may return {@code (int)} {@link #RECORD_TOO_LARGE} when no boundary
     * was found and the open record already exceeds the cap, so callers fail fast instead of
     * growing the buffer further.
     */
    int findLastRecordBoundary(byte[] buf, int offset, int length) throws IOException;

    /**
     * Returns the index of the byte that terminates the last complete record within
     * {@code buf[0..length)}, or {@code -1} if no complete record terminates inside that range.
     */
    default int findLastRecordBoundary(byte[] buf, int length) throws IOException {
        return findLastRecordBoundary(buf, 0, length);
    }

    /**
     * Maximum bytes a single record may occupy before the splitter reports {@link #RECORD_TOO_LARGE}.
     */
    int maxRecordBytes();

    /**
     * Whether it is safe to begin a boundary probe at an arbitrary mid-file offset (as the stride-based
     * segmentation in {@code FileSplitProvider} and {@code ParallelParsingCoordinator} does).
     * <p>
     * This holds only when every record terminator is unambiguous from the byte immediately at it, i.e. a
     * raw newline is always a true record boundary regardless of the state the scan started in. It is
     * <em>false</em> for splitters whose grammar can carry a record across a raw newline (quoted fields
     * with embedded newlines, bracketed multi-value cells): starting a probe inside such a construct
     * misreads an in-construct newline as a terminator and desyncs the parse. Callers that get {@code false}
     * must not split at arbitrary offsets; they read the file as one sequential stream instead.
     */
    default boolean supportsStridedProbing() {
        return true;
    }

    /**
     * Whether this splitter can <em>prove</em> a record start at an arbitrary mid-file offset even though it is
     * not strided (i.e. {@link #supportsStridedProbing()} is {@code false} because its grammar can carry a record
     * across a raw newline). This is the second capability boolean on the SPI, orthogonal to
     * {@link #supportsStridedProbing()}: a splitter that returns {@code true} here restores cross-node macro
     * splitting for quoted/escaped text by running a bounded {@link #findProvenRecordBoundary(InputStream)} probe
     * and falling back to a monotonic {@link #findRecordStartAtOrAfter(InputStream, long, BooleanSupplier)} exact
     * walk, both of which yield only true record starts, so a split boundary is never cut inside a quoted field or
     * an escaped newline.
     * <p>
     * Callers must check this before calling {@link #findProvenRecordBoundary(InputStream)} or
     * {@link #findRecordStartAtOrAfter(InputStream, long, BooleanSupplier)}, exactly as they check
     * {@link #supportsStridedProbing()} before {@link #findNextRecordBoundary(InputStream)} on the strided path.
     */
    default boolean supportsProvenProbing() {
        return false;
    }

    /**
     * Probes for a proven record start starting from the current stream position (an arbitrary mid-file stride
     * offset). Returns the byte count consumed <em>through</em> the record terminator at which the probe proved a
     * record start (mirroring {@link #findNextRecordBoundary(InputStream)}'s {@code consumed} return, so the caller
     * adds it to the stream's base offset), or {@link #AMBIGUOUS} if it could not prove one within its bounded
     * convergence window. Never returns {@link #RECORD_TOO_LARGE}: window exhaustion is always {@link #AMBIGUOUS},
     * deferring the cap verdict to {@link #findRecordStartAtOrAfter(InputStream, long, BooleanSupplier)}.
     * <p>
     * The default implementation throws so a splitter that advertises {@link #supportsProvenProbing()} but forgets
     * to override this fails loud rather than silently delegating to a quote-unaware scan.
     */
    default long findProvenRecordBoundary(InputStream stream) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " does not support proven probing");
    }

    /**
     * Exact forward walk from a stream opened at a <em>known</em> record start, keeping cross-record parse state
     * (e.g. in-quote) across records in a single pass, returning the stream-relative offset of the first record
     * start at or after {@code minSkip}. {@code minSkip} is stream-relative and always {@code > 0}, so the returned
     * offset is strictly greater than the stream's base record start (never {@code 0}). Returns
     * {@link #RECORD_TOO_LARGE} if a single record exceeds {@link #maxRecordBytes()} before a boundary, or
     * {@code -1} at EOF before reaching {@code minSkip}.
     * <p>
     * {@code isCancelled} is polled inside the byte loop so a long walk (a record up to {@link #maxRecordBytes()})
     * aborts promptly; a cancelled walk throws {@link org.elasticsearch.tasks.TaskCancelledException}. Read-time
     * callers that do not carry a cancellable task pass {@code () -> false}.
     * <p>
     * The default implementation throws so a splitter that advertises {@link #supportsProvenProbing()} but forgets
     * to override this fails loud.
     */
    default long findRecordStartAtOrAfter(InputStream stream, long minSkip, BooleanSupplier isCancelled) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " does not support proven probing");
    }
}
