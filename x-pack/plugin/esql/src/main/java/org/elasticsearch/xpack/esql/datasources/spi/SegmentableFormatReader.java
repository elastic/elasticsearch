/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Extension of {@link FormatReader} for line-oriented text formats (CSV, NDJSON)
 * that support intra-file parallel parsing.
 * <p>
 * Formats that implement this interface declare they can find record boundaries
 * within an arbitrary byte stream, enabling the framework to split a single file
 * into byte-range segments and parse them concurrently on multiple threads.
 * <p>
 * Columnar formats (Parquet, ORC) should not implement this interface — they have
 * row-group-level parallelism instead.
 */
public interface SegmentableFormatReader extends FormatReader {

    /**
     * Default cap on the bytes a single record may occupy; the streaming splitter fails the query rather
     * than buffering past this when a scanner cannot find a boundary. Overridable via the
     * {@code external_max_record_size} pragma.
     */
    int DEFAULT_MAX_RECORD_BYTES = 64 * 1024 * 1024;

    /**
     * Returns the record-boundary splitter for this reader.
     */
    default RecordSplitter recordSplitter() {
        return recordSplitter(DEFAULT_MAX_RECORD_BYTES);
    }

    /**
     * Returns the record-boundary splitter with a caller-supplied record-size cap.
     * Implementations report {@link RecordSplitter#RECORD_TOO_LARGE} when a record exceeds
     * {@code maxRecordBytes}.
     */
    RecordSplitter recordSplitter(int maxRecordBytes);

    /**
     * Returns the minimum segment size in bytes below which splitting is not worthwhile.
     * <p>
     * It is a guarantee about the tail and advice about everything before it. Splitting stops once fewer than
     * this many bytes are left, so the final segment is never short. Between two segments it only sets the
     * spacing of the offsets that get probed: a boundary resolves somewhere inside its probe window, so a
     * segment can come out shorter than this by up to the width of that window, and no pass merges it into its
     * neighbour. Implementations must therefore read a segment of any size, and should read this as the size
     * they are asking to be aimed at rather than the size they are promised.
     * <p>
     * Defaults to 1 MiB. ClickHouse benchmarks show 1 MiB chunks are optimal for
     * parallel parsing — 100 KB chunks are ~40% slower due to per-chunk overhead,
     * while 10 MiB chunks offer only marginal improvement.
     * Implementations may override to reflect their parsing overhead.
     */
    default long minimumSegmentSize() {
        return 1024 * 1024;
    }

    /**
     * Called once by the coordinator at close time to deliver the total CPU nanoseconds spent on
     * background threads (segmentator + all parser threads) back into this reader's own counters.
     * <p>
     * The default no-op is correct for readers that do not track read CPU.
     */
    default void acceptReadCpuNanos(long nanos) {}

}
