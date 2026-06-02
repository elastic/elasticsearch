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
     * {@code max_record_size} pragma.
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
     * Segments smaller than this will be merged with an adjacent segment.
     * <p>
     * Defaults to 1 MiB. ClickHouse benchmarks show 1 MiB chunks are optimal for
     * parallel parsing — 100 KB chunks are ~40% slower due to per-chunk overhead,
     * while 10 MiB chunks offer only marginal improvement.
     * Implementations may override to reflect their parsing overhead.
     */
    default long minimumSegmentSize() {
        return 1024 * 1024;
    }

}
