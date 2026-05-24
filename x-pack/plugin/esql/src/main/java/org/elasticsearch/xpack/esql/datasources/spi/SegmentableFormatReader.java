/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

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
     * Scans forward from the current position in the stream to find the start of
     * the next complete record. Returns the number of bytes consumed (skipped)
     * to reach that boundary.
     * <p>
     * For newline-delimited formats (CSV, NDJSON), this means scanning until
     * the first {@code \n} or {@code \r\n} and returning the byte count
     * including the line terminator. The next byte in the stream is the start
     * of a complete record.
     * <p>
     * <b>Note on quoting:</b> Implementations for formats that support quoting
     * (e.g. CSV with quoted fields containing embedded newlines) should either
     * track quoting state during the scan or document that parallel parsing is
     * not safe for files with embedded newlines in quoted fields.
     * <p>
     * The stream is positioned at an arbitrary byte offset within the file
     * (typically a segment boundary). The implementation must consume bytes
     * until it finds a record boundary, leaving the stream positioned at the
     * start of the next record.
     *
     * @param stream an open stream positioned at an arbitrary offset within the file
     * @return the number of bytes consumed to reach the next record boundary,
     *         or {@code -1} if the end of stream is reached without finding a boundary
     * @throws IOException if an I/O error occurs while scanning
     */
    long findNextRecordBoundary(InputStream stream) throws IOException;

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

    /**
     * Returns the offset of the byte that terminates the latest complete record within
     * {@code buf[0..length)}, or {@code -1} if no complete record terminates inside the buffer.
     * Used by streaming-parallel chunkers to slice on a record boundary; bytes after the offset
     * are carried into the next chunk.
     * <p>
     * <b>Open-tail contract:</b> when the tail is mid-record (e.g. an unterminated quoted cell),
     * implementations MUST return the offset of the last complete record that <em>precedes</em>
     * the open region (or {@code -1} if none). Returning an offset inside the open region would
     * dispatch a malformed chunk.
     * <p>
     * Default: drives {@link #findNextRecordBoundary} forward through the buffer, so embedded
     * newlines inside quoted fields are handled correctly by any implementation that tracks
     * quote state in its boundary scanner. A sub-stream is created per record because
     * {@code findNextRecordBoundary} implementations may read beyond the returned boundary
     * (e.g. via internal bulk-read buffers); each sub-stream is a lightweight view into the
     * same backing array with no data copying.
     */
    default int findLastRecordBoundary(byte[] buf, int length) throws IOException {
        if (length <= 0) {
            return -1;
        }
        int lastBoundary = -1;
        int cumulative = 0;
        while (cumulative < length) {
            long consumed = findNextRecordBoundary(new ByteArrayInputStream(buf, cumulative, length - cumulative));
            if (consumed < 0) {
                return lastBoundary;
            }
            cumulative += Math.toIntExact(consumed);
            lastBoundary = cumulative - 1;
        }
        return lastBoundary;
    }
}
