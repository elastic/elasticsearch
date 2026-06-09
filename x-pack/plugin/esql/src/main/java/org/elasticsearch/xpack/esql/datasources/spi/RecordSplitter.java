/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;

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
     * record boundary, not a byte inside the open tail.
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
}
