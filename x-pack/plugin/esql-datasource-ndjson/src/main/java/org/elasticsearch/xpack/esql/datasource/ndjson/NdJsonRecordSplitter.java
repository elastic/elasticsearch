/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * NDJSON record-boundary splitter for byte-oriented parallel parsing.
 */
final class NdJsonRecordSplitter implements RecordSplitter {

    private static final int SCAN_BUFFER_SIZE = 8 * 1024;

    private final int maxRecordBytes;

    NdJsonRecordSplitter(int maxRecordBytes) {
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        this.maxRecordBytes = maxRecordBytes;
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        // The caller only cares about the byte offset of the terminator; a lone CR followed by
        // some non-LF byte may have been consumed from the stream by the scanner, but the
        // caller discards the stream after this call so that is acceptable.
        // Wrap cold streams to restore the 8 KB fast path; if already buffered, pass through.
        InputStream buffered = stream instanceof BufferedInputStream ? stream : new BufferedInputStream(stream, SCAN_BUFFER_SIZE);
        return scanForTerminator(buffered).consumed();
    }

    /**
     * NDJSON records never contain embedded newlines, so a backward scan for a line terminator
     * is always correct and offset-aware without copying.
     */
    @Override
    public int findLastRecordBoundary(byte[] buf, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, buf.length);
        if (length > maxRecordBytes) {
            return findLastRecordBoundaryByForwardScan(buf, offset, length);
        }
        for (int i = offset + length - 1; i >= offset; i--) {
            byte b = buf[i];
            if (b == '\n' || b == '\r') {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int maxRecordBytes() {
        return maxRecordBytes;
    }

    IOException recordTooLargeException() {
        return new IOException("NDJSON line exceeded max_record_size [" + maxRecordBytes + "]");
    }

    private int findLastRecordBoundaryByForwardScan(byte[] buf, int offset, int length) {
        int end = offset + length;
        int recordStart = offset;
        int lastBoundary = -1;
        for (int i = offset; i < end; i++) {
            byte b = buf[i];
            if (b == '\n') {
                if (recordExceedsLimit(recordStart, i)) {
                    return lastBoundary >= 0 ? lastBoundary : (int) RECORD_TOO_LARGE;
                }
                lastBoundary = i;
                recordStart = i + 1;
            } else if (b == '\r') {
                int boundary = i;
                if (i + 1 < end && buf[i + 1] == '\n') {
                    boundary = ++i;
                }
                if (recordExceedsLimit(recordStart, boundary)) {
                    return lastBoundary >= 0 ? lastBoundary : (int) RECORD_TOO_LARGE;
                }
                lastBoundary = boundary;
                recordStart = boundary + 1;
            }
        }
        return end - recordStart > maxRecordBytes && lastBoundary < 0 ? (int) RECORD_TOO_LARGE : lastBoundary;
    }

    private boolean recordExceedsLimit(int recordStart, int boundary) {
        return boundary - recordStart + 1 > maxRecordBytes;
    }

    /**
     * Reads one byte at a time from {@code in} until a record terminator (LF, CRLF, or lone CR)
     * is consumed. Returns {@link LineScan#consumed} as the number of bytes read through-and-
     * including the terminator; for the lone-CR case the byte that follows (which is the first
     * byte of the next record) is read from the stream and exposed via {@link LineScan#peekedByte}
     * so callers can preserve it. Returns {@link LineScan#EOF} if the stream ends before any
     * terminator is seen.
     */
    LineScan scanForTerminator(InputStream in) throws IOException {
        long consumed = 0;
        int b;
        while ((b = in.read()) != -1) {
            consumed++;
            if (consumed > maxRecordBytes) {
                return LineScan.RECORD_TOO_LARGE;
            }
            if (b == '\n') {
                return new LineScan(consumed, -1);
            }
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    consumed++;
                    return consumed > maxRecordBytes ? LineScan.RECORD_TOO_LARGE : new LineScan(consumed, -1);
                }
                // EOF after CR is reported as a clean terminator with no peeked byte.
                return new LineScan(consumed, next);
            }
        }
        return LineScan.EOF;
    }

    /** Outcome of a single scan for the next record terminator. */
    record LineScan(long consumed, int peekedByte) {
        /** Sentinel returned when the stream ended before any terminator. */
        static final LineScan EOF = new LineScan(-1, -1);
        /** Sentinel returned when scanning past the configured record-size budget. */
        static final LineScan RECORD_TOO_LARGE = new LineScan(RecordSplitter.RECORD_TOO_LARGE, -1);
    }
}
