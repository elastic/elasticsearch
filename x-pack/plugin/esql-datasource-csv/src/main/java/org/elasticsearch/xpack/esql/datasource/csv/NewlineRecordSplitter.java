/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Record-boundary splitter for the no-quote modes ({@link CsvFormatOptions.Mode#PLAIN} and
 * {@link CsvFormatOptions.Mode#ESCAPED}). Without quoting, a raw line terminator is always a
 * record boundary — in {@code ESCAPED} data an in-field newline is the two bytes {@code \}+{@code n},
 * never raw — so boundary detection is a plain terminator scan with no quote-state machine. That is
 * what makes this path cheap; it must never grow per-byte mode state. (Structurally the same scan
 * as the NDJSON splitter, which relies on the same no-raw-newlines-in-a-record property.)
 */
final class NewlineRecordSplitter implements RecordSplitter {

    private static final int SCAN_BUFFER_SIZE = 8 * 1024;

    private final int maxRecordBytes;

    NewlineRecordSplitter(int maxRecordBytes) {
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        this.maxRecordBytes = maxRecordBytes;
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        InputStream in = stream instanceof BufferedInputStream ? stream : new BufferedInputStream(stream, SCAN_BUFFER_SIZE);
        long consumed = 0;
        int b;
        while ((b = in.read()) != -1) {
            consumed++;
            if (consumed > maxRecordBytes) {
                return RECORD_TOO_LARGE;
            }
            if (b == '\n') {
                return consumed;
            }
            if (b == '\r') {
                // Same peek discipline as the sibling splitters (CsvRecordSplitter, NdJsonRecordSplitter):
                // a lone CR is a clean terminator and the peeked first byte of the next record is
                // restored, so a caller that keeps reading the stream loses nothing.
                in.mark(1);
                int next = in.read();
                if (next == '\n') {
                    consumed++;
                    return consumed > maxRecordBytes ? RECORD_TOO_LARGE : consumed;
                }
                if (next != -1) {
                    in.reset();
                }
                return consumed;
            }
        }
        return -1;
    }

    /**
     * No-quote records never contain embedded raw newlines, so a backward scan for a line
     * terminator is always correct.
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

    /**
     * Forward scan used only when the buffer exceeds the per-record budget, so the cap can be
     * enforced per record rather than rejecting the whole buffer on a backward scan.
     */
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
}
