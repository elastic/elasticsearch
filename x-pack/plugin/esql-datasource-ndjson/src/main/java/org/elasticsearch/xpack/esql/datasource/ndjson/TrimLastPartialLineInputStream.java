/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Wraps an NDJSON byte stream and exposes only bytes through the last {@code '\n'} in the stream,
 * dropping a trailing partial line (split boundary). Reads the delegate lazily and keeps at most a
 * small read buffer plus any uncommitted tail after the last newline seen so far.
 *
 * <p>If a line without a delimiter exceeds the configured {@code max_record_size}, {@link ErrorPolicy#isStrict()}
 * causes an {@link IOException}; otherwise the buffered partial line is discarded as bogus and
 * reading continues. Discards are logged like {@link NdJsonPageDecoder} parse skips:
 * {@link Level#INFO} when {@link ErrorPolicy#logErrors()} is true, otherwise {@link Level#DEBUG}.
 */
final class TrimLastPartialLineInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(TrimLastPartialLineInputStream.class);

    private static final int DEFAULT_TRIM_CHUNK_SIZE = 8192;

    private final InputStream delegate;
    private final byte[] chunk;
    private final ErrorPolicy errorPolicy;
    private final SkipWarnings skipWarnings;
    private final NdJsonRecordSplitter recordSplitter;

    /** Bytes after the last {@code '\n'} observed across all chunks read so far; discarded at EOF. */
    private byte[] carry;

    private byte[] buffer = new byte[DEFAULT_TRIM_CHUNK_SIZE];
    private int readIdx;
    private int writeIdx;
    private boolean eof;
    private boolean discardingOversizedPartial;
    private boolean discardingOptionalLfAfterCr;
    private final byte[] single = new byte[1];

    TrimLastPartialLineInputStream(InputStream delegate, ErrorPolicy errorPolicy, String sourceLocation) {
        this(
            delegate,
            DEFAULT_TRIM_CHUNK_SIZE,
            errorPolicy,
            sourceLocation,
            new NdJsonRecordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES),
            null
        );
    }

    TrimLastPartialLineInputStream(InputStream delegate, int chunkSize, ErrorPolicy errorPolicy, String sourceLocation) {
        this(
            delegate,
            chunkSize,
            errorPolicy,
            sourceLocation,
            new NdJsonRecordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES),
            null
        );
    }

    TrimLastPartialLineInputStream(
        InputStream delegate,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonRecordSplitter recordSplitter
    ) {
        this(delegate, DEFAULT_TRIM_CHUNK_SIZE, errorPolicy, sourceLocation, recordSplitter, null);
    }

    TrimLastPartialLineInputStream(
        InputStream delegate,
        int chunkSize,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonRecordSplitter recordSplitter
    ) {
        this(delegate, chunkSize, errorPolicy, sourceLocation, recordSplitter, null);
    }

    TrimLastPartialLineInputStream(
        InputStream delegate,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonRecordSplitter recordSplitter,
        @Nullable Consumer<String> warningSink
    ) {
        this(delegate, DEFAULT_TRIM_CHUNK_SIZE, errorPolicy, sourceLocation, recordSplitter, warningSink);
    }

    TrimLastPartialLineInputStream(
        InputStream delegate,
        int chunkSize,
        ErrorPolicy errorPolicy,
        String sourceLocation,
        NdJsonRecordSplitter recordSplitter,
        @Nullable Consumer<String> warningSink
    ) {
        this.delegate = delegate;
        Check.isTrue(chunkSize > 0, "chunkSize must strictly positive");
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        this.chunk = new byte[chunkSize];
        this.errorPolicy = errorPolicy;
        Check.isTrue(recordSplitter != null, "recordSplitter must not be null");
        this.recordSplitter = recordSplitter;
        this.skipWarnings = SkipWarnings.of(
            errorPolicy,
            "NDJSON read from [" + sourceLocation + "] discarded an oversized partial line (policy: " + errorPolicy.modeName() + ")",
            warningSink
        );
    }

    @Override
    public int read() throws IOException {
        int n = read(single, 0, 1);
        return n < 0 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int total = 0;
        while (len > 0) {
            if (readIdx < writeIdx) {
                int n = Math.min(len, writeIdx - readIdx);
                System.arraycopy(buffer, readIdx, b, off, n);
                readIdx += n;
                off += n;
                len -= n;
                total += n;
                continue;
            }
            if (eof) {
                return total > 0 ? total : -1;
            }
            if (fillMore() == false) {
                return total > 0 ? total : -1;
            }
        }
        return total;
    }

    private boolean fillMore() throws IOException {
        while (readIdx >= writeIdx) {
            int n = delegate.read(chunk);
            if (n < 0) {
                eof = true;
                carry = null;
                return readIdx < writeIdx;
            }
            if (n == 0) {
                continue;
            }
            int start = skipDiscardedOversizedPartial(chunk, n);
            if (start == n) {
                continue;
            }
            int scanLength = n - start;
            int lastBoundary = recordSplitter.findLastRecordBoundary(chunk, start, scanLength);
            if (lastBoundary == RecordSplitter.RECORD_TOO_LARGE) {
                if (errorPolicy.isStrict()) {
                    throw carryLimitExceeded();
                }
                logDiscardedOversizedPartial(scanLength, "partial_line_without_delimiter");
                carry = null;
                discardingOversizedPartial = true;
                continue;
            }
            if (lastBoundary >= 0) {
                int carryLen = carry == null ? 0 : carry.length;
                int emitLen = carryLen + (lastBoundary - start) + 1;
                if (emitLen > recordSplitter.maxRecordBytes()) {
                    if (errorPolicy.isStrict()) {
                        throw carryLimitExceeded();
                    }
                    logDiscardedOversizedPartial(emitLen, "record_before_delimiter");
                    carry = tailAfterBoundary(chunk, lastBoundary, n);
                    discardingOversizedPartial = false;
                    if (carry == null && lastBoundary == n - 1 && chunk[lastBoundary] == '\r') {
                        discardingOptionalLfAfterCr = true;
                    }
                    continue;
                }
                ensureWritable(emitLen);
                if (carryLen > 0) {
                    System.arraycopy(carry, 0, buffer, writeIdx, carryLen);
                    writeIdx += carryLen;
                    carry = null;
                }
                int chunkEmitLen = lastBoundary - start + 1;
                System.arraycopy(chunk, start, buffer, writeIdx, chunkEmitLen);
                writeIdx += chunkEmitLen;
                carry = tailAfterBoundary(chunk, lastBoundary, n);
                return true;
            }
            carry = append(carry, chunk, start, scanLength);
        }
        return true;
    }

    private int skipDiscardedOversizedPartial(byte[] data, int n) throws IOException {
        int start = 0;
        if (discardingOptionalLfAfterCr) {
            discardingOptionalLfAfterCr = false;
            if (n > 0 && data[0] == '\n') {
                start = 1;
            }
        }
        if (discardingOversizedPartial == false) {
            return start;
        }
        NdJsonRecordSplitter.LineScan scan = recordSplitter.scanForTerminator(new ByteArrayInputStream(data, start, n - start));
        if (scan.consumed() < 0) {
            return n;
        }
        discardingOversizedPartial = false;
        int consumed = start + Math.toIntExact(scan.consumed());
        if (consumed == n && data[n - 1] == '\r') {
            discardingOptionalLfAfterCr = true;
        }
        return consumed;
    }

    private byte[] tailAfterBoundary(byte[] data, int boundary, int n) throws IOException {
        if (boundary + 1 >= n) {
            return null;
        }
        int tailLen = n - (boundary + 1);
        if (tailLen > recordSplitter.maxRecordBytes()) {
            if (errorPolicy.isStrict()) {
                throw carryLimitExceeded();
            }
            logDiscardedOversizedPartial(tailLen, "trailing_fragment_after_delimiter");
            discardingOversizedPartial = true;
            return null;
        }
        return Arrays.copyOfRange(data, boundary + 1, n);
    }

    private IOException carryLimitExceeded() {
        return recordSplitter.recordTooLargeException();
    }

    /** Same level choice as {@link NdJsonPageDecoder#onNdjsonLineParseError}. */
    private void logDiscardedOversizedPartial(long discardedBytes, String kind) {
        skipWarnings.add(
            "Skipping NDJSON ["
                + kind
                + "] of approximately ["
                + discardedBytes
                + "] bytes while trimming split suffix; limit is ["
                + recordSplitter.maxRecordBytes()
                + "]"
        );
        logger.log(
            errorPolicy.logErrors() ? Level.INFO : Level.DEBUG,
            LoggerMessageFormat.format(
                "Skipping NDJSON [{}] of approximately [{}] bytes while trimming split suffix; limit is [{}]",
                kind,
                discardedBytes,
                recordSplitter.maxRecordBytes()
            )
        );
    }

    private byte[] append(byte[] prefix, byte[] data, int offset, int n) throws IOException {
        if (n <= 0) {
            return prefix;
        }
        long newLen = (prefix == null || prefix.length == 0) ? n : (long) prefix.length + n;
        if (newLen > recordSplitter.maxRecordBytes()) {
            if (errorPolicy.isStrict()) {
                throw carryLimitExceeded();
            }
            long dropped = prefix == null ? 0 : (long) prefix.length;
            logDiscardedOversizedPartial(dropped + n, "partial_line_without_delimiter");
            // Bogus line: drop the partial tail buffered so far and discard through the next delimiter.
            discardingOversizedPartial = true;
            return null;
        }
        if (prefix == null || prefix.length == 0) {
            return Arrays.copyOfRange(data, offset, offset + n);
        }
        byte[] out = Arrays.copyOf(prefix, prefix.length + n);
        System.arraycopy(data, offset, out, prefix.length, n);
        return out;
    }

    private void ensureWritable(int additional) {
        if (writeIdx + additional <= buffer.length) {
            return;
        }
        int unread = writeIdx - readIdx;
        if (readIdx > 0 && unread + additional <= buffer.length) {
            System.arraycopy(buffer, readIdx, buffer, 0, unread);
            writeIdx = unread;
            readIdx = 0;
            return;
        }
        // Must fit a copy starting at writeIdx (not only the compacted tail unread + additional).
        int need = writeIdx + additional;
        int newLen = Math.max(buffer.length * 2, need);
        buffer = Arrays.copyOf(buffer, newLen);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegate);
    }
}
