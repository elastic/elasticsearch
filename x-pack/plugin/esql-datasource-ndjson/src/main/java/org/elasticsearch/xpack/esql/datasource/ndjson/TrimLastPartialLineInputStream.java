/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Wraps an NDJSON byte stream and exposes only bytes through the last {@code '\n'} in the stream,
 * dropping a trailing partial line (split boundary). Reads the delegate lazily and keeps at most a
 * small read buffer plus any uncommitted tail after the last newline seen so far.
 *
 * <p>If a line without a delimiter exceeds {@link #MAX_CARRY_BYTES}, {@link ErrorPolicy#isStrict()}
 * causes an {@link IOException}; otherwise the buffered partial line is discarded as bogus and
 * reading continues. Discards are logged like {@link NdJsonPageDecoder} parse skips:
 * {@link Level#INFO} when {@link ErrorPolicy#logErrors()} is true, otherwise {@link Level#DEBUG}.
 *
 * <p>Only line feed ({@code '\n'}) is a record boundary here. A carriage return from CRLF that is
 * split across read chunks can remain as a trailing {@code '\r'} on the emitted line (pre-existing
 * limitation for Windows-style line endings in this path).
 */
final class TrimLastPartialLineInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(TrimLastPartialLineInputStream.class);

    private static final int DEFAULT_TRIM_CHUNK_SIZE = 8192;
    /** Record delimiter for trimming (LF only; see class Javadoc for CRLF). */
    private static final char SPLIT_BOUNDARY = '\n';

    static final ByteSizeValue MAX_CARRY = ByteSizeValue.ofMb(32);
    /** Max bytes held in {@link #carry} when no {@code '\n'} has been seen yet (pathological single line). */
    static final long MAX_CARRY_BYTES = MAX_CARRY.getBytes();

    private final InputStream delegate;
    private final byte[] chunk;
    private final ErrorPolicy errorPolicy;
    private final SkipWarnings skipWarnings;

    /** Bytes after the last {@code '\n'} observed across all chunks read so far; discarded at EOF. */
    private byte[] carry;

    private byte[] buffer = new byte[DEFAULT_TRIM_CHUNK_SIZE];
    private int readIdx;
    private int writeIdx;
    private boolean eof;
    private final byte[] single = new byte[1];

    TrimLastPartialLineInputStream(InputStream delegate, ErrorPolicy errorPolicy, String sourceLocation) {
        this(delegate, DEFAULT_TRIM_CHUNK_SIZE, errorPolicy, sourceLocation);
    }

    TrimLastPartialLineInputStream(InputStream delegate, int chunkSize, ErrorPolicy errorPolicy, String sourceLocation) {
        this.delegate = delegate;
        Check.isTrue(chunkSize > 0, "chunkSize must strictly positive");
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        this.chunk = new byte[chunkSize];
        this.errorPolicy = errorPolicy;
        this.skipWarnings = SkipWarnings.of(
            errorPolicy,
            "NDJSON read from [" + sourceLocation + "] discarded an oversized partial line (policy: " + errorPolicy.modeName() + ")"
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
            int lastNl = -1;
            for (int i = n - 1; i >= 0; i--) {
                if (chunk[i] == SPLIT_BOUNDARY) {
                    lastNl = i;
                    break;
                }
            }
            if (lastNl >= 0) {
                int carryLen = carry == null ? 0 : carry.length;
                int emitLen = carryLen + lastNl + 1;
                ensureWritable(emitLen);
                if (carryLen > 0) {
                    System.arraycopy(carry, 0, buffer, writeIdx, carryLen);
                    writeIdx += carryLen;
                    carry = null;
                }
                System.arraycopy(chunk, 0, buffer, writeIdx, lastNl + 1);
                writeIdx += lastNl + 1;
                if (lastNl + 1 < n) {
                    int tailLen = n - (lastNl + 1);
                    if (tailLen > MAX_CARRY_BYTES) {
                        if (errorPolicy.isStrict()) {
                            throw carryLimitExceeded();
                        }
                        logDiscardedOversizedPartial(tailLen, "trailing_fragment_after_delimiter");
                        carry = null;
                    } else {
                        carry = Arrays.copyOfRange(chunk, lastNl + 1, n);
                    }
                }
                return true;
            }
            carry = append(carry, chunk, n);
        }
        return true;
    }

    private static IOException carryLimitExceeded() {
        return new IOException("NDJSON lines longer than [" + MAX_CARRY + "] are not supported");
    }

    /** Same level choice as {@link NdJsonPageDecoder#onNdjsonLineParseError}. */
    private void logDiscardedOversizedPartial(long discardedBytes, String kind) {
        skipWarnings.add(
            "Skipping NDJSON ["
                + kind
                + "] of approximately ["
                + discardedBytes
                + "] bytes while trimming split suffix; limit is ["
                + MAX_CARRY
                + "]"
        );
        logger.log(
            errorPolicy.logErrors() ? Level.INFO : Level.DEBUG,
            LoggerMessageFormat.format(
                "Skipping NDJSON [{}] of approximately [{}] bytes while trimming split suffix; limit is [{}]",
                kind,
                discardedBytes,
                MAX_CARRY
            )
        );
    }

    private byte[] append(byte[] prefix, byte[] data, int n) throws IOException {
        if (n <= 0) {
            return prefix;
        }
        long newLen = (prefix == null || prefix.length == 0) ? n : (long) prefix.length + n;
        if (newLen > MAX_CARRY_BYTES) {
            if (errorPolicy.isStrict()) {
                throw carryLimitExceeded();
            }
            long dropped = prefix == null ? 0 : (long) prefix.length;
            logDiscardedOversizedPartial(dropped + n, "partial_line_without_delimiter");
            // Bogus line: drop the partial tail buffered so far; continue with this chunk only.
            return Arrays.copyOfRange(data, 0, n);
        }
        if (prefix == null || prefix.length == 0) {
            return Arrays.copyOfRange(data, 0, n);
        }
        byte[] out = Arrays.copyOf(prefix, prefix.length + n);
        System.arraycopy(data, 0, out, prefix.length, n);
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
