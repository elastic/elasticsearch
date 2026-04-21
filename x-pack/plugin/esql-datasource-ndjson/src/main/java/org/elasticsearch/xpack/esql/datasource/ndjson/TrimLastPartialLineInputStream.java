/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.core.util.Check;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Wraps an NDJSON byte stream and exposes only bytes through the last {@code '\n'} in the stream,
 * dropping a trailing partial line (split boundary). Reads the delegate lazily and keeps at most a
 * small read buffer plus any uncommitted tail after the last newline seen so far.
 */
final class TrimLastPartialLineInputStream extends InputStream {

    private static final char SPLIT_BOUNDARY = '\n';

    static final ByteSizeValue MAX_CARRY = ByteSizeValue.ofMb(32);
    /** Max bytes held in {@link #carry} when no {@code '\n'} has been seen yet (pathological single line). */
    static final long MAX_CARRY_BYTES = MAX_CARRY.getBytes();

    private final InputStream delegate;
    private final byte[] chunk;

    /** Bytes after the last {@code '\n'} observed across all chunks read so far; discarded at EOF. */
    private byte[] carry;

    private byte[] buffer = new byte[8192];
    private int readIdx;
    private int writeIdx;
    private boolean eof;
    private final byte[] single = new byte[1];

    TrimLastPartialLineInputStream(InputStream delegate, int chunkSize) {
        this.delegate = delegate;
        Check.isTrue(chunkSize > 0, "chunkSize must strictly positive");
        this.chunk = new byte[chunkSize];
    }

    @Override
    public int read() throws IOException {
        int n = read(single, 0, 1);
        return n < 0 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte @NonNull [] b, int off, int len) throws IOException {
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
                        throw carryLimitExceeded();
                    }
                    carry = Arrays.copyOfRange(chunk, lastNl + 1, n);
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

    private static byte[] append(byte[] prefix, byte[] data, int n) throws IOException {
        if (n <= 0) {
            return prefix;
        }
        long newLen = (prefix == null || prefix.length == 0) ? n : (long) prefix.length + n;
        if (newLen > MAX_CARRY_BYTES) {
            throw carryLimitExceeded();
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
