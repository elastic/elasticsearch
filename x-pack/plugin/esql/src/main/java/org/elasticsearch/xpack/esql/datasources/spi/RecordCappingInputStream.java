/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Stream wrapper that enforces the {@code max_record_size} byte cap on every record without
 * decoding characters or visiting bytes outside the bulk-read fast path. The byte counter starts at
 * zero, accumulates every byte (including line terminators), and resets when an unquoted record
 * terminator is observed: {@code '\n'}, {@code '\r'}, or {@code '\r\n'}. The cap matches the
 * terminator-inclusive byte length used by record splitters so an oversized record fails at every
 * layer with the same threshold.
 *
 * <p>Format-specific subclasses provide the {@link #recordTooLarge() exception factory}; the rest
 * of the state machine — single-byte / bulk read accounting, CRLF spanning across reads, mark/reset
 * lockout — is shared. This keeps the CSV and NDJSON cap streams from drifting in lockstep when one
 * gets a CRLF or boundary-handling fix.
 *
 * <p>The hot path is {@link #read(byte[], int, int)}: it delegates a single bulk read to the
 * underlying stream and then sweeps the freshly populated region in one tight loop. {@link #read()}
 * is wired to the same accounting but is not on the throughput-critical path (the downstream parser
 * buffers in bulk).
 *
 * <p>{@code mark}/{@code reset} are disabled to keep the byte counter monotonic with the bytes the
 * downstream parser consumes; a rewind would skew the cap relative to the actual record boundaries.
 *
 * <p><b>Semantics under lenient error policies:</b> a thrown {@link IOException} from a bulk read
 * leaves the underlying stream in an undefined position (the parser may have buffered partial bytes
 * ahead of the trip), so the wrapper cannot resume after a cap failure. Callers that need row-level
 * recovery under lenient policy must keep this wrapper out of those paths.
 */
public abstract class RecordCappingInputStream extends FilterInputStream {

    private final int maxRecordBytes;
    /** Bytes accumulated since the last terminator. Reset to zero on {@code \n} / lone {@code \r} / {@code \r\n}. */
    private long bytesSinceLastTerminator;
    /** True iff the previous byte (across reads) was a {@code \r}; a leading {@code \n} on the next read pairs with it as CRLF. */
    private boolean pendingCr;

    protected RecordCappingInputStream(InputStream in, int maxRecordBytes) {
        super(in);
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        this.maxRecordBytes = maxRecordBytes;
    }

    /**
     * Format-specific exception thrown when a record exceeds the byte cap. Subclasses should return
     * an exception with the same {@code "record exceeded max_record_size [N]"} message shape used by
     * the matching record splitter so log lines stay consistent across enforcement points.
     */
    protected abstract IOException recordTooLarge();

    @Override
    public final int read() throws IOException {
        int b = in.read();
        if (b < 0) {
            return b;
        }
        if (pendingCr) {
            pendingCr = false;
            if (b == '\n') {
                // CRLF: the LF closes the same record that the prior CR opened the terminator for. Count it toward
                // that record, check the cap once more, then reset so the next byte starts a fresh run.
                bytesSinceLastTerminator++;
                if (bytesSinceLastTerminator > maxRecordBytes) {
                    throw recordTooLarge();
                }
                bytesSinceLastTerminator = 0;
                return b;
            }
            // Lone CR: the prior CR was itself the terminator. Reset now so the current byte starts a fresh record.
            bytesSinceLastTerminator = 0;
        }
        bytesSinceLastTerminator++;
        if (bytesSinceLastTerminator > maxRecordBytes) {
            throw recordTooLarge();
        }
        if (b == '\n') {
            // LF terminator of a non-CR-prefixed record: reset immediately.
            bytesSinceLastTerminator = 0;
        } else if (b == '\r') {
            // Defer the reset until the next read disambiguates lone CR vs CRLF; the count is already cap-validated.
            pendingCr = true;
        }
        return b;
    }

    @Override
    public final int read(byte[] buf, int off, int len) throws IOException {
        int n = in.read(buf, off, len);
        if (n <= 0) {
            return n;
        }
        scanRegion(buf, off, n);
        return n;
    }

    @Override
    public final boolean markSupported() {
        return false;
    }

    @Override
    public final synchronized void mark(int readLimit) {
        // Intentional no-op: see class-level Javadoc on monotonic counter invariant.
    }

    @Override
    public final synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported on " + getClass().getSimpleName());
    }

    private void scanRegion(byte[] buf, int off, int len) throws IOException {
        int end = off + len;
        long runningBytes = bytesSinceLastTerminator;
        int i = off;
        if (pendingCr) {
            pendingCr = false;
            if (buf[i] == '\n') {
                // CRLF spanning two reads: count the LF toward the same record before resetting (matches the
                // splitter byte-extent rule, which counts both CR and LF toward the terminated record).
                runningBytes++;
                if (runningBytes > maxRecordBytes) {
                    bytesSinceLastTerminator = runningBytes;
                    throw recordTooLarge();
                }
                runningBytes = 0;
                i++;
            } else {
                // Lone CR carried across reads: the prior CR was itself the terminator. Reset before counting the
                // current byte as the first byte of the new record below.
                runningBytes = 0;
            }
        }
        for (; i < end; i++) {
            byte b = buf[i];
            runningBytes++;
            if (runningBytes > maxRecordBytes) {
                // Mirror the splitter's recordExceedsLimit check: include the terminator byte in the cap test so
                // the failure threshold matches the splitter and the legacy logical-record reader to the byte.
                bytesSinceLastTerminator = runningBytes;
                throw recordTooLarge();
            }
            if (b == '\n') {
                runningBytes = 0;
                continue;
            }
            if (b == '\r') {
                if (i + 1 < end) {
                    if (buf[i + 1] == '\n') {
                        runningBytes++;
                        if (runningBytes > maxRecordBytes) {
                            bytesSinceLastTerminator = runningBytes;
                            throw recordTooLarge();
                        }
                        i++;
                    }
                    runningBytes = 0;
                } else {
                    // CR is the last byte of this read; defer the reset to the next call which will see either an LF
                    // (CRLF) or a non-LF (lone CR) and account for it accordingly.
                    pendingCr = true;
                }
            }
        }
        bytesSinceLastTerminator = runningBytes;
    }
}
