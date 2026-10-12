/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gzip;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Gzip decompression codec for compound extensions like {@code .csv.gz} or {@code .ndjson.gz}.
 *
 * <p>Uses {@link java.util.zip.GZIPInputStream} from the JDK; no external dependencies.
 * The JDK decoder (21, 22, 27+) concatenates members after a trailer only when
 * {@code available() > 0} or leftover inflater bytes {@code n > 26}. HTTP/S3 bodies report 0, so
 * a leftover {@code n <= 26} or a next member still on the wire is dropped.
 * {@link AtLeastOneAvailableInputStream} restores the JDK 23–26 leftover-buffer probe.
 */
public class GzipDecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".gz", ".gzip");

    /**
     * Raw-side read buffer handed to the {@link GZIPInputStream}{@code (InputStream, int size)}
     * constructor. The JDK default is 512 bytes, which forces a JNI trip into zlib for every
     * kilobyte of compressed data and dominates wall time for large files. 64 KiB sits at
     * the knee of the throughput-vs-buffer-size curve: it captures roughly +20% inflate
     * throughput over the default on JDK 26 / aarch64, and going beyond 64 KiB buys less
     * than 2% (well within run-to-run noise) while linearly increasing the per-open-stream
     * heap footprint. See {@code GzipInflateBenchmark} for the sweep data.
     */
    private static final int RAW_BUFFER_SIZE = 64 * 1024;

    /**
     * Native zlib inflate window ({@code MAX_WBITS=15} → 32 KiB) plus {@code inflate_state}
     * (~8 KiB). Charged against the query breaker for the life of the stream, matching zstd's
     * DStream accounting. The Java 64 KiB raw buffer is heap and already visible to GC.
     */
    static final long NATIVE_INFLATER_BYTES = 40L * 1024;

    static final String BREAKER_LABEL = "gzip-inflater";

    @Override
    public String name() {
        return "gzip";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        return decompress(raw, null);
    }

    @Override
    public InputStream decompress(InputStream raw, @Nullable CircuitBreaker breaker) throws IOException {
        AtLeastOneAvailableInputStream probe = new AtLeastOneAvailableInputStream(raw);
        GZIPInputStream gzip = new GZIPInputStream(probe, RAW_BUFFER_SIZE);
        InputStream decoded = gzip;
        if (breaker != null) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(NATIVE_INFLATER_BYTES, BREAKER_LABEL);
            } catch (Throwable t) {
                // inf.end() via GZIPInputStream.close(). Production wraps raw in UncloseableInputStream
                // first, so this does not drain the GET; DecompressingStorageObject.abortStream does.
                try {
                    gzip.close();
                } catch (Exception closeEx) {
                    t.addSuppressed(closeEx);
                }
                throw t;
            }
            decoded = new AccountedGzipInputStream(gzip, breaker, NATIVE_INFLATER_BYTES);
        }
        return new RethrowProbeFailureInputStream(decoded, probe);
    }

    /**
     * {@link GZIPInputStream#readTrailer} concatenates members when {@code in.available() > 0} or
     * leftover inflater bytes {@code n > 26} (JDK 21, 22, 27+). A 64 KiB fill of a tiny concatenated
     * object can leave {@code n <= 26} in the inflater buffer, and HTTP bodies report 0, so the
     * probe is skipped and those bytes are drained unread. Reporting at least 1 until a {@code read}
     * has seen EOF restores the leftover-buffer probe without a second GET. Also covers a next
     * member that is still on the wire ({@code n == 0} and {@code available() == 0}).
     */
    private static final class AtLeastOneAvailableInputStream extends FilterInputStream {
        private boolean seenEof;
        private IOException unexpectedFailure;

        AtLeastOneAvailableInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int available() throws IOException {
            if (seenEof) {
                return 0;
            }
            int n = in.available();
            return n > 0 ? n : 1;
        }

        @Override
        public int read() throws IOException {
            try {
                int b = in.read();
                if (b < 0) {
                    seenEof = true;
                }
                return b;
            } catch (IOException e) {
                unexpectedFailure = e;
                throw e;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                int n = in.read(b, off, len);
                if (n < 0) {
                    seenEof = true;
                }
                return n;
            } catch (IOException e) {
                unexpectedFailure = e;
                throw e;
            }
        }
    }

    /**
     * {@link GZIPInputStream#readTrailer} swallows {@link IOException} from the next-member probe and
     * treats it as EOS. The wrapper above forces that probe, so a transport reset at a member
     * boundary would otherwise look like a clean truncated object. Rethrow the stashed failure
     * when gzip reports {@code -1}.
     */
    private static final class RethrowProbeFailureInputStream extends FilterInputStream {
        private final AtLeastOneAvailableInputStream probe;

        private RethrowProbeFailureInputStream(InputStream decoded, AtLeastOneAvailableInputStream probe) {
            super(decoded);
            this.probe = probe;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            throwIfProbeFailed(b);
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            throwIfProbeFailed(n);
            return n;
        }

        private void throwIfProbeFailed(int n) throws IOException {
            if (n < 0 && probe.unexpectedFailure != null) {
                throw probe.unexpectedFailure;
            }
        }
    }

    /**
     * Refunds {@link #NATIVE_INFLATER_BYTES} on {@link #close()} (idempotent). Construction already
     * charged {@link CircuitBreaker#addEstimateBytesAndMaybeBreak}; this is the matching release.
     */
    private static final class AccountedGzipInputStream extends FilterInputStream {
        private final CircuitBreaker breaker;
        private final long charged;
        private boolean closed;

        private AccountedGzipInputStream(GZIPInputStream in, CircuitBreaker breaker, long charged) {
            super(in);
            this.breaker = breaker;
            this.charged = charged;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                in.close();
            } finally {
                breaker.addWithoutBreaking(-charged);
            }
        }
    }
}
