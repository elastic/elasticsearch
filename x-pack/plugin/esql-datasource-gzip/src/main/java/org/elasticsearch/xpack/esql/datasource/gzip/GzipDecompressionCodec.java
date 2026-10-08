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
        GZIPInputStream gzip = new GZIPInputStream(raw, RAW_BUFFER_SIZE);
        if (breaker == null) {
            return gzip;
        }
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
        return new AccountedGzipInputStream(gzip, breaker, NATIVE_INFLATER_BYTES);
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
