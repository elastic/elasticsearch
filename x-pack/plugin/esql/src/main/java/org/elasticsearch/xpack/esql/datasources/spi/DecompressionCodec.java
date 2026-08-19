/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * SPI for decompression codecs. Plugins implement this to provide decompression
 * for compound extensions like {@code .csv.gz} or {@code .ndjson.gz}.
 *
 * <p>When a path ends with a known compression extension (e.g. {@code .gz}),
 * the framework strips it, resolves the inner format, and wraps the raw
 * {@link StorageObject} stream with {@link #decompress(InputStream)} before
 * delegating to the inner format reader.
 *
 * <p>Stream-only codecs (gzip, zstd) do not support random access; formats
 * that require {@link StorageObject#newStream(long, long)} (e.g. Parquet, ORC)
 * are not supported for compressed files.
 */
public interface DecompressionCodec {

    /**
     * Codec name for logging and diagnostics (e.g. "gzip").
     */
    String name();

    /**
     * File extensions this codec handles (with leading dot, e.g. [".gz", ".gzip"]).
     */
    List<String> extensions();

    /**
     * Wraps the raw compressed input stream with a decompressing stream.
     *
     * @param raw the compressed input stream
     * @return an input stream that yields decompressed bytes
     */
    InputStream decompress(InputStream raw) throws IOException;

    /**
     * Breaker-aware variant of {@link #decompress(InputStream)}. Codecs that hold a native
     * decompression footprint (e.g. zstd's streaming context) override this to account that
     * footprint against {@code breaker}. {@code breaker} may be {@code null}; implementations must
     * skip accounting in that case. The default implementation ignores the breaker and
     * delegates to {@link #decompress(InputStream)}, preserving existing behavior for codecs
     * (gzip, bzip2) with no native reservation to track.
     *
     * @param raw     the compressed input stream
     * @param breaker circuit breaker to account the decompressor's native footprint against, or
     *                {@code null} to skip accounting
     * @return an input stream that yields decompressed bytes
     */
    default InputStream decompress(InputStream raw, @Nullable CircuitBreaker breaker) throws IOException {
        return decompress(raw);
    }
}
