/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gzip;

import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

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
        return new GZIPInputStream(raw, RAW_BUFFER_SIZE);
    }
}
