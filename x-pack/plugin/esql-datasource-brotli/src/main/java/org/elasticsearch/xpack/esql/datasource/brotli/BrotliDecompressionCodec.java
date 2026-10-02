/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.brotli;

import org.brotli.dec.BrotliInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Brotli decompression codec for compound extensions like {@code .csv.br} or {@code .ndjson.br}.
 *
 * <p>Uses the standard Brotli stream format (RFC 7932). The underlying
 * {@link BrotliInputStream} is Google's official pure-Java Brotli decoder
 * ({@code org.brotli:dec}), which is decode-only with no native dependencies.
 */
public class BrotliDecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".br");

    @Override
    public String name() {
        return "brotli";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        return new BrotliInputStream(raw);
    }
}
