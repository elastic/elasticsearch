/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.snappy;

import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.xerial.snappy.SnappyFramedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Snappy decompression codec for compound extensions like {@code .csv.snappy} or {@code .ndjson.snappy}.
 *
 * <p>Uses the standard Snappy framing format (64KB chunks with CRC-32C checksums).
 * CRC-32C verification is enabled by default to detect silent data corruption
 * from remote blob stores. The underlying {@link SnappyFramedInputStream} is
 * provided by snappy-java via the {@code esql-datasource-compression-libs} parent plugin.
 */
public class SnappyDecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".snappy");

    @Override
    public String name() {
        return "snappy";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        return new SnappyFramedInputStream(raw, /* verifyCrc32c= */ true);
    }
}
