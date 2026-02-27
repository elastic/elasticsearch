/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Bzip2 decompression codec for compound extensions like {@code .csv.bz2} or {@code .ndjson.bz}.
 *
 * <p>Uses {@link BZip2CompressorInputStream} from Apache Commons Compress.
 */
public class Bzip2DecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".bz2", ".bz");

    @Override
    public String name() {
        return "bzip2";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        return new BZip2CompressorInputStream(raw);
    }
}
