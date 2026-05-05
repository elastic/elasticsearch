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
        return new GZIPInputStream(raw);
    }
}
