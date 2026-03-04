/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.zstd;

import com.github.luben.zstd.ZstdInputStream;

import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Zstd decompression codec for compound extensions like {@code .csv.zst} or {@code .ndjson.zstd}.
 *
 * <p>Uses {@link ZstdInputStream} from zstd-jni for streaming decompression.
 */
public class ZstdDecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".zst", ".zstd");

    @Override
    public String name() {
        return "zstd";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        return new ZstdInputStream(raw);
    }
}
