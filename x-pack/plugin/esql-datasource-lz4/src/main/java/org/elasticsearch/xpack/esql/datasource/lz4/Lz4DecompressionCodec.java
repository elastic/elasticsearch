/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lz4;

import net.jpountz.lz4.LZ4FrameInputStream;

import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * LZ4 decompression codec for compound extensions like {@code .csv.lz4} or {@code .ndjson.lz4}.
 *
 * <p>Uses the standard LZ4 Frame format (RFC, compatible with the {@code lz4} CLI tool).
 * The underlying {@link LZ4FrameInputStream} is provided by {@code lz4-java},
 * already on the server classpath via {@code libs/lz4}.
 */
public class Lz4DecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".lz4");

    @Override
    public String name() {
        return "lz4";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        return new LZ4FrameInputStream(raw);
    }
}
