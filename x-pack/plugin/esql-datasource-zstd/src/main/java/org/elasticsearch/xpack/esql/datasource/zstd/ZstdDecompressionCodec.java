/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.zstd;

import org.elasticsearch.xpack.esql.datasource.compress.PanamaZstd;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Zstd decompression codec for compound extensions like {@code .csv.zst} or {@code .ndjson.zstd}.
 *
 * <p>Backed by the Panama FFI {@code ZSTD_decompressStream} binding through {@link PanamaZstd}, so
 * page reads no longer cross the zstd-jni JNI boundary on every refill. Matches Lucene's stance on
 * native zstd availability: the constructor hard-fails when {@link PanamaZstd#isAvailable()} is
 * {@code false}, surfacing the failure at plugin load (via
 * {@link ZstdDataSourcePlugin#decompressionCodecs}) rather than at first query — there is no
 * zstd-jni fallback on this streaming path.
 */
public class ZstdDecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".zst", ".zstd");

    private final PanamaZstd panamaZstd;

    public ZstdDecompressionCodec() {
        this(PanamaZstd.instance());
    }

    ZstdDecompressionCodec(PanamaZstd panamaZstd) {
        if (panamaZstd.isAvailable() == false) {
            throw new IllegalStateException(
                "Native zstd is not available on this platform [os.name="
                    + System.getProperty("os.name")
                    + ", os.arch="
                    + System.getProperty("os.arch")
                    + "]; ESQL .csv.zst / .ndjson.zstd streaming requires the libzstd binding shipped with Elasticsearch"
            );
        }
        this.panamaZstd = panamaZstd;
    }

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
        return panamaZstd.wrap(raw);
    }
}
