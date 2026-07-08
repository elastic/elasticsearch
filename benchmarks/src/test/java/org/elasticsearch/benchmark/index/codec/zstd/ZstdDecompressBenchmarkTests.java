/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.zstd;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

/**
 * Verifies that the ZstdDecompressBenchmark setup/teardown works correctly
 * and that decompression produces valid output for each directory type and mode.
 */
public class ZstdDecompressBenchmarkTests extends ESTestCase {

    private final String directoryType;
    private final String decompressMode;

    public ZstdDecompressBenchmarkTests(String directoryType, String decompressMode) {
        this.directoryType = directoryType;
        this.decompressMode = decompressMode;
    }

    public void testDecompress() throws Exception {
        var bench = new ZstdDecompressBenchmark();
        bench.directoryType = directoryType;
        bench.blockSize = 4096;
        bench.decompressMode = decompressMode;
        bench.setup();
        try {
            Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
            BytesRef bytes = new BytesRef();
            IndexInput in = bench.input;

            int off = decompressMode.equals("FULL") ? 0 : 4096 / 4;
            int len = decompressMode.equals("FULL") ? 4096 : 4096 / 2;

            for (int i = 0; i < bench.blockOffsets.length; i++) {
                in.seek(bench.blockOffsets[i]);
                decompressor.decompress(in, 4096, off, len, bytes);

                byte[] expected = Arrays.copyOfRange(bench.originalBlocks[i], off, off + len);
                byte[] actual = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
                assertArrayEquals("block " + i + " mismatch", expected, actual);
            }
        } finally {
            bench.tearDown();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<String> dirTypes = List.of("NIOFS", "MMAP", "SNAP");
        List<String> modes = List.of("FULL", "SLICE");
        return () -> dirTypes.stream().flatMap(d -> modes.stream().map(m -> new Object[] { d, m })).iterator();
    }
}
