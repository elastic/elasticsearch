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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.store.DirectoryType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

/**
 * Verifies that the ZstdDecompressBenchmark setup/teardown works correctly
 * and that decompression produces valid output for each directory type and mode.
 */
public class ZstdDecompressBenchmarkTests extends ESTestCase {

    private final DirectoryType directoryType;
    private final String decompressMode;

    public ZstdDecompressBenchmarkTests(DirectoryType directoryType, String decompressMode) {
        this.directoryType = directoryType;
        this.decompressMode = decompressMode;
    }

    public void testDecompress() throws Exception {
        var bench = new ZstdDecompressBenchmark();
        bench.directoryType = directoryType;
        bench.blockSize = 4096;
        bench.decompressMode = decompressMode;
        bench.hitsPerBlock = randomFrom(1, 2, 3, 32);
        bench.setup();
        try {
            final int numBlocks = bench.blockOffsets.length;
            final int hitsPerBlock = bench.hitsPerBlock;
            // go around the file more than once so that the benchmark's decompressor is reused across all blocks
            for (int doc = 0; doc < 2 * numBlocks * hitsPerBlock + randomIntBetween(0, hitsPerBlock); doc++) {
                int block = doc / hitsPerBlock % numBlocks;
                int len = decompressMode.equals("FULL") ? 4096 : 4096 / 2 / hitsPerBlock;
                int off = decompressMode.equals("FULL") ? 0 : 4096 / 4 + doc % hitsPerBlock * len;

                BytesRef bytes = bench.readNextDocument();

                byte[] expected = Arrays.copyOfRange(bench.originalBlocks[block], off, off + len);
                byte[] actual = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
                assertArrayEquals("doc " + doc + " (block " + block + ") mismatch", expected, actual);
            }
        } finally {
            bench.tearDown();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<DirectoryType> dirTypes = List.of(DirectoryType.NIO, DirectoryType.MMAP, DirectoryType.SNAP);
        List<String> modes = List.of("FULL", "SLICE");
        return () -> dirTypes.stream().flatMap(d -> modes.stream().map(m -> new Object[] { d, m })).iterator();
    }
}
