/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;

public class BlockMetadataAccumulatorTests extends LuceneTestCase {

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private static final String META_CODEC = "TestCodec";
    private static final int VERSION_CURRENT = 0;

    public void testSingleBlock() throws IOException {
        try (Directory dir = newDirectory()) {
            final long[] docsPerBlock = { 128 };
            final long[] bytesPerBlock = { 512 };
            assertRoundTrip(dir, docsPerBlock, bytesPerBlock);
        }
    }

    public void testMultipleBlocks() throws IOException {
        try (Directory dir = newDirectory()) {
            final long[] docsPerBlock = { 128, 64, 256 };
            final long[] bytesPerBlock = { 512, 1024, 768 };
            assertRoundTrip(dir, docsPerBlock, bytesPerBlock);
        }
    }

    public void testUniformBlocks() throws IOException {
        try (Directory dir = newDirectory()) {
            final long[] docsPerBlock = new long[10];
            final long[] bytesPerBlock = new long[10];
            for (int i = 0; i < 10; i++) {
                docsPerBlock[i] = 128;
                bytesPerBlock[i] = 1024;
            }
            assertRoundTrip(dir, docsPerBlock, bytesPerBlock);
        }
    }

    public void testRandom() throws IOException {
        try (Directory dir = newDirectory()) {
            for (int iter = 0; iter < 50; iter++) {
                int numBlocks = TestUtil.nextInt(random(), 1, 200);
                final long[] docsPerBlock = new long[numBlocks];
                final long[] bytesPerBlock = new long[numBlocks];
                for (int i = 0; i < numBlocks; i++) {
                    docsPerBlock[i] = TestUtil.nextInt(random(), 1, 1000);
                    bytesPerBlock[i] = TestUtil.nextInt(random(), 1, 10000);
                }
                assertRoundTrip(dir, docsPerBlock, bytesPerBlock);
            }
        }
    }

    private static void assertRoundTrip(final Directory dir, final long[] docsPerBlock, final long[] bytesPerBlock) throws IOException {
        int numBlocks = docsPerBlock.length;
        long addressesStart;

        try (
            IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT);
            IndexOutput data = dir.createOutput("data", IOContext.DEFAULT)
        ) {
            addressesStart = data.getFilePointer();
            try (
                BlockMetadataAccumulator acc = new BlockMetadataAccumulator(
                    dir,
                    IOContext.DEFAULT,
                    data,
                    addressesStart,
                    META_CODEC,
                    VERSION_CURRENT,
                    DIRECT_MONOTONIC_BLOCK_SHIFT
                )
            ) {
                for (int i = 0; i < numBlocks; i++) {
                    acc.addDoc(docsPerBlock[i], bytesPerBlock[i]);
                }
                acc.build(meta, data);
            }
        }

        try (IndexInput meta = dir.openInput("meta", IOContext.DEFAULT); IndexInput data = dir.openInput("data", IOContext.DEFAULT)) {
            final DirectMonotonicReader.Meta addressesMeta = DirectMonotonicReader.loadMeta(
                meta,
                numBlocks + 1,
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long addressesLength = meta.readLong();
            long dataDocRangeStart = meta.readLong();
            final DirectMonotonicReader.Meta docRangesMeta = DirectMonotonicReader.loadMeta(
                meta,
                numBlocks + 1,
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long docRangesLength = meta.readLong();

            assertCumulativeOffsets(
                "address",
                DirectMonotonicReader.getInstance(addressesMeta, data.randomAccessSlice(0, addressesLength)),
                bytesPerBlock,
                addressesStart
            );
            assertCumulativeOffsets(
                "doc range",
                DirectMonotonicReader.getInstance(docRangesMeta, data.randomAccessSlice(dataDocRangeStart, docRangesLength)),
                docsPerBlock,
                0
            );
        } finally {
            dir.deleteFile("meta");
            dir.deleteFile("data");
        }
    }

    private static void assertCumulativeOffsets(
        final String label,
        final DirectMonotonicReader reader,
        final long[] deltas,
        long startOffset
    ) {
        long expected = startOffset;
        for (int i = 0; i <= deltas.length; i++) {
            assertEquals(label + " mismatch at block " + i, expected, reader.get(i));
            if (i < deltas.length) {
                expected += deltas[i];
            }
        }
    }
}
