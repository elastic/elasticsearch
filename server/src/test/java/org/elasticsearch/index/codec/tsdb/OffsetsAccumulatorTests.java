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

public class OffsetsAccumulatorTests extends LuceneTestCase {

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    public void testSingleDoc() throws IOException {
        try (Directory dir = newDirectory()) {
            final int[] valueCounts = { 3 };
            final long[] expectedOffsets = buildExpectedOffsets(valueCounts);
            final long[] actualOffsets = writeAndReadOffsets(dir, 1, valueCounts);
            assertOffsetsEqual(expectedOffsets, actualOffsets);
        }
    }

    public void testMultipleDocs() throws IOException {
        try (Directory dir = newDirectory()) {
            final int[] valueCounts = { 1, 4, 2, 7, 3 };
            final long[] expectedOffsets = buildExpectedOffsets(valueCounts);
            final long[] actualOffsets = writeAndReadOffsets(dir, 5, valueCounts);
            assertOffsetsEqual(expectedOffsets, actualOffsets);
        }
    }

    public void testZeroValueCounts() throws IOException {
        try (Directory dir = newDirectory()) {
            final int[] valueCounts = { 0, 0, 0 };
            final long[] expectedOffsets = buildExpectedOffsets(valueCounts);
            final long[] actualOffsets = writeAndReadOffsets(dir, 3, valueCounts);
            assertOffsetsEqual(expectedOffsets, actualOffsets);
        }
    }

    public void testMixedValueCounts() throws IOException {
        try (Directory dir = newDirectory()) {
            final int[] valueCounts = { 0, 5, 0, 10 };
            final long[] expectedOffsets = buildExpectedOffsets(valueCounts);
            final long[] actualOffsets = writeAndReadOffsets(dir, 4, valueCounts);
            assertOffsetsEqual(expectedOffsets, actualOffsets);
        }
    }

    public void testRandom() throws IOException {
        try (Directory dir = newDirectory()) {
            for (int iter = 0; iter < 50; iter++) {
                int numDocsWithField = TestUtil.nextInt(random(), 1, 500);
                final int[] valueCounts = new int[numDocsWithField];
                for (int i = 0; i < numDocsWithField; i++) {
                    valueCounts[i] = TestUtil.nextInt(random(), 0, 100);
                }
                final long[] expectedOffsets = buildExpectedOffsets(valueCounts);
                final long[] actualOffsets = writeAndReadOffsets(dir, numDocsWithField, valueCounts);
                assertOffsetsEqual(expectedOffsets, actualOffsets);
            }
        }
    }

    public void testLargeValueCounts() throws IOException {
        try (Directory dir = newDirectory()) {
            final int[] valueCounts = { 10000, 20000, 30000 };
            final long[] expectedOffsets = buildExpectedOffsets(valueCounts);
            final long[] actualOffsets = writeAndReadOffsets(dir, 3, valueCounts);
            assertOffsetsEqual(expectedOffsets, actualOffsets);
        }
    }

    private static long[] buildExpectedOffsets(final int[] valueCounts) {
        final long[] offsets = new long[valueCounts.length + 1];
        offsets[0] = 0;
        for (int i = 0; i < valueCounts.length; i++) {
            offsets[i + 1] = offsets[i] + valueCounts[i];
        }
        return offsets;
    }

    private static long[] writeAndReadOffsets(final Directory dir, int numDocsWithField, final int[] valueCounts) throws IOException {
        try (
            IndexOutput meta = dir.createOutput("meta", IOContext.DEFAULT);
            IndexOutput data = dir.createOutput("data", IOContext.DEFAULT)
        ) {
            try (
                OffsetsAccumulator acc = new OffsetsAccumulator(
                    dir,
                    IOContext.DEFAULT,
                    data,
                    numDocsWithField,
                    DIRECT_MONOTONIC_BLOCK_SHIFT
                )
            ) {
                for (int count : valueCounts) {
                    acc.addDoc(count);
                }
                acc.build(meta, data);
            }
        }

        try (IndexInput meta = dir.openInput("meta", IOContext.DEFAULT); IndexInput data = dir.openInput("data", IOContext.DEFAULT)) {
            long start = meta.readLong();
            int blockShift = meta.readVInt();
            final DirectMonotonicReader.Meta addressesMeta = DirectMonotonicReader.loadMeta(meta, numDocsWithField + 1, blockShift);
            long length = meta.readLong();

            final DirectMonotonicReader reader = DirectMonotonicReader.getInstance(addressesMeta, data.randomAccessSlice(start, length));
            final long[] result = new long[numDocsWithField + 1];
            for (int i = 0; i <= numDocsWithField; i++) {
                result[i] = reader.get(i);
            }
            return result;
        } finally {
            dir.deleteFile("meta");
            dir.deleteFile("data");
        }
    }

    private static void assertOffsetsEqual(final long[] expected, final long[] actual) {
        assertEquals("offset array length mismatch", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("offset mismatch at index " + i, expected[i], actual[i]);
        }
    }
}
