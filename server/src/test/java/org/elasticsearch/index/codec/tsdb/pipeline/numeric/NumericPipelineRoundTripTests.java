/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class NumericPipelineRoundTripTests extends ESTestCase {

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 9);
    }

    public void testConstantValues() throws IOException {
        int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, randomLong());
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testMonotonicValues() throws IOException {
        int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        long base = randomLong() >>> 1;
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + (long) i * randomIntBetween(1, 100);
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testNegativeValues() throws IOException {
        int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = -randomLongBetween(1, Long.MAX_VALUE);
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testMixedPositiveAndNegativeValues() throws IOException {
        int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testZeros() throws IOException {
        int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize, blockSize);
    }

    public void testGcdValues() throws IOException {
        int blockSize = randomBlockSize();
        long gcd = randomIntBetween(2, 1000);
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = gcd * randomIntBetween(0, 10000);
        }
        Arrays.sort(values);
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testPartialBlock() throws IOException {
        int blockSize = randomBlockSize();
        int count = randomIntBetween(1, blockSize - 1);
        final long[] values = new long[blockSize];
        for (int i = 0; i < count; i++) {
            values[i] = randomLong();
        }
        assertRoundTrip(values, blockSize, count);
    }

    public void testMultipleBlocks() throws IOException {
        int blockSize = randomBlockSize();
        int numBlocks = randomIntBetween(2, 10);
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        final IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test");

        final long[][] allValues = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            allValues[b] = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                allValues[b][i] = randomLong();
            }
            final long[] copy = Arrays.copyOf(allValues[b], blockSize);
            blockEncoder.encode(copy, blockSize, out);
        }
        out.close();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final ByteBuffersDataInput in = bufferOut.toDataInput();

        for (int b = 0; b < numBlocks; b++) {
            final long[] decoded = new long[blockSize];
            blockDecoder.decode(decoded, blockSize, in);
            assertArrayEquals("block " + b, allValues[b], decoded);
        }
    }

    public void testDescriptorRoundTrip() {
        int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final PipelineDescriptor descriptor = encoder.descriptor();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(descriptor);
        assertNotNull(decoder);
        assertEquals(blockSize, decoder.blockSize());
    }

    public void testConstantIntervalMonotonicProducesMinimalOutput() throws IOException {
        final int blockSize = 128;
        final long base = 1000;
        final long interval = 10;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + (long) i * interval;
        }
        final long encodedSize = assertRoundTripAndReturnSize(values, blockSize, blockSize);
        assertEquals(5, encodedSize);
    }

    public void testAllSameValueProducesMinimalOutput() throws IOException {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        Arrays.fill(values, 42L);
        final long encodedSize = assertRoundTripAndReturnSize(values, blockSize, blockSize);
        assertEquals(3, encodedSize);
    }

    public void testGcdMultiplesProducesCompactOutput() throws IOException {
        final int blockSize = 256;
        final long gcd = 7;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = gcd * i;
        }
        final long encodedSize = assertRoundTripAndReturnSize(values, blockSize, blockSize);
        assertEquals(4, encodedSize);
    }

    public void testSplitDeltaBoundaryBlockRoundTrip() throws IOException {
        final int blockSize = 128;
        final int numBlocks = 4;
        final long[][] allValues = new long[numBlocks][];
        final long baseTimestamp = 1_700_000_000_000L;
        final long interval = 10_000L;
        final long boundaryJump = 240L * 60L * 1000L;
        for (int b = 0; b < numBlocks; b++) {
            allValues[b] = new long[blockSize];
            final int boundary = blockSize / 2 + b;
            long current = baseTimestamp + (long) b * boundaryJump;
            for (int i = 0; i < boundary; i++) {
                allValues[b][i] = current - (long) i * interval;
            }
            long secondStart = current + boundaryJump;
            for (int i = boundary; i < blockSize; i++) {
                allValues[b][i] = secondStart - (long) (i - boundary) * interval;
            }
        }

        final PipelineConfig config = PipelineConfig.forLongs(blockSize).splitDelta().delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        final IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test");
        for (int b = 0; b < numBlocks; b++) {
            blockEncoder.encode(Arrays.copyOf(allValues[b], blockSize), blockSize, out);
        }
        out.close();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final ByteBuffersDataInput in = bufferOut.toDataInput();
        for (int b = 0; b < numBlocks; b++) {
            final long[] decoded = new long[blockSize];
            blockDecoder.decode(decoded, blockSize, in);
            assertArrayEquals("block " + b, allValues[b], decoded);
        }
    }

    public void testSplitDeltaRoundTripAcrossBlockSizes() throws IOException {
        // splitDelta kMax scales with block size (up to 64), so a large block can hold more than
        // DEFAULT_K_MAX=16 splits; the decoder, rebuilt from the stage id, must fit them.
        for (int blockSize : new int[] { 128, 512, 1024, 2048 }) {
            final int kMax = Math.clamp((long) blockSize / 32, 4, 64);
            final long[] values = piecewiseDescendingRuns(blockSize, 64);
            final PipelineConfig config = PipelineConfig.forLongs(blockSize).splitDelta(kMax).delta().offset().gcd().bitPack();
            final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
            final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

            final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
            try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
                blockEncoder.encode(Arrays.copyOf(values, blockSize), blockSize, out);
            }

            final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
            final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
            final long[] decoded = new long[blockSize];
            blockDecoder.decode(decoded, blockSize, bufferOut.toDataInput());

            assertArrayEquals("bs=" + blockSize, values, decoded);
        }
    }

    private void assertRoundTrip(long[] values, int blockSize, int count) throws IOException {
        assertRoundTripAndReturnSize(values, blockSize, count);
    }

    private long assertRoundTripAndReturnSize(long[] values, int blockSize, int count) throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        final IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test");
        final long[] original = Arrays.copyOf(values, values.length);
        blockEncoder.encode(values, count, out);
        out.close();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final long[] decoded = new long[blockSize];
        blockDecoder.decode(decoded, count, bufferOut.toDataInput());

        for (int i = 0; i < count; i++) {
            assertEquals("index " + i, original[i], decoded[i]);
        }
        return bufferOut.size();
    }

    // Descends within each run, jumps up at each boundary; splitDelta emits one split per boundary.
    private static long[] piecewiseDescendingRuns(int size, int runLength) {
        final long[] values = new long[size];
        long base = 1_000_000L;
        long current = base;
        for (int i = 0; i < size; i++) {
            if (i > 0 && i % runLength == 0) {
                base += 1_000_000L;
                current = base;
            } else if (i > 0) {
                current -= 10L;
            }
            values[i] = current;
        }
        return values;
    }
}
