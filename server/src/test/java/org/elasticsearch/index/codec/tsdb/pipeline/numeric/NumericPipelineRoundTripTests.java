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
}
