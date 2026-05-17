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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class NumericPipelineDeltaOfDeltaRoundTripTests extends ESTestCase {

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 9);
    }

    public void testConstantIntervalMonotonicRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long base = randomLongBetween(0, 1_000_000);
        final long interval = randomLongBetween(1, 10_000);
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + (long) i * interval;
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testConstantRateOfChangeRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long base = randomLongBetween(0, 1_000_000);
        final long firstInterval = randomLongBetween(1, 10_000);
        final long rateOfChange = randomLongBetween(1, 100);
        values[0] = base;
        long interval = firstInterval;
        for (int i = 1; i < blockSize; i++) {
            values[i] = values[i - 1] + interval;
            interval += rateOfChange;
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testJitteryIntervalsRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long base = randomLongBetween(0, 1_000_000);
        final long meanInterval = randomLongBetween(100, 10_000);
        long current = base;
        for (int i = 0; i < blockSize; i++) {
            values[i] = current;
            current += meanInterval + randomLongBetween(-5, 5);
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testNonMonotonicRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }
        assertRoundTrip(values, blockSize, blockSize);
    }

    public void testZerosRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        assertRoundTrip(new long[blockSize], blockSize, blockSize);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int count = randomIntBetween(1, blockSize - 1);
        final long[] values = new long[blockSize];
        final long base = randomLongBetween(0, 1_000_000);
        for (int i = 0; i < count; i++) {
            values[i] = base + (long) i * 10;
        }
        assertRoundTrip(values, blockSize, count);
    }

    public void testMultipleBlocksRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 10);
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).deltaOfDelta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        final IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test");

        final long[][] allValues = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            allValues[b] = new long[blockSize];
            final long base = randomLongBetween(0, 1_000_000);
            final long interval = randomLongBetween(1, 1000);
            for (int i = 0; i < blockSize; i++) {
                allValues[b][i] = base + (long) i * interval;
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

    public void testConstantIntervalProducesMinimalOutput() throws IOException {
        final int blockSize = 128;
        final long base = 1000;
        final long interval = 10;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = base + (long) i * interval;
        }
        final long encodedSize = assertRoundTripAndReturnSize(values, blockSize, blockSize);
        assertTrue("constant interval should compress tightly, was " + encodedSize + " bytes", encodedSize <= 8);
    }

    public void testConstantRateOfChangeProducesMinimalOutput() throws IOException {
        final int blockSize = 128;
        final long base = 1000;
        final long firstInterval = 100;
        final long rateOfChange = 10;
        final long[] values = new long[blockSize];
        values[0] = base;
        long interval = firstInterval;
        for (int i = 1; i < blockSize; i++) {
            values[i] = values[i - 1] + interval;
            interval += rateOfChange;
        }
        final long encodedSize = assertRoundTripAndReturnSize(values, blockSize, blockSize);
        assertTrue("constant rate of change should compress tightly, was " + encodedSize + " bytes", encodedSize <= 8);
    }

    private void assertRoundTrip(long[] values, int blockSize, int count) throws IOException {
        assertRoundTripAndReturnSize(values, blockSize, count);
    }

    private long assertRoundTripAndReturnSize(long[] values, int blockSize, int count) throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).deltaOfDelta().offset().gcd().bitPack();
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
