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

    private static final int BLOCK_SIZE = 128;

    public void testConstantValues() throws IOException {
        final long[] values = new long[BLOCK_SIZE];
        Arrays.fill(values, randomLong());
        assertRoundTrip(values);
    }

    public void testMonotonicValues() throws IOException {
        final long[] values = new long[BLOCK_SIZE];
        long base = randomLong() >>> 1;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = base + (long) i * randomIntBetween(1, 100);
        }
        assertRoundTrip(values);
    }

    public void testRandomValues() throws IOException {
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = randomLong();
        }
        assertRoundTrip(values);
    }

    public void testZeros() throws IOException {
        assertRoundTrip(new long[BLOCK_SIZE]);
    }

    public void testGcdValues() throws IOException {
        long gcd = randomIntBetween(2, 1000);
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = gcd * randomIntBetween(0, 10000);
        }
        Arrays.sort(values);
        assertRoundTrip(values);
    }

    public void testPartialBlock() throws IOException {
        int count = randomIntBetween(1, BLOCK_SIZE - 1);
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < count; i++) {
            values[i] = randomLong();
        }
        assertRoundTrip(values, count);
    }

    public void testMultipleBlocks() throws IOException {
        int numBlocks = randomIntBetween(2, 10);
        final PipelineConfig config = PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        final IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test");

        final long[][] allValues = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            allValues[b] = new long[BLOCK_SIZE];
            for (int i = 0; i < BLOCK_SIZE; i++) {
                allValues[b][i] = randomLong();
            }
            final long[] copy = Arrays.copyOf(allValues[b], BLOCK_SIZE);
            blockEncoder.encode(copy, BLOCK_SIZE, out);
        }
        out.close();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final ByteBuffersDataInput in = bufferOut.toDataInput();

        for (int b = 0; b < numBlocks; b++) {
            final long[] decoded = new long[BLOCK_SIZE];
            blockDecoder.decode(decoded, BLOCK_SIZE, in);
            assertArrayEquals("block " + b, allValues[b], decoded);
        }
    }

    public void testDescriptorRoundTrip() {
        final PipelineConfig config = PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final PipelineDescriptor descriptor = encoder.descriptor();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(descriptor);
        assertNotNull(decoder);
        assertEquals(BLOCK_SIZE, decoder.blockSize());
    }

    private void assertRoundTrip(long[] values) throws IOException {
        assertRoundTrip(values, values.length);
    }

    private void assertRoundTrip(long[] values, int count) throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        final IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test");
        final long[] original = Arrays.copyOf(values, values.length);
        blockEncoder.encode(values, count, out);
        out.close();

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final long[] decoded = new long[BLOCK_SIZE];
        blockDecoder.decode(decoded, count, bufferOut.toDataInput());

        for (int i = 0; i < count; i++) {
            assertEquals("index " + i, original[i], decoded[i]);
        }
    }
}
