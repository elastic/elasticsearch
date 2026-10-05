/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ES95NumericFieldEncoderTests extends ESTestCase {

    public void testDescriptorMatchesUnderlyingEncoder() {
        final NumericEncoder numericEncoder = baselineEncoder(randomBlockSize());
        final ES95NumericFieldEncoder fieldEncoder = new ES95NumericFieldEncoder(numericEncoder);

        assertEquals(numericEncoder.descriptor(), fieldEncoder.descriptor());
    }

    public void testRepeatedDescriptorCallsReturnSameInstance() {
        final ES95NumericFieldEncoder fieldEncoder = new ES95NumericFieldEncoder(baselineEncoder(randomBlockSize()));

        final PipelineDescriptor first = fieldEncoder.descriptor();
        final PipelineDescriptor second = fieldEncoder.descriptor();
        assertSame(first, second);
    }

    public void testEncodeBlockRoundTripsThroughMatchingDecoder() throws IOException {
        final int blockSize = randomBlockSize();
        final NumericEncoder numericEncoder = baselineEncoder(blockSize);
        final ES95NumericFieldEncoder fieldEncoder = new ES95NumericFieldEncoder(numericEncoder);

        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }
        final long[] original = values.clone();

        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            fieldEncoder.encodeBlock(values, blockSize, out);
        }

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(fieldEncoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final long[] decoded = new long[blockSize];
        blockDecoder.decode(decoded, blockSize, bufferOut.toDataInput());

        assertArrayEquals(original, decoded);
    }

    public void testEncodesMultipleBlocksContiguously() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final ES95NumericFieldEncoder fieldEncoder = new ES95NumericFieldEncoder(baselineEncoder(blockSize));

        final long[][] expected = new long[numBlocks][];
        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            for (int b = 0; b < numBlocks; b++) {
                expected[b] = new long[blockSize];
                for (int i = 0; i < blockSize; i++) {
                    expected[b][i] = randomLong();
                }
                fieldEncoder.encodeBlock(expected[b].clone(), blockSize, out);
            }
        }

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(fieldEncoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final ByteBuffersDataInput in = bufferOut.toDataInput();
        for (int b = 0; b < numBlocks; b++) {
            final long[] decoded = new long[blockSize];
            blockDecoder.decode(decoded, blockSize, in);
            assertArrayEquals("block " + b, expected[b], decoded);
        }
    }

    public void testIndependentInstancesHaveIndependentBlockEncoders() throws IOException {
        final int blockSize = randomBlockSize();
        final NumericEncoder shared = baselineEncoder(blockSize);

        final ES95NumericFieldEncoder first = new ES95NumericFieldEncoder(shared);
        final ES95NumericFieldEncoder second = new ES95NumericFieldEncoder(shared);

        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }

        final ByteBuffersDataOutput firstOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(firstOut, "first", "first")) {
            first.encodeBlock(values.clone(), blockSize, out);
        }

        final ByteBuffersDataOutput secondOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(secondOut, "second", "second")) {
            second.encodeBlock(values.clone(), blockSize, out);
        }

        assertEquals(firstOut.size(), secondOut.size());
    }

    private static NumericEncoder baselineEncoder(final int blockSize) {
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        return NumericCodecFactory.DEFAULT.createEncoder(config);
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 9);
    }
}
