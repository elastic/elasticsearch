/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractPayloadStageTestCase extends ESTestCase {

    private static final int BASE_BLOCK_SIZE = 128;

    protected static int randomBlockSize() {
        return BASE_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    protected static long randomValueWithExactBits(int bits) {
        if (bits == 0) {
            return 0;
        }
        final long highBit = 1L << (bits - 1);
        return highBit | (randomLong() & (highBit - 1));
    }

    protected abstract PayloadCodecStage createStage(int blockSize);

    protected void assertPayloadRoundTrip(final long[] original) throws IOException {
        final int blockSize = original.length;
        final int pipelineLength = 1;

        final long[] values = original.clone();
        final EncodingContext encodingContext = new EncodingContext(blockSize, pipelineLength);
        encodingContext.setValueCount(blockSize);
        encodingContext.setCurrentPosition(0);
        encodingContext.applyStage(0);

        final byte[] buffer = new byte[blockSize * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final PayloadCodecStage stage = createStage(blockSize);
        stage.encode(values, blockSize, out, encodingContext);

        final long[] decoded = new long[blockSize];
        final DecodingContext decodingContext = new DecodingContext(blockSize, pipelineLength);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final int count = stage.decode(decoded, in, decodingContext);

        assertEquals(blockSize, count);
        assertArrayEquals(original, decoded);
    }

    protected void assertMultiBlockPayloadRoundTrip(final long[][] blocks) throws IOException {
        final int blockSize = blocks[0].length;
        final int pipelineLength = 1;

        final byte[] buffer = new byte[blocks.length * blockSize * Long.BYTES + 256 * blocks.length];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        final PayloadCodecStage stage = createStage(blockSize);
        for (final long[] block : blocks) {
            assertEquals(blockSize, block.length);
            final long[] values = block.clone();
            final EncodingContext encodingContext = new EncodingContext(blockSize, pipelineLength);
            encodingContext.setValueCount(blockSize);
            encodingContext.setCurrentPosition(0);
            encodingContext.applyStage(0);
            stage.encode(values, blockSize, out, encodingContext);
        }

        final long[] reused = new long[blockSize];
        Arrays.fill(reused, Long.MAX_VALUE);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());

        for (int b = 0; b < blocks.length; b++) {
            final DecodingContext decodingContext = new DecodingContext(blockSize, pipelineLength);
            final int count = stage.decode(reused, in, decodingContext);
            assertEquals(blockSize, count);
            assertArrayEquals("Block " + b + " mismatch", blocks[b], reused);
        }
    }

    protected void assertPayloadRoundTrip(final long[] original, final int valueCount, final int blockSize) throws IOException {
        final int pipelineLength = 1;

        final long[] values = new long[blockSize];
        System.arraycopy(original, 0, values, 0, valueCount);

        final EncodingContext encodingContext = new EncodingContext(blockSize, pipelineLength);
        encodingContext.setValueCount(valueCount);
        encodingContext.setCurrentPosition(0);
        encodingContext.applyStage(0);

        final byte[] buffer = new byte[blockSize * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final PayloadCodecStage stage = createStage(blockSize);
        stage.encode(values, valueCount, out, encodingContext);

        final long[] decoded = new long[blockSize];
        final DecodingContext decodingContext = new DecodingContext(blockSize, pipelineLength);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final int count = stage.decode(decoded, in, decodingContext);

        assertEquals(blockSize, count);
        // NOTE: only compare valueCount elements, padding beyond that is undefined
        for (int i = 0; i < valueCount; i++) {
            assertEquals("Mismatch at index " + i, original[i], decoded[i]);
        }
    }
}
