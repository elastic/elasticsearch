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
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.TestPayloadCodecStage;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractTransformStageTestCase extends ESTestCase {

    private static final int BASE_BLOCK_SIZE = 128;

    protected static int randomBlockSize() {
        return BASE_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    protected static EncodingContext createEncodingContext(int blockSize) {
        final EncodingContext context = new EncodingContext(blockSize, 2);
        context.setValueCount(blockSize);
        context.setCurrentPosition(0);
        return context;
    }

    protected static void assertStageSkipped(final NumericCodecStage stage, final long[] values, final int valueCount) {
        final long[] original = values.clone();
        final EncodingContext context = createEncodingContext(valueCount);
        stage.encode(values, valueCount, context);
        assertFalse(context.isStageApplied(0));
        assertArrayEquals(original, values);
    }

    protected static void assertTransformRoundTrip(final NumericCodecStage stage, final long[] original) throws IOException {
        final int blockSize = original.length;
        final int pipelineLength = 2;

        final long[] values = original.clone();
        final EncodingContext encodingContext = new EncodingContext(blockSize, pipelineLength);
        encodingContext.setValueCount(blockSize);
        encodingContext.setCurrentPosition(0);
        stage.encode(values, blockSize, encodingContext);

        if (encodingContext.isStageApplied(0) == false) {
            assertArrayEquals(original, values);
            return;
        }

        encodingContext.applyStage(1);
        final byte[] buffer = new byte[blockSize * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        BlockFormat.writeBlock(out, values, TestPayloadCodecStage.INSTANCE, encodingContext);

        final long[] decoded = new long[blockSize];
        final DecodingContext decodingContext = new DecodingContext(blockSize, pipelineLength);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        BlockFormat.readBlock(in, decoded, TestPayloadCodecStage.INSTANCE, decodingContext, 1);

        decodingContext.setDataInput(in);
        stage.decode(decoded, blockSize, decodingContext);

        assertArrayEquals(original, decoded);
    }

    protected static void assertMultiBlockTransformRoundTrip(final NumericCodecStage stage, final long[][] blocks) throws IOException {
        final int blockSize = blocks[0].length;
        final int pipelineLength = 2;

        final byte[] buffer = new byte[blocks.length * blockSize * Long.BYTES + 256 * blocks.length];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        final boolean[] applied = new boolean[blocks.length];
        for (int b = 0; b < blocks.length; b++) {
            assertEquals(blockSize, blocks[b].length);
            final long[] values = blocks[b].clone();
            final EncodingContext encodingContext = new EncodingContext(blockSize, pipelineLength);
            encodingContext.setValueCount(blockSize);
            encodingContext.setCurrentPosition(0);
            stage.encode(values, blockSize, encodingContext);
            applied[b] = encodingContext.isStageApplied(0);
            encodingContext.applyStage(1);
            BlockFormat.writeBlock(out, values, TestPayloadCodecStage.INSTANCE, encodingContext);
        }

        final long[] reused = new long[blockSize];
        Arrays.fill(reused, Long.MAX_VALUE);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());

        for (int b = 0; b < blocks.length; b++) {
            final DecodingContext decodingContext = new DecodingContext(blockSize, pipelineLength);
            BlockFormat.readBlock(in, reused, TestPayloadCodecStage.INSTANCE, decodingContext, 1);
            if (applied[b]) {
                decodingContext.setDataInput(in);
                stage.decode(reused, blockSize, decodingContext);
            }
            assertArrayEquals("Block " + b + " mismatch", blocks[b], reused);
        }
    }

    protected static long[] randomMonotonicIncreasing(final int size) {
        final long[] values = new long[size];
        values[0] = randomLongBetween(-10000, 10000);
        for (int i = 1; i < size; i++) {
            values[i] = values[i - 1] + randomLongBetween(1, 1000);
        }
        return values;
    }

    protected static long[] randomMonotonicDecreasing(final int size) {
        final long[] values = new long[size];
        values[0] = randomLongBetween(-10000, 10000);
        for (int i = 1; i < size; i++) {
            values[i] = values[i - 1] - randomLongBetween(1, 1000);
        }
        return values;
    }
}
