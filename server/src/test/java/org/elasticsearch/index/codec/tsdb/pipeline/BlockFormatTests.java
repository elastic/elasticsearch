/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.TestPayloadCodecStage;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BlockFormatTests extends ESTestCase {

    private static int randomBlockSize() {
        return ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    public void testWriteReadBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.OFFSET.id, StageId.BIT_PACK.id };
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        encodingContext.setCurrentPosition(0);
        encodingContext.metadata().writeZLong(1000L);
        encodingContext.applyStage(2);

        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = i * 10L;
        }

        final DecodingContext decodingContext = writeAndRead(stageIds, values, encodingContext, blockSize, 2);

        assertTrue(decodingContext.isStageApplied(0));
        assertFalse(decodingContext.isStageApplied(1));
        assertTrue(decodingContext.isStageApplied(2));
    }

    public void testBitmapFor8OrFewerStages() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02, 0x033, 0x044, 0x05, 0x06, 0x07, 0x08 };
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        for (int i = 0; i < 8; i++) {
            encodingContext.applyStage(i);
        }

        final DecodingContext decodingContext = writeAndRead(stageIds, new long[blockSize], encodingContext, blockSize, 7);

        for (int i = 0; i < 8; i++) {
            assertTrue("Position " + i + " should be active", decodingContext.isStageApplied(i));
        }
    }

    public void testBitmapForMoreThan8Stages() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10 };
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        encodingContext.applyStage(0);
        encodingContext.applyStage(5);
        encodingContext.applyStage(9);

        final DecodingContext decodingContext = writeAndRead(stageIds, new long[blockSize], encodingContext, blockSize, 9);

        assertTrue(decodingContext.isStageApplied(0));
        assertFalse(decodingContext.isStageApplied(1));
        assertTrue(decodingContext.isStageApplied(5));
        assertTrue(decodingContext.isStageApplied(9));
    }

    public void testReadBlockThrowsWhenPipelineNotSet() {
        final int blockSize = randomBlockSize();
        final DecodingContext decodingContext = new DecodingContext(blockSize, new byte[0]);

        final byte[] buffer = new byte[256];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer);
        final long[] values = new long[blockSize];

        final AssertionError e = expectThrows(
            AssertionError.class,
            () -> BlockFormat.readBlock(in, values, TestPayloadCodecStage.INSTANCE, decodingContext, 0)
        );
        assertEquals("Pipeline must be set for decoding", e.getMessage());
    }

    public void testEmptyBitmap() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02 };
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        final DecodingContext decodingContext = writeAndRead(stageIds, new long[blockSize], encodingContext, blockSize, 1);

        assertFalse(decodingContext.isStageApplied(0));
        assertFalse(decodingContext.isStageApplied(1));
    }

    private EncodingContext createEncodingContext(final byte[] stageIds, int blockSize) {
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);
        final EncodingContext context = new EncodingContext(blockSize, pipeline.pipelineLength());
        context.setValueCount(blockSize);
        return context;
    }

    private DecodingContext writeAndRead(
        final byte[] stageIds,
        final long[] values,
        final EncodingContext encodingContext,
        int blockSize,
        int payloadPosition
    ) throws IOException {
        final byte[] buffer = new byte[blockSize * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        BlockFormat.writeBlock(out, values, TestPayloadCodecStage.INSTANCE, encodingContext);

        final DecodingContext decodingContext = new DecodingContext(blockSize, stageIds);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        BlockFormat.readBlock(in, values, TestPayloadCodecStage.INSTANCE, decodingContext, payloadPosition);

        return decodingContext;
    }
}
