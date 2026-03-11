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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BlockFormatTests extends ESTestCase {

    private static final int BASE_BLOCK_SIZE = 128;

    private static int randomBlockSize() {
        return BASE_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    public void testWriteReadBlockRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA_STAGE.id, StageId.OFFSET_STAGE.id, StageId.BITPACK_PAYLOAD.id };
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        encodingContext.setCurrentPosition(0);
        encodingContext.metadata().writeZLong(1000L);
        encodingContext.applyStage(2);

        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = randomLong();
        }

        final long[] decoded = new long[blockSize];
        final DecodingContext decodingContext = writeAndRead(stageIds, values, decoded, encodingContext, blockSize, 2);

        assertEquals(blockSize, decodingContext.blockSize());
        assertTrue(decodingContext.isStageApplied(0));
        assertFalse(decodingContext.isStageApplied(1));
        assertTrue(decodingContext.isStageApplied(2));
        assertArrayEquals(values, decoded);
    }

    public void testBitmapRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final int numStages = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final byte[] stageIds = new byte[numStages];
        for (int i = 0; i < numStages; i++) {
            stageIds[i] = (byte) (i + 1);
        }
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        final boolean[] applied = new boolean[numStages];
        applied[numStages - 1] = true;
        encodingContext.applyStage(numStages - 1);
        for (int i = 0; i < numStages - 1; i++) {
            applied[i] = randomBoolean();
            if (applied[i]) {
                encodingContext.applyStage(i);
            }
        }

        final DecodingContext decodingContext = writeAndRead(
            stageIds,
            new long[blockSize],
            new long[blockSize],
            encodingContext,
            blockSize,
            numStages - 1
        );

        for (int i = 0; i < numStages; i++) {
            assertEquals("Position " + i, applied[i], decodingContext.isStageApplied(i));
        }
    }

    public void testReadBlockThrowsWhenPipelineNotSet() {
        final int blockSize = randomBlockSize();
        final DecodingContext decodingContext = new DecodingContext(blockSize, 0);

        final byte[] buffer = new byte[256];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer);
        final long[] values = new long[blockSize];

        final IOException e = expectThrows(
            IOException.class,
            () -> BlockFormat.readBlock(in, values, TestPayloadCodecStage.INSTANCE, decodingContext, 0)
        );
        assertEquals("Pipeline must be set for decoding", e.getMessage());
    }

    public void testEmptyBitmapThrowsOnMissingPayload() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02 };
        final EncodingContext encodingContext = createEncodingContext(stageIds, blockSize);

        final byte[] buffer = new byte[blockSize * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        BlockFormat.writeBlock(out, new long[blockSize], TestPayloadCodecStage.INSTANCE, encodingContext);

        final DecodingContext decodingContext = new DecodingContext(blockSize, stageIds.length);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());

        final IOException e = expectThrows(
            IOException.class,
            () -> BlockFormat.readBlock(in, new long[blockSize], TestPayloadCodecStage.INSTANCE, decodingContext, 1)
        );
        assertEquals("Payload stage not applied - possible data corruption", e.getMessage());
    }

    private EncodingContext createEncodingContext(final byte[] stageIds, final int blockSize) {
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);
        final EncodingContext context = new EncodingContext(blockSize, pipeline.pipelineLength());
        context.setValueCount(blockSize);
        return context;
    }

    private DecodingContext writeAndRead(
        final byte[] stageIds,
        final long[] values,
        final long[] decoded,
        final EncodingContext encodingContext,
        final int blockSize,
        final int payloadPosition
    ) throws IOException {
        final byte[] buffer = new byte[blockSize * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        BlockFormat.writeBlock(out, values, TestPayloadCodecStage.INSTANCE, encodingContext);

        final DecodingContext decodingContext = new DecodingContext(blockSize, stageIds.length);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final int decodedCount = BlockFormat.readBlock(in, decoded, TestPayloadCodecStage.INSTANCE, decodingContext, payloadPosition);

        assertEquals(blockSize, decodedCount);
        return decodingContext;
    }
}
