/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class EncodingContextTests extends ESTestCase {

    private static final int BASE_BLOCK_SIZE = 128;

    private static int randomBlockSize() {
        return BASE_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    private static int randomPositionExcluding(final int position, final int pipelineLength) {
        return (position + 1 + randomIntBetween(0, pipelineLength - 2)) % pipelineLength;
    }

    public void testNewContextStartsWithNoActivePositions() {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(randomBlockSize(), pipelineLength);
        assertEquals(0, context.positionBitmap());
    }

    public void testActivatePositionSetsBitmapBit() {
        final int blockSize = randomBlockSize();
        final int pipelineLength = randomIntBetween(2, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(blockSize, pipelineLength);

        final int pos1 = randomIntBetween(0, pipelineLength - 1);
        context.applyStage(pos1);
        assertTrue(context.isStageApplied(pos1));

        final int pos2 = randomPositionExcluding(pos1, pipelineLength);
        assertFalse(context.isStageApplied(pos2));

        context.applyStage(pos2);
        assertTrue(context.isStageApplied(pos1));
        assertTrue(context.isStageApplied(pos2));
    }

    public void testMetadataActivatesPosition() {
        final int pipelineLength = randomIntBetween(2, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(randomBlockSize(), pipelineLength);

        final int pos = randomIntBetween(0, pipelineLength - 1);
        context.setCurrentPosition(pos);
        context.metadata().writeVLong(randomLongBetween(0, Long.MAX_VALUE));
        assertTrue(context.isStageApplied(pos));

        for (int i = 0; i < pipelineLength; i++) {
            if (i != pos) {
                assertFalse(context.isStageApplied(i));
            }
        }
    }

    public void testMetadataFailsWhenPositionNotSet() {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(randomBlockSize(), pipelineLength);

        expectThrows(AssertionError.class, context::metadata);
    }

    public void testClearResetsBitmapAndMetadata() {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, pipelineLength);

        final int positions = randomIntBetween(1, pipelineLength);
        for (int p = 0; p < positions; p++) {
            context.setCurrentPosition(p);
            context.metadata().writeVLong(randomLongBetween(0, Long.MAX_VALUE));
        }
        context.setValueCount(blockSize);

        context.clear();

        assertEquals(0, context.positionBitmap());
        assertEquals(0, context.valueCount());
        assertEquals(pipelineLength, context.pipelineLength());
    }

    public void testWriteStageMetadata() throws IOException {
        final int pipelineLength = randomIntBetween(2, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(randomBlockSize(), pipelineLength);

        final int positions = randomIntBetween(1, pipelineLength - 1);
        for (int p = 0; p < positions; p++) {
            context.setCurrentPosition(p);
            context.metadata().writeVLong(randomLongBetween(0, Long.MAX_VALUE));
        }

        final byte[] buffer = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        context.writeStageMetadata(out);

        assertTrue(out.getPosition() > 0);
    }

    public void testContextReusableAcrossMultipleBlocks() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 3);

        for (int block = 0; block < 10; block++) {
            context.clear();

            final int positions = randomIntBetween(1, 3);
            for (int p = 0; p < positions; p++) {
                context.setCurrentPosition(p);
                context.metadata().writeVInt(block * 10 + p);
            }

            for (int p = 0; p < positions; p++) {
                assertTrue(context.isStageApplied(p));
            }
        }
    }

    public void testValueCount() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 1);

        context.setValueCount(blockSize);
        assertEquals(blockSize, context.valueCount());
    }

    public void testBlockSize() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 1);
        assertEquals(blockSize, context.blockSize());
    }

    public void testPipelineLength() {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(randomBlockSize(), pipelineLength);
        assertEquals(pipelineLength, context.pipelineLength());
    }

    public void testSingleBufferSharedAcrossPositions() throws IOException {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 4);

        context.setCurrentPosition(0);
        context.metadata().writeByte((byte) 0xAA);
        context.setCurrentPosition(1);
        context.metadata().writeByte((byte) 0xBB).writeByte((byte) 0xCC);
        context.setCurrentPosition(2);
        context.metadata().writeByte((byte) 0xDD);

        final byte[] buffer = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        context.writeStageMetadata(out);

        assertEquals(4, out.getPosition());
    }

    public void testWriteStageMetadataWritesInReverseOrder() throws IOException {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 3);

        context.setCurrentPosition(0);
        context.metadata().writeByte((byte) 0x00);
        context.setCurrentPosition(1);
        context.metadata().writeByte((byte) 0x11);

        final byte[] buffer = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        context.writeStageMetadata(out);

        assertEquals(2, out.getPosition());
        assertEquals((byte) 0x11, buffer[0]);
        assertEquals((byte) 0x00, buffer[1]);
    }

    public void testSetCurrentPositionOutOfRangeThrows() {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final EncodingContext context = new EncodingContext(randomBlockSize(), pipelineLength);
        expectThrows(IllegalArgumentException.class, () -> context.setCurrentPosition(-1));
        expectThrows(IllegalArgumentException.class, () -> context.setCurrentPosition(pipelineLength));
    }
}
