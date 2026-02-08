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
import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class EncodingContextTests extends ESTestCase {

    private static int randomBlockSize() {
        return ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    public void testNewContextStartsWithNoActivePositions() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 1);
        assertEquals(0, context.positionBitmap());
    }

    public void testActivatePositionSetsBitmapBit() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 3);

        context.applyStage(0);
        assertEquals(0b001, context.positionBitmap());
        assertTrue(context.isStageApplied(0));
        assertFalse(context.isStageApplied(1));
        assertFalse(context.isStageApplied(2));

        context.applyStage(2);
        assertEquals(0b101, context.positionBitmap());
        assertTrue(context.isStageApplied(0));
        assertFalse(context.isStageApplied(1));
        assertTrue(context.isStageApplied(2));
    }

    public void testSetCurrentPositionAndMetadata() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 2);

        context.setCurrentPosition(0);

        final MetadataWriter writer = context.metadata();
        assertNotNull(writer);
        assertTrue(context.isStageApplied(0));

        writer.writeVLong(12345L);

        context.setCurrentPosition(1);
        context.metadata().writeZInt(-100);

        assertEquals(0b11, context.positionBitmap());
    }

    public void testMetadataActivatesPosition() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 4);

        context.setCurrentPosition(1);
        context.metadata().writeVLong(100L);
        assertTrue(context.isStageApplied(1));
        assertFalse(context.isStageApplied(0));
        assertFalse(context.isStageApplied(2));
        assertFalse(context.isStageApplied(3));
    }

    public void testMetadataFailsWhenPositionNotSet() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 1);

        expectThrows(AssertionError.class, context::metadata);
    }

    public void testClearResetsBitmapAndMetadata() {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 2);

        context.setCurrentPosition(0);
        context.metadata().writeVLong(12345L);
        context.setCurrentPosition(1);
        context.metadata().writeZInt(-100);
        context.setValueCount(blockSize);

        context.clear();

        assertEquals(0, context.positionBitmap());
        assertEquals(0, context.valueCount());
        assertEquals(2, context.pipelineLength());
    }

    public void testWriteStageMetadata() throws IOException {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 3);

        context.setCurrentPosition(0);
        context.metadata().writeByte((byte) 42);
        context.setCurrentPosition(1);
        context.metadata().writeVLong(999L);

        final byte[] buffer = new byte[64];
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
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 5);
        assertEquals(5, context.pipelineLength());
    }

    public void testFluentMetadataChaining() throws IOException {
        final int blockSize = randomBlockSize();
        final EncodingContext context = new EncodingContext(blockSize, 2);

        context.setCurrentPosition(0);
        context.metadata().writeVLong(100L).writeZLong(-200L).writeZInt(10);

        assertTrue(context.isStageApplied(0));

        final byte[] buffer = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        context.writeStageMetadata(out);

        assertTrue(out.getPosition() > 0);
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
}
