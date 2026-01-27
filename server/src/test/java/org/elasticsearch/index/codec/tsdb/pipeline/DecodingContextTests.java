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
import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

public class DecodingContextTests extends ESTestCase {

    private static int randomBlockSize() {
        return ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    public void testSetPositionBitmap() {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02, 0x03 };
        final DecodingContext context = new DecodingContext(blockSize, stageIds);

        context.setPositionBitmap((short) 0b101);
        assertTrue(context.isStageApplied(0));
        assertFalse(context.isStageApplied(1));
        assertTrue(context.isStageApplied(2));
    }

    public void testClearResetsBitmap() {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02 };
        final DecodingContext context = new DecodingContext(blockSize, stageIds);

        final byte[] buffer = new byte[16];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, buffer.length);
        context.setDataInput(in);
        context.setPositionBitmap((short) 0b11);

        context.clear();

        assertFalse(context.isStageApplied(0));
        assertFalse(context.isStageApplied(1));
        assertEquals(2, context.pipelineLength());
    }

    public void testBlockSize() {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01 };
        final DecodingContext context = new DecodingContext(blockSize, stageIds);
        assertEquals(blockSize, context.blockSize());
    }

    public void testPipelineLength() {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02, 0x03, 0x04, 0x05 };
        final DecodingContext context = new DecodingContext(blockSize, stageIds);
        assertEquals(5, context.pipelineLength());
    }

    public void testMetadataReaderReadsFromDataInput() throws Exception {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01, 0x02 };
        final DecodingContext context = new DecodingContext(blockSize, stageIds);

        final byte[] buffer = { 0x42, 0x7F };
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, buffer.length);
        context.setDataInput(in);
        context.setPositionBitmap((short) 0b11);

        final MetadataReader reader = context.metadata();
        assertEquals((byte) 0x42, reader.readByte());
        assertEquals((byte) 0x7F, reader.readByte());
    }

    public void testSetDataInput() {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { 0x01 };
        final DecodingContext context = new DecodingContext(blockSize, stageIds);

        final byte[] buffer = new byte[16];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, buffer.length);

        context.setDataInput(in);
        context.setPositionBitmap((short) 0b1);
        assertNotNull(context.metadata());
    }
}
