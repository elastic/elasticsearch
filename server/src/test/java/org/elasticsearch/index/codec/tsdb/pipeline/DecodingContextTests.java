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

public class DecodingContextTests extends ESTestCase {

    private static final int BASE_BLOCK_SIZE = 128;

    private static int randomBlockSize() {
        return BASE_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    private static int randomPositionExcluding(final int position, final int pipelineLength) {
        return (position + 1 + randomIntBetween(0, pipelineLength - 2)) % pipelineLength;
    }

    public void testSetPositionBitmap() {
        final int pipelineLength = randomIntBetween(2, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final DecodingContext context = new DecodingContext(randomBlockSize(), pipelineLength);

        final int applied = randomIntBetween(0, pipelineLength - 1);
        final int notApplied = randomPositionExcluding(applied, pipelineLength);
        context.setPositionBitmap((short) (1 << applied));
        assertTrue(context.isStageApplied(applied));
        assertFalse(context.isStageApplied(notApplied));
    }

    public void testClearResetsBitmap() {
        final int pipelineLength = randomIntBetween(2, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final DecodingContext context = new DecodingContext(randomBlockSize(), pipelineLength);

        short bitmap = 0;
        for (int i = 0; i < pipelineLength; i++) {
            if (randomBoolean()) {
                bitmap |= (short) (1 << i);
            }
        }
        final byte[] buffer = new byte[16];
        context.setDataInput(new ByteArrayDataInput(buffer, 0, buffer.length));
        context.setPositionBitmap(bitmap);

        context.clear();

        for (int i = 0; i < pipelineLength; i++) {
            assertFalse(context.isStageApplied(i));
        }
        assertEquals(pipelineLength, context.pipelineLength());
    }

    public void testBlockSize() {
        final int blockSize = randomBlockSize();
        final DecodingContext context = new DecodingContext(blockSize, 1);
        assertEquals(blockSize, context.blockSize());
    }

    public void testPipelineLength() {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final DecodingContext context = new DecodingContext(randomBlockSize(), pipelineLength);
        assertEquals(pipelineLength, context.pipelineLength());
    }

    public void testMetadataReaderReadsFromDataInput() throws Exception {
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final DecodingContext context = new DecodingContext(randomBlockSize(), pipelineLength);

        final int numBytes = randomIntBetween(1, 16);
        final byte[] buffer = randomByteArrayOfLength(numBytes);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, buffer.length);
        context.setDataInput(in);

        short bitmap = (short) (1 << randomIntBetween(0, pipelineLength - 1));
        for (int i = 0; i < pipelineLength; i++) {
            if (randomBoolean()) {
                bitmap |= (short) (1 << i);
            }
        }
        context.setPositionBitmap(bitmap);

        final MetadataReader reader = context.metadata();
        for (int i = 0; i < numBytes; i++) {
            assertEquals(buffer[i], reader.readByte());
        }
    }

    public void testReadBytesConvenienceMethod() throws Exception {
        final int numBytes = randomIntBetween(1, 64);
        final byte[] source = randomByteArrayOfLength(numBytes);
        final int pipelineLength = randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH);
        final DecodingContext context = new DecodingContext(randomBlockSize(), pipelineLength);
        context.setDataInput(new ByteArrayDataInput(source, 0, source.length));
        context.setPositionBitmap((short) (1 << randomIntBetween(0, pipelineLength - 1)));

        final byte[] dest = new byte[numBytes];
        context.metadata().readBytes(dest);
        assertArrayEquals(source, dest);
    }

    public void testReverseWrittenMetadataIsReadSequentially() throws IOException {
        final int numTransformStages = randomIntBetween(2, 8);
        final int pipelineLength = numTransformStages + 1;
        final EncodingContext encodingContext = new EncodingContext(randomBlockSize(), pipelineLength);

        for (int pos = 0; pos < numTransformStages; pos++) {
            encodingContext.setCurrentPosition(pos);
            encodingContext.metadata().writeVLong(pos);
        }

        final byte[] buffer = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        encodingContext.writeStageMetadata(out);

        final DecodingContext decodingContext = new DecodingContext(randomBlockSize(), pipelineLength);
        decodingContext.setDataInput(new ByteArrayDataInput(buffer, 0, out.getPosition()));
        decodingContext.setPositionBitmap(encodingContext.positionBitmap());

        for (int pos = numTransformStages - 1; pos >= 0; pos--) {
            assertEquals(pos, decodingContext.readVLong());
        }
    }

    public void testAllMetadataReaderMethodsRoundtrip() throws Exception {
        final byte byteVal = randomByte();
        final int intVal = randomInt();
        final long longVal = randomLong();
        final int vintVal = randomIntBetween(0, Integer.MAX_VALUE);
        final long vlongVal = randomLongBetween(0L, Long.MAX_VALUE);
        final int zintVal = randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
        final long zlongVal = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);

        final MetadataBuffer writer = new MetadataBuffer();
        writer.writeByte(byteVal);
        writer.writeInt(intVal);
        writer.writeLong(longVal);
        writer.writeVInt(vintVal);
        writer.writeVLong(vlongVal);
        writer.writeZInt(zintVal);
        writer.writeZLong(zlongVal);

        final byte[] buffer = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        writer.writeTo(out, 0, writer.size());

        final DecodingContext context = new DecodingContext(randomBlockSize(), 1);
        context.setDataInput(new ByteArrayDataInput(buffer, 0, out.getPosition()));
        context.setPositionBitmap((short) 0b1);

        assertEquals(byteVal, context.readByte());
        assertEquals(intVal, context.readInt());
        assertEquals(longVal, context.readLong());
        assertEquals(vintVal, context.readVInt());
        assertEquals(vlongVal, context.readVLong());
        assertEquals(zintVal, context.readZInt());
        assertEquals(zlongVal, context.readZLong());
    }
}
