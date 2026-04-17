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

public class MetadataBufferTests extends ESTestCase {

    public void testNewBufferIsEmpty() {
        final MetadataBuffer buffer = new MetadataBuffer();
        assertEquals(0, buffer.size());
    }

    public void testClearResetsBuffer() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(1, 10000));
        buffer.writeVLong(randomLongBetween(1L, 100000L));

        buffer.clear();

        assertEquals(0, buffer.size());
    }

    public void testBufferReusableAfterClear() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(1, 10000));
        buffer.clear();

        final long expected = randomLongBetween(1L, 100000L);
        buffer.writeVLong(expected);

        final byte[] output = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        assertEquals(expected, in.readVLong());
    }

    public void testBufferGrowsWithLargeData() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int count = randomIntBetween(100, 200);

        for (int i = 0; i < count; i++) {
            buffer.writeVInt(i);
        }

        final int expectedSize = buffer.size();
        assertTrue("Buffer should have grown beyond default capacity", expectedSize > 64);

        final byte[] output = new byte[4096];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        assertEquals("writeTo should write exactly size() bytes", expectedSize, out.getPosition());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        for (int i = 0; i < count; i++) {
            assertEquals(i, in.readVInt());
        }
    }

    public void testWriteToSlice() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();

        final int first = randomIntBetween(1, 1000);
        final int second = randomIntBetween(1001, 2000);
        final int third = randomIntBetween(2001, 3000);

        buffer.writeVInt(first);
        final int offset1 = buffer.size();
        buffer.writeVInt(second);
        final int offset2 = buffer.size();
        buffer.writeVInt(third);

        final byte[] output = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, offset1, offset2 - offset1);

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        assertEquals(second, in.readVInt());
    }

    public void testWriteToEmptySlice() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(1, 10000));

        final byte[] output = new byte[64];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, 0);

        assertEquals(0, out.getPosition());
    }

    public void testWriteBytesWithOffsetAndLength() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte[] source = randomByteArrayOfLength(randomIntBetween(10, 50));
        final int offset = randomIntBetween(0, source.length / 2);
        final int length = randomIntBetween(1, source.length - offset);

        buffer.writeBytes(source, offset, length);

        assertEquals(length, buffer.size());

        final byte[] output = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        final byte[] read = new byte[length];
        in.readBytes(read, 0, length);

        final byte[] expected = new byte[length];
        System.arraycopy(source, offset, expected, 0, length);
        assertArrayEquals(expected, read);
    }

    public void testRoundtrip() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte byteVal = randomByte();
        final int intVal = randomIntBetween(0, Integer.MAX_VALUE);
        final long longVal = randomLongBetween(0L, Long.MAX_VALUE);
        final int zintVal = randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
        final long zlongVal = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);

        buffer.writeByte(byteVal);
        buffer.writeVInt(intVal);
        buffer.writeVLong(longVal);
        buffer.writeZInt(zintVal);
        buffer.writeZLong(zlongVal);

        final byte[] output = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        assertEquals(byteVal, in.readByte());
        assertEquals(intVal, in.readVInt());
        assertEquals(longVal, in.readVLong());
        assertEquals(zintVal, in.readZInt());
        assertEquals(zlongVal, in.readZLong());
    }

    public void testWriteLongBoundaryValues() throws IOException {
        final long[] values = new long[] { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x00000000FFFFFFFFL, 0xFFFFFFFF00000000L };

        for (final long value : values) {
            final MetadataBuffer buffer = new MetadataBuffer();
            buffer.writeLong(value);

            final byte[] output = new byte[64];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
            buffer.writeTo(out, 0, buffer.size());

            assertEquals(
                "Roundtrip failed for value " + Long.toHexString(value),
                value,
                new ByteArrayDataInput(output, 0, out.getPosition()).readLong()
            );

            final byte[] luceneOutput = new byte[Long.BYTES];
            new ByteArrayDataOutput(luceneOutput).writeLong(value);
            final byte[] bufferBytes = new byte[Long.BYTES];
            new ByteArrayDataOutput(bufferBytes).writeBytes(output, 0, Long.BYTES);
            assertArrayEquals("Byte layout mismatch for value " + Long.toHexString(value), luceneOutput, bufferBytes);
        }
    }

    public void testWriteLongMultipleValues() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int count = randomIntBetween(5, 20);
        final long[] values = new long[count];

        for (int i = 0; i < count; i++) {
            values[i] = randomLong();
            buffer.writeLong(values[i]);
        }

        assertEquals(count * Long.BYTES, buffer.size());

        final byte[] output = new byte[count * Long.BYTES];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        for (int i = 0; i < count; i++) {
            assertEquals("Value at index " + i, values[i], in.readLong());
        }
    }

    public void testWriteLongMixedWithOtherTypes() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte byteVal = randomByte();
        final long longVal = randomLong();
        final int vintVal = randomIntBetween(0, Integer.MAX_VALUE);
        final long longVal2 = randomLong();

        buffer.writeByte(byteVal);
        buffer.writeLong(longVal);
        buffer.writeVInt(vintVal);
        buffer.writeLong(longVal2);

        final byte[] output = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        assertEquals(byteVal, in.readByte());
        assertEquals(longVal, in.readLong());
        assertEquals(vintVal, in.readVInt());
        assertEquals(longVal2, in.readLong());
    }

    public void testWriteBytesConvenienceMethod() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte[] source = randomByteArrayOfLength(randomIntBetween(5, 20));

        buffer.writeBytes(source);

        assertEquals(source.length, buffer.size());

        final byte[] output = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
        buffer.writeTo(out, 0, buffer.size());

        final ByteArrayDataInput in = new ByteArrayDataInput(output, 0, out.getPosition());
        final byte[] read = new byte[source.length];
        in.readBytes(read, 0, source.length);
        assertArrayEquals(source, read);
    }

    public void testEmptyMethod() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.empty();
        assertEquals(0, buffer.size());
    }

    public void testWriteIntBoundaryValues() throws IOException {
        final int[] values = new int[] { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE };

        for (final int value : values) {
            final MetadataBuffer buffer = new MetadataBuffer();
            buffer.writeInt(value);

            final byte[] output = new byte[64];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(output);
            buffer.writeTo(out, 0, buffer.size());

            assertEquals(
                "Roundtrip failed for value " + Integer.toHexString(value),
                value,
                new ByteArrayDataInput(output, 0, out.getPosition()).readInt()
            );

            final byte[] luceneOutput = new byte[Integer.BYTES];
            new ByteArrayDataOutput(luceneOutput).writeInt(value);
            final byte[] bufferBytes = new byte[Integer.BYTES];
            new ByteArrayDataOutput(bufferBytes).writeBytes(output, 0, Integer.BYTES);
            assertArrayEquals("Byte layout mismatch for value " + Integer.toHexString(value), luceneOutput, bufferBytes);
        }
    }

}
