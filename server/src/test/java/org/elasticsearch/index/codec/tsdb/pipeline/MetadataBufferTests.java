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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class MetadataBufferTests extends ESTestCase {

    public void testWriteRandomBytes() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(1, 500);
        final byte[] expected = new byte[numBytes];

        // WHEN
        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        // THEN
        assertEquals(numBytes, buffer.size());
        assertArrayEquals(expected, buffer.toByteArray());
    }

    public void testWriteByteGrowsBuffer() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = 200; // Exceeds default capacity of 64
        final byte[] expected = new byte[numBytes];

        // WHEN
        for (int i = 0; i < numBytes; i++) {
            expected[i] = (byte) i;
            buffer.writeByte(expected[i]);
        }

        // THEN
        assertEquals(numBytes, buffer.size());
        assertArrayEquals(expected, buffer.toByteArray());
    }

    public void testWriteRandomVInts() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 1000);
        final int[] values = new int[numValues];

        // WHEN
        for (int i = 0; i < numValues; i++) {
            values[i] = randomIntBetween(0, Integer.MAX_VALUE);
            buffer.writeVInt(values[i]);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], input.readVInt());
        }
    }

    public void testVIntBoundaryValues() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int[] boundaryValues = { 0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, 0x1FFFFF, 0x200000, 0x0FFFFFFF, 0x10000000, Integer.MAX_VALUE };

        // WHEN
        for (final int value : boundaryValues) {
            buffer.writeVInt(value);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (final int expected : boundaryValues) {
            assertEquals(expected, input.readVInt());
        }
    }

    public void testVIntNegativeThrows() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int negativeValue = randomIntBetween(Integer.MIN_VALUE, -1);

        // WHEN/THEN
        expectThrows(IllegalArgumentException.class, () -> buffer.writeVInt(negativeValue));
    }

    public void testWriteRandomVLongs() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 1000);
        final long[] values = new long[numValues];

        // WHEN
        for (int i = 0; i < numValues; i++) {
            values[i] = randomLongBetween(0, Long.MAX_VALUE);
            buffer.writeVLong(values[i]);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], input.readVLong());
        }
    }

    public void testVLongBoundaryValues() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final long[] boundaryValues = {
            0L,
            1L,
            0x7FL,
            0x80L,
            0x3FFFL,
            0x4000L,
            0x1FFFFFL,
            0x200000L,
            0x0FFFFFFFL,
            0x10000000L,
            0x7FFFFFFFFL,
            0x800000000L,
            0x3FFFFFFFFFFL,
            0x40000000000L,
            0x1FFFFFFFFFFFFL,
            0x2000000000000L,
            0x0FFFFFFFFFFFFFFL,
            Long.MAX_VALUE };

        // WHEN
        for (final long value : boundaryValues) {
            buffer.writeVLong(value);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (final long expected : boundaryValues) {
            assertEquals(expected, input.readVLong());
        }
    }

    public void testVLongNegativeThrows() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final long negativeValue = randomLongBetween(Long.MIN_VALUE, -1);

        // WHEN/THEN
        expectThrows(IllegalArgumentException.class, () -> buffer.writeVLong(negativeValue));
    }

    public void testWriteRandomZLongs() throws IOException {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 1000);
        final long[] values = new long[numValues];

        // WHEN
        for (int i = 0; i < numValues; i++) {
            values[i] = randomLong();
            buffer.writeZLong(values[i]);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], input.readZLong());
        }
    }

    public void testZLongBoundaryValues() throws IOException {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final long[] boundaryValues = { 0L, 1L, -1L, 63L, -64L, 64L, -65L, 8191L, -8192L, Long.MAX_VALUE, Long.MIN_VALUE };

        // WHEN
        for (final long value : boundaryValues) {
            buffer.writeZLong(value);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (final long expected : boundaryValues) {
            assertEquals(expected, input.readZLong());
        }
    }

    public void testWriteRandomZInts() throws IOException {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 500);
        final int[] values = new int[numValues];

        // WHEN
        for (int i = 0; i < numValues; i++) {
            values[i] = randomInt();
            buffer.writeZInt(values[i]);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], input.readZInt());
        }
    }

    public void testZIntBoundaryValues() throws IOException {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int[] boundaryValues = { 0, 1, -1, 63, -64, 64, -65, 8191, -8192, Integer.MAX_VALUE, Integer.MIN_VALUE };

        // WHEN
        for (final int value : boundaryValues) {
            buffer.writeZInt(value);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (final int expected : boundaryValues) {
            assertEquals(expected, input.readZInt());
        }
    }

    public void testRandomMixedTypes() throws IOException {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numOperations = randomIntBetween(50, 200);
        final Object[] expected = new Object[numOperations];
        final int[] types = new int[numOperations];

        // WHEN
        for (int i = 0; i < numOperations; i++) {
            final int type = randomIntBetween(0, 4);
            types[i] = type;
            switch (type) {
                case 0 -> {
                    final byte b = randomByte();
                    expected[i] = b;
                    buffer.writeByte(b);
                }
                case 1 -> {
                    final int v = randomIntBetween(0, Integer.MAX_VALUE);
                    expected[i] = v;
                    buffer.writeVInt(v);
                }
                case 2 -> {
                    final long v = randomLongBetween(0, Long.MAX_VALUE);
                    expected[i] = v;
                    buffer.writeVLong(v);
                }
                case 3 -> {
                    final int v = randomInt();
                    expected[i] = v;
                    buffer.writeZInt(v);
                }
                case 4 -> {
                    final long v = randomLong();
                    expected[i] = v;
                    buffer.writeZLong(v);
                }
            }
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (int i = 0; i < numOperations; i++) {
            switch (types[i]) {
                case 0 -> assertEquals((byte) expected[i], input.readByte());
                case 1 -> assertEquals((int) expected[i], input.readVInt());
                case 2 -> assertEquals((long) expected[i], input.readVLong());
                case 3 -> assertEquals((int) expected[i], input.readZInt());
                case 4 -> assertEquals((long) expected[i], input.readZLong());
            }
        }
    }

    public void testAutoGrowWithLargeData() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int targetSize = 1024;
        final byte[] expected = new byte[targetSize];

        // WHEN
        for (int i = 0; i < targetSize; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        // THEN
        assertEquals(targetSize, buffer.size());
        assertArrayEquals(expected, buffer.toByteArray());
    }

    public void testLargeZLongSequence() throws IOException {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(1000, 5000);
        final long[] values = new long[numValues];

        // WHEN
        for (int i = 0; i < numValues; i++) {
            values[i] = randomLong();
            buffer.writeZLong(values[i]);
        }

        // THEN
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], input.readZLong());
        }
    }

    public void testClearResetsSize() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(0, Integer.MAX_VALUE));
        buffer.writeVLong(randomLongBetween(0, Long.MAX_VALUE));
        assertTrue(buffer.size() > 0);

        // WHEN
        buffer.clear();

        // THEN
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.toByteArray().length);
    }

    public void testClearRetainsCapacityForReuse() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(100, 500);
        final byte[] expected = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        // WHEN
        buffer.clear();
        for (int i = 0; i < numBytes; i++) {
            buffer.writeByte(expected[i]);
        }

        // THEN
        assertEquals(numBytes, buffer.size());
        assertArrayEquals(expected, buffer.toByteArray());
    }

    public void testClearAndReuseWithDifferentData() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(0, Integer.MAX_VALUE));
        buffer.writeVLong(randomLongBetween(0, Long.MAX_VALUE));

        // WHEN
        buffer.clear();
        final int newValue = randomIntBetween(0, Integer.MAX_VALUE);
        buffer.writeVInt(newValue);

        // THEN
        assertTrue(buffer.size() > 0);
        final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
        assertEquals(newValue, input.readVInt());
    }

    public void testMultipleClearCycles() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numCycles = randomIntBetween(5, 20);

        // WHEN/THEN
        for (int cycle = 0; cycle < numCycles; cycle++) {
            buffer.clear();
            final int numValues = randomIntBetween(10, 100);
            final int[] values = new int[numValues];
            for (int i = 0; i < numValues; i++) {
                values[i] = randomIntBetween(0, Integer.MAX_VALUE);
                buffer.writeVInt(values[i]);
            }
            final ByteArrayDataInput input = new ByteArrayDataInput(buffer.toByteArray());
            for (int i = 0; i < numValues; i++) {
                assertEquals(values[i], input.readVInt());
            }
        }
    }

    public void testToByteArrayReturnsCopy() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(10, 100);
        final byte[] expected = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        // WHEN
        final byte[] copy1 = buffer.toByteArray();
        final byte[] copy2 = buffer.toByteArray();
        copy1[0] = (byte) (copy1[0] + 1);

        // THEN
        assertEquals(numBytes, copy1.length);
        assertEquals(numBytes, copy2.length);
        assertNotEquals(copy1[0], copy2[0]);
        assertEquals(expected[0], copy2[0]);
    }

    public void testToByteArrayEmptyBuffer() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();

        // WHEN
        final byte[] copy = buffer.toByteArray();

        // THEN
        assertEquals(0, copy.length);
    }

    public void testWriteToDestinationArray() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(10, 100);
        final byte[] expected = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }
        final byte[] dest = new byte[numBytes + 20];
        final int offset = randomIntBetween(0, 10);

        // WHEN
        int bytesWritten = buffer.writeTo(dest, offset);

        // THEN
        assertEquals(numBytes, bytesWritten);
        assertArrayEquals(expected, Arrays.copyOfRange(dest, offset, offset + numBytes));
    }

    public void testWriteToEmptyBuffer() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte[] dest = new byte[10];

        // WHEN
        int bytesWritten = buffer.writeTo(dest, 0);

        // THEN
        assertEquals(0, bytesWritten);
    }

    public void testSizeAfterWrites() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();

        // WHEN/THEN
        assertEquals(0, buffer.size());
        buffer.writeByte((byte) 1);
        assertEquals(1, buffer.size());
        buffer.writeVInt(127);
        assertEquals(2, buffer.size()); // 1 byte for value <= 127
        buffer.writeVInt(128);
        assertEquals(4, buffer.size()); // 2 more bytes for value 128
    }

    public void testSizeMatchesToByteArrayLength() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numOperations = randomIntBetween(20, 50);

        // WHEN
        for (int i = 0; i < numOperations; i++) {
            final int type = randomIntBetween(0, 4);
            switch (type) {
                case 0 -> buffer.writeByte(randomByte());
                case 1 -> buffer.writeVInt(randomIntBetween(0, Integer.MAX_VALUE));
                case 2 -> buffer.writeVLong(randomLongBetween(0, Long.MAX_VALUE));
                case 3 -> buffer.writeZInt(randomInt());
                case 4 -> buffer.writeZLong(randomLong());
            }
        }

        // THEN
        assertEquals(buffer.size(), buffer.toByteArray().length);
    }

    public void testWriteToInsufficientSpace() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 2);
        buffer.writeByte((byte) 3);
        final byte[] dest = new byte[2];

        // WHEN/THEN
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> buffer.writeTo(dest, 0));
        assertTrue(e.getMessage().contains("insufficient space"));
    }

    public void testWriteToInsufficientSpaceWithOffset() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 1);
        buffer.writeByte((byte) 2);
        final byte[] dest = new byte[3];
        final int offset = 2;

        // WHEN/THEN
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> buffer.writeTo(dest, offset));
        assertTrue(e.getMessage().contains("insufficient space"));
    }

    public void testWriteToNegativeOffset() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 1);
        final byte[] dest = new byte[10];

        // WHEN/THEN
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> buffer.writeTo(dest, -1));
        assertTrue(e.getMessage().contains("non-negative"));
    }

    public void testWriteToNullDestination() {
        // GIVEN
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 1);

        // WHEN/THEN
        expectThrows(NullPointerException.class, () -> buffer.writeTo(null, 0));
    }
}
