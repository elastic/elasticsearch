/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.test.ESTestCase;

public class MetadataBufferTests extends ESTestCase {

    public void testWriteReadRandomBytes() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(1, 500);
        final byte[] expected = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        assertEquals(numBytes, buffer.size());
        assertEquals(numBytes, buffer.position());

        buffer.setPosition(0);
        for (int i = 0; i < numBytes; i++) {
            assertEquals(expected[i], buffer.readByte());
        }
    }

    public void testWriteReadRandomVInts() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 1000);
        final int[] values = new int[numValues];

        for (int i = 0; i < numValues; i++) {
            values[i] = randomIntBetween(0, Integer.MAX_VALUE);
            buffer.writeVInt(values[i]);
        }

        buffer.setPosition(0);
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], buffer.readVInt());
        }
    }

    public void testWriteReadRandomVLongs() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 1000);
        final long[] values = new long[numValues];

        for (int i = 0; i < numValues; i++) {
            values[i] = randomLongBetween(0, Long.MAX_VALUE);
            buffer.writeVLong(values[i]);
        }

        buffer.setPosition(0);
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], buffer.readVLong());
        }
    }

    public void testWriteReadRandomZLongs() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(100, 1000);
        final long[] values = new long[numValues];

        for (int i = 0; i < numValues; i++) {
            values[i] = randomLong();
            buffer.writeZLong(values[i]);
        }

        buffer.setPosition(0);
        for (int i = 0; i < numValues; i++) {
            assertEquals(values[i], buffer.readZLong());
        }
    }

    public void testVIntBoundaryValues() {
        final MetadataBuffer buffer = new MetadataBuffer();

        final int[] boundaryValues = { 0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, 0x1FFFFF, 0x200000, 0x0FFFFFFF, 0x10000000, Integer.MAX_VALUE };

        for (final int value : boundaryValues) {
            buffer.writeVInt(value);
        }

        buffer.setPosition(0);
        for (final int expected : boundaryValues) {
            assertEquals(expected, buffer.readVInt());
        }
    }

    public void testVLongBoundaryValues() {
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

        for (final long value : boundaryValues) {
            buffer.writeVLong(value);
        }

        buffer.setPosition(0);
        for (final long expected : boundaryValues) {
            assertEquals(expected, buffer.readVLong());
        }
    }

    public void testZLongBoundaryValues() {
        final MetadataBuffer buffer = new MetadataBuffer();

        final long[] boundaryValues = { 0L, 1L, -1L, 63L, -64L, 64L, -65L, 8191L, -8192L, Long.MAX_VALUE, Long.MIN_VALUE };

        for (final long value : boundaryValues) {
            buffer.writeZLong(value);
        }

        buffer.setPosition(0);
        for (final long expected : boundaryValues) {
            assertEquals(expected, buffer.readZLong());
        }
    }

    public void testVIntNegativeThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int negativeValue = randomIntBetween(Integer.MIN_VALUE, -1);
        expectThrows(IllegalArgumentException.class, () -> buffer.writeVInt(negativeValue));
    }

    public void testVLongNegativeThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final long negativeValue = randomLongBetween(Long.MIN_VALUE, -1);
        expectThrows(IllegalArgumentException.class, () -> buffer.writeVLong(negativeValue));
    }

    public void testRandomMixedTypes() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numOperations = randomIntBetween(50, 200);

        final Object[] expected = new Object[numOperations];
        final int[] types = new int[numOperations];

        for (int i = 0; i < numOperations; i++) {
            final int type = randomIntBetween(0, 3);
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
                    final long v = randomLong();
                    expected[i] = v;
                    buffer.writeZLong(v);
                }
            }
        }

        buffer.setPosition(0);

        for (int i = 0; i < numOperations; i++) {
            switch (types[i]) {
                case 0 -> assertEquals((byte) expected[i], buffer.readByte());
                case 1 -> assertEquals((int) expected[i], buffer.readVInt());
                case 2 -> assertEquals((long) expected[i], buffer.readVLong());
                case 3 -> assertEquals((long) expected[i], buffer.readZLong());
            }
        }
    }

    public void testAutoGrowWithRandomData() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(200, 1000);
        final byte[] expected = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        assertEquals(numBytes, buffer.size());
        assertEquals(numBytes, buffer.position());
        assertTrue(buffer.capacity() >= numBytes);

        buffer.setPosition(0);
        for (int i = 0; i < numBytes; i++) {
            assertEquals(expected[i], buffer.readByte());
        }
    }

    public void testAutoGrowRetainsCapacity() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(100, 500);

        for (int i = 0; i < numBytes; i++) {
            buffer.writeByte(randomByte());
        }

        final int capacityAfterGrow = buffer.capacity();
        assertTrue(capacityAfterGrow >= numBytes);

        buffer.clear();
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.position());
        assertEquals(capacityAfterGrow, buffer.capacity());
    }

    public void testMultipleGrowthCycles() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int initialCapacity = buffer.capacity();

        // Force multiple growth cycles: 64 -> 128 -> 256 -> 512 -> 1024
        final int targetSize = initialCapacity * 16;
        final byte[] expected = new byte[targetSize];

        int previousCapacity = initialCapacity;
        int growthCount = 0;

        for (int i = 0; i < targetSize; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);

            final int currentCapacity = buffer.capacity();
            if (currentCapacity > previousCapacity) {
                growthCount++;
                assertTrue("Capacity should at least double", currentCapacity >= previousCapacity * 2);
                previousCapacity = currentCapacity;
            }
        }

        assertTrue("Buffer should have grown multiple times", growthCount >= 4);
        assertEquals(targetSize, buffer.size());

        buffer.setPosition(0);
        for (int i = 0; i < targetSize; i++) {
            assertEquals(expected[i], buffer.readByte());
        }
    }

    public void testClearAndReuse() {
        final MetadataBuffer buffer = new MetadataBuffer();

        final int firstValue = randomIntBetween(0, Integer.MAX_VALUE);
        final long secondValue = randomLongBetween(0, Long.MAX_VALUE);

        buffer.writeVInt(firstValue);
        buffer.writeVLong(secondValue);

        assertTrue(buffer.size() > 0);
        assertTrue(buffer.position() > 0);

        buffer.clear();

        assertEquals(0, buffer.size());
        assertEquals(0, buffer.position());

        final int newValue = randomIntBetween(0, Integer.MAX_VALUE);
        buffer.writeVInt(newValue);
        final int writtenBytes = buffer.size();
        assertTrue(writtenBytes > 0);
        assertEquals(writtenBytes, buffer.position());

        buffer.setPosition(0);
        assertEquals(newValue, buffer.readVInt());
        assertEquals(writtenBytes, buffer.size());
        assertEquals(writtenBytes, buffer.position());
    }

    public void testReadPastEndThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(1, 10);
        for (int i = 0; i < numBytes; i++) {
            buffer.writeByte(randomByte());
        }

        buffer.setPosition(0);
        for (int i = 0; i < numBytes; i++) {
            buffer.readByte();
        }

        expectThrows(IllegalStateException.class, buffer::readByte);
    }

    public void testEmptyBufferReadThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        assertEquals(0, buffer.size());
        expectThrows(IllegalStateException.class, buffer::readByte);
    }

    public void testEmptyBufferReadVIntThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        expectThrows(IllegalStateException.class, buffer::readVInt);
    }

    public void testEmptyBufferReadVLongThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        expectThrows(IllegalStateException.class, buffer::readVLong);
    }

    public void testEmptyBufferReadZLongThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        expectThrows(IllegalStateException.class, buffer::readZLong);
    }

    public void testInvalidVIntEncodingThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 0x80);
        buffer.writeByte((byte) 0x80);
        buffer.writeByte((byte) 0x80);
        buffer.writeByte((byte) 0x80);
        buffer.writeByte((byte) 0x70);

        buffer.setPosition(0);
        expectThrows(IllegalStateException.class, buffer::readVInt);
    }

    public void testInvalidVLongEncodingThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        // Write 9 bytes all with continuation bit set (invalid for VLong)
        for (int i = 0; i < 9; i++) {
            buffer.writeByte((byte) 0x80);
        }

        buffer.setPosition(0);
        expectThrows(IllegalStateException.class, buffer::readVLong);
    }

    public void testInvalidZLongEncodingThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        // Write 9 bytes with continuation bits, then 10th byte with invalid bits (bits 1-7 should be 0)
        for (int i = 0; i < 9; i++) {
            buffer.writeByte((byte) 0x80);
        }
        buffer.writeByte((byte) 0x02);  // Invalid: bit 1 is set, only bit 0 is allowed

        buffer.setPosition(0);
        expectThrows(IllegalStateException.class, buffer::readZLong);
    }

    public void testReadVIntPastEndThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 0x80);

        buffer.setPosition(0);
        expectThrows(IllegalStateException.class, buffer::readVInt);
    }

    public void testReadVLongPastEndThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 0x80);
        buffer.writeByte((byte) 0x80);

        buffer.setPosition(0);
        expectThrows(IllegalStateException.class, buffer::readVLong);
    }

    public void testReadZLongPastEndThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte((byte) 0x80);
        buffer.writeByte((byte) 0x80);

        buffer.setPosition(0);
        expectThrows(IllegalStateException.class, buffer::readZLong);
    }

    public void testRandomPositionAccess() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(10, 100);
        final byte[] expected = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        for (int i = 0; i < 20; i++) {
            final int pos = randomIntBetween(0, numBytes - 1);
            buffer.setPosition(pos);
            assertEquals(expected[pos], buffer.readByte());
        }
    }

    public void testSetPositionInvalidThrows() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(1, 10);
        for (int i = 0; i < numBytes; i++) {
            buffer.writeByte(randomByte());
        }

        expectThrows(IllegalArgumentException.class, () -> buffer.setPosition(-1));
        expectThrows(IllegalArgumentException.class, () -> buffer.setPosition(numBytes + 1));
    }

    public void testMultipleClearCyclesRandom() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numCycles = randomIntBetween(5, 20);

        for (int cycle = 0; cycle < numCycles; cycle++) {
            buffer.clear();
            final int numValues = randomIntBetween(10, 100);
            final int[] values = new int[numValues];

            for (int i = 0; i < numValues; i++) {
                values[i] = randomIntBetween(0, Integer.MAX_VALUE);
                buffer.writeVInt(values[i]);
            }

            buffer.setPosition(0);
            for (int i = 0; i < numValues; i++) {
                assertEquals(values[i], buffer.readVInt());
            }
        }
    }

    public void testPositionAdvancesCorrectlyRandom() {
        final MetadataBuffer buffer = new MetadataBuffer();
        int expectedPosition;

        final int numOperations = randomIntBetween(20, 50);

        for (int i = 0; i < numOperations; i++) {
            final int type = randomIntBetween(0, 3);

            switch (type) {
                case 0 -> buffer.writeByte(randomByte());
                case 1 -> buffer.writeVInt(randomIntBetween(0, Integer.MAX_VALUE));
                case 2 -> buffer.writeVLong(randomLongBetween(0, Long.MAX_VALUE));
                case 3 -> buffer.writeZLong(randomLong());
            }

            expectedPosition = buffer.size();
            assertEquals(expectedPosition, buffer.position());
        }
    }

    public void testLargeRandomSequence() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numValues = randomIntBetween(1000, 5000);
        final long[] values = new long[numValues];

        for (int i = 0; i < numValues; i++) {
            values[i] = randomLong();
            buffer.writeZLong(values[i]);
        }

        buffer.setPosition(0);
        for (int i = 0; i < numValues; i++) {
            assertEquals("Value at index " + i + " should match", values[i], buffer.readZLong());
        }
    }

    public void testToByteArrayReturnsCopy() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int numBytes = randomIntBetween(10, 100);
        final byte[] expected = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            expected[i] = randomByte();
            buffer.writeByte(expected[i]);
        }

        final byte[] copy = buffer.toByteArray();

        // Verify length matches size, not internal capacity
        assertEquals(numBytes, copy.length);

        // Verify contents match
        for (int i = 0; i < numBytes; i++) {
            assertEquals(expected[i], copy[i]);
        }

        // Verify it's a copy - modifying it doesn't affect original
        copy[0] = (byte) (copy[0] + 1);
        buffer.setPosition(0);
        assertEquals(expected[0], buffer.readByte());
    }

    public void testToByteArrayEmptyBuffer() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte[] copy = buffer.toByteArray();
        assertEquals(0, copy.length);
    }
}
