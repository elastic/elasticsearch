/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

public class ArrowBufConstantDetectionTests extends ESTestCase {

    private BufferAllocator allocator;

    @Before
    public void setup() {
        allocator = new RootAllocator();
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    // ---- isUniform ----

    public void testIsUniformByteAllSame() {
        try (ArrowBuf buf = allocator.buffer(8)) {
            for (int i = 0; i < 8; i++) {
                buf.setByte(i, 7);
            }
            assertTrue(ArrowBufConstantDetection.isUniform(buf, 8, Byte.BYTES));
        }
    }

    public void testIsUniformByteOneDiffers() {
        try (ArrowBuf buf = allocator.buffer(8)) {
            for (int i = 0; i < 8; i++) {
                buf.setByte(i, 7);
            }
            buf.setByte(5, (byte) 8);
            assertFalse(ArrowBufConstantDetection.isUniform(buf, 8, Byte.BYTES));
        }
    }

    public void testIsUniformShort() {
        try (ArrowBuf buf = allocator.buffer(8L * Short.BYTES)) {
            for (int i = 0; i < 8; i++) {
                buf.setShort((long) i * Short.BYTES, 12345);
            }
            assertTrue(ArrowBufConstantDetection.isUniform(buf, 8, Short.BYTES));
            buf.setShort(6L * Short.BYTES, (short) 9999);
            assertFalse(ArrowBufConstantDetection.isUniform(buf, 8, Short.BYTES));
        }
    }

    public void testIsUniformInt() {
        try (ArrowBuf buf = allocator.buffer(16L * Integer.BYTES)) {
            for (int i = 0; i < 16; i++) {
                buf.setInt((long) i * Integer.BYTES, 42);
            }
            assertTrue(ArrowBufConstantDetection.isUniform(buf, 16, Integer.BYTES));
            buf.setInt(15L * Integer.BYTES, -1);
            assertFalse(ArrowBufConstantDetection.isUniform(buf, 16, Integer.BYTES));
        }
    }

    public void testIsUniformLong() {
        try (ArrowBuf buf = allocator.buffer(8L * Long.BYTES)) {
            for (int i = 0; i < 8; i++) {
                buf.setLong((long) i * Long.BYTES, Long.MIN_VALUE);
            }
            assertTrue(ArrowBufConstantDetection.isUniform(buf, 8, Long.BYTES));
            buf.setLong(0, 0L);
            assertFalse(ArrowBufConstantDetection.isUniform(buf, 8, Long.BYTES));
        }
    }

    public void testIsUniformSingleRowAlwaysTrue() {
        try (ArrowBuf buf = allocator.buffer(Long.BYTES)) {
            buf.setLong(0, 0xDEADBEEFL);
            assertTrue(ArrowBufConstantDetection.isUniform(buf, 1, Long.BYTES));
        }
    }

    public void testIsUniformZeroRowsTrue() {
        try (ArrowBuf buf = allocator.buffer(Long.BYTES)) {
            assertTrue(ArrowBufConstantDetection.isUniform(buf, 0, Long.BYTES));
        }
    }

    public void testIsUniformUnsupportedByteSizeThrows() {
        try (ArrowBuf buf = allocator.buffer(8)) {
            assertThrows(IllegalArgumentException.class, () -> ArrowBufConstantDetection.isUniform(buf, 8, 3));
        }
    }

    // ---- isUniformBits ----

    public void testIsUniformBitsAllTrueByteAligned() {
        try (ArrowBuf buf = allocator.buffer(2)) {
            buf.setByte(0, (byte) 0xFF);
            buf.setByte(1, (byte) 0xFF);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 16));
        }
    }

    public void testIsUniformBitsAllFalseByteAligned() {
        try (ArrowBuf buf = allocator.buffer(2)) {
            buf.setByte(0, (byte) 0x00);
            buf.setByte(1, (byte) 0x00);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 16));
        }
    }

    public void testIsUniformBitsPartialTailAllTrue() {
        // 9 bits: byte 0 = 0xFF, byte 1's low bit set, rest don't care
        try (ArrowBuf buf = allocator.buffer(2)) {
            buf.setByte(0, (byte) 0xFF);
            buf.setByte(1, (byte) 0x01);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 9));
            buf.setByte(1, (byte) 0xFF);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 9));
        }
    }

    public void testIsUniformBitsPartialTailAllFalse() {
        // 9 bits: byte 0 = 0x00, byte 1's low bit clear, rest don't care
        try (ArrowBuf buf = allocator.buffer(2)) {
            buf.setByte(0, (byte) 0x00);
            buf.setByte(1, (byte) 0xFE);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 9));
        }
    }

    public void testIsUniformBitsMixed() {
        try (ArrowBuf buf = allocator.buffer(2)) {
            buf.setByte(0, (byte) 0x0F);
            buf.setByte(1, (byte) 0x00);
            assertFalse(ArrowBufConstantDetection.isUniformBits(buf, 16));
        }
    }

    public void testIsUniformBitsSingleBit() {
        try (ArrowBuf buf = allocator.buffer(1)) {
            buf.setByte(0, (byte) 0x01);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 1));
            buf.setByte(0, (byte) 0x00);
            assertTrue(ArrowBufConstantDetection.isUniformBits(buf, 1));
        }
    }
}
