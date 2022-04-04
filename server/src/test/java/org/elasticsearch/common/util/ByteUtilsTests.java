/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ByteUtilsTests extends ESTestCase {

    public void testZigZag(long l) {
        assertEquals(l, ByteUtils.zigZagDecode(ByteUtils.zigZagEncode(l)));
    }

    public void testZigZag() {
        testZigZag(0);
        testZigZag(1);
        testZigZag(-1);
        testZigZag(Long.MAX_VALUE);
        testZigZag(Long.MIN_VALUE);
        for (int i = 0; i < 1000; ++i) {
            testZigZag(randomLong());
            assertTrue(ByteUtils.zigZagEncode(randomInt(1000)) >= 0);
            assertTrue(ByteUtils.zigZagEncode(-randomInt(1000)) >= 0);
        }
    }

    public void testFloat() throws IOException {
        final float[] data = new float[scaledRandomIntBetween(1000, 10000)];
        final byte[] encoded = new byte[data.length * 4];
        for (int i = 0; i < data.length; ++i) {
            data[i] = randomFloat();
            ByteUtils.writeFloatLE(data[i], encoded, i * 4);
        }
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], ByteUtils.readFloatLE(encoded, i * 4), Float.MIN_VALUE);
        }
    }

    public void testDouble() throws IOException {
        final double[] data = new double[scaledRandomIntBetween(1000, 10000)];
        final byte[] encoded = new byte[data.length * 8];
        for (int i = 0; i < data.length; ++i) {
            data[i] = randomDouble();
            ByteUtils.writeDoubleLE(data[i], encoded, i * 8);
        }
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], ByteUtils.readDoubleLE(encoded, i * 8), Double.MIN_VALUE);
        }
    }

    private byte[] readLongLEHelper(long number, int offset) {
        byte[] arr = new byte[8];
        ByteUtils.writeLongLE(number, arr, offset);
        return arr;
    }

    private byte[] readLongBEHelper(long number, int offset) {
        byte[] arr = new byte[8];
        ByteUtils.writeLongBE(number, arr, offset);
        return arr;
    }

    public void testLongToBytes() {
        assertThat(readLongLEHelper(123456L, 0), is(new byte[] { 64, -30, 1, 0, 0, 0, 0, 0 }));
        assertThat(readLongLEHelper(-123456L, 0), is(new byte[] { -64, 29, -2, -1, -1, -1, -1, -1 }));
        assertThat(readLongLEHelper(0L, 0), is(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }));
        assertThat(readLongLEHelper(Long.MAX_VALUE + 1, 0), is(new byte[] { 0, 0, 0, 0, 0, 0, 0, -128 }));
        assertThat(readLongLEHelper(Long.MAX_VALUE + 127, 0), is(new byte[] { 126, 0, 0, 0, 0, 0, 0, -128 }));
        assertThat(readLongLEHelper(Long.MIN_VALUE - 1, 0), is(new byte[] { -1, -1, -1, -1, -1, -1, -1, 127 }));
        assertThat(readLongLEHelper(Long.MIN_VALUE - 127, 0), is(new byte[] { -127, -1, -1, -1, -1, -1, -1, 127 }));

        assertThat(readLongBEHelper(123456L, 0), is(new byte[] { 0, 0, 0, 0, 0, 1, -30, 64 }));
        assertThat(readLongBEHelper(-123456L, 0), is(new byte[] { -1, -1, -1, -1, -1, -2, 29, -64 }));
        assertThat(readLongBEHelper(0L, 0), is(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }));
        assertThat(readLongBEHelper(Long.MAX_VALUE + 1, 0), is(new byte[] { -128, 0, 0, 0, 0, 0, 0, 0 }));
        assertThat(readLongBEHelper(Long.MAX_VALUE + 127, 0), is(new byte[] { -128, 0, 0, 0, 0, 0, 0, 126 }));
        assertThat(readLongBEHelper(Long.MIN_VALUE - 1, 0), is(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 }));
        assertThat(readLongBEHelper(Long.MIN_VALUE - 127, 0), is(new byte[] { 127, -1, -1, -1, -1, -1, -1, -127 }));
    }

    public void testBytesToLong() {
        assertThat(ByteUtils.readLongLE(new byte[] { 64, -30, 1, 0, 0, 0, 0, 0 }, 0), is(123456L));
        assertThat(ByteUtils.readLongLE(new byte[] { -64, 29, -2, -1, -1, -1, -1, -1 }, 0), is(-123456L));
        assertThat(ByteUtils.readLongLE(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0), is(0L));
        assertThat(ByteUtils.readLongLE(new byte[] { 0, 0, 0, 0, 0, 0, 0, -128 }, 0), is(Long.MIN_VALUE));
        assertThat(ByteUtils.readLongLE(new byte[] { 126, 0, 0, 0, 0, 0, 0, -128 }, 0), is(Long.MIN_VALUE + 127 - 1));
        assertThat(ByteUtils.readLongLE(new byte[] { -1, -1, -1, -1, -1, -1, -1, 127 }, 0), is(Long.MAX_VALUE));
        assertThat(ByteUtils.readLongLE(new byte[] { -127, -1, -1, -1, -1, -1, -1, 127, randomByte() }, 0), is(Long.MAX_VALUE - 127 + 1));

        assertThat(ByteUtils.readLongLE(new byte[] { randomByte(), 64, -30, 1, 0, 0, 0, 0, 0 }, 1), is(123456L));
        assertThat(ByteUtils.readLongLE(new byte[] { randomByte(), -64, 29, -2, -1, -1, -1, -1, -1 }, 1), is(-123456L));

        assertThat(ByteUtils.readLongBE(new byte[] { 0, 0, 0, 0, 0, 1, -30, 64 }, 0), is(123456L));
        assertThat(ByteUtils.readLongBE(new byte[] { -1, -1, -1, -1, -1, -2, 29, -64 }, 0), is(-123456L));
        assertThat(ByteUtils.readLongBE(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0), is(0L));
        assertThat(ByteUtils.readLongBE(new byte[] { -128, 0, 0, 0, 0, 0, 0, 0 }, 0), is(Long.MIN_VALUE));
        assertThat(ByteUtils.readLongBE(new byte[] { -128, 0, 0, 0, 0, 0, 0, 126 }, 0), is(Long.MIN_VALUE + 127 - 1));
        assertThat(ByteUtils.readLongBE(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 }, 0), is(Long.MAX_VALUE));
        assertThat(ByteUtils.readLongBE(new byte[] { 127, -1, -1, -1, -1, -1, -1, -127, randomByte() }, 0), is(Long.MAX_VALUE - 127 + 1));

        assertThat(ByteUtils.readLongBE(new byte[] { randomByte(), 0, 0, 0, 0, 0, 1, -30, 64 }, 1), is(123456L));
        assertThat(ByteUtils.readLongBE(new byte[] { randomByte(), -1, -1, -1, -1, -1, -2, 29, -64 }, 1), is(-123456L));
    }

    private byte[] readIntLEHelper(int number, int offset) {
        byte[] arr = new byte[4];
        ByteUtils.writeIntLE(number, arr, offset);
        return arr;
    }

    public void testIntToBytes() {
        assertThat(readIntLEHelper(123456, 0), is(new byte[] { 64, -30, 1, 0 }));
        assertThat(readIntLEHelper(-123456, 0), is(new byte[] { -64, 29, -2, -1 }));
        assertThat(readIntLEHelper(0, 0), is(new byte[] { 0, 0, 0, 0 }));
        assertThat(readIntLEHelper(Integer.MAX_VALUE + 1, 0), is(new byte[] { 0, 0, 0, -128 }));
        assertThat(readIntLEHelper(Integer.MAX_VALUE + 127, 0), is(new byte[] { 126, 0, 0, -128 }));
        assertThat(readIntLEHelper(Integer.MIN_VALUE - 1, 0), is(new byte[] { -1, -1, -1, 127 }));
        assertThat(readIntLEHelper(Integer.MIN_VALUE - 127, 0), is(new byte[] { -127, -1, -1, 127 }));
    }

    public void testBytesToInt() {
        assertThat(ByteUtils.readIntLE(new byte[] { 64, -30, 1, 0 }, 0), is(123456));
        assertThat(ByteUtils.readIntLE(new byte[] { -64, 29, -2, -1 }, 0), is(-123456));
        assertThat(ByteUtils.readIntLE(new byte[] { 0, 0, 0, 0 }, 0), is(0));
        assertThat(ByteUtils.readIntLE(new byte[] { 0, 0, 0, -128 }, 0), is(Integer.MIN_VALUE));
        assertThat(ByteUtils.readIntLE(new byte[] { 126, 0, 0, -128 }, 0), is(Integer.MIN_VALUE + 127 - 1));
        assertThat(ByteUtils.readIntLE(new byte[] { -1, -1, -1, 127 }, 0), is(Integer.MAX_VALUE));
        assertThat(ByteUtils.readIntLE(new byte[] { -127, -1, -1, 127, 0 }, 0), is(Integer.MAX_VALUE - 127 + 1));

        assertThat(ByteUtils.readIntLE(new byte[] { 100, 64, -30, 1, 0 }, 1), is(123456));
        assertThat(ByteUtils.readIntLE(new byte[] { -100, -64, 29, -2, -1 }, 1), is(-123456));
    }
}
