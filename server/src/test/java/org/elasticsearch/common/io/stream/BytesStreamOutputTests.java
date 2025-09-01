/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public class BytesStreamOutputTests extends ESTestCase {

    private BytesStreamOutput stream;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        stream = new BytesStreamOutput();
    }

    public void testDefaultConstructor() {
        assertEquals(0, stream.size());
        assertEquals(0, stream.position());
        assertEquals(BytesArray.EMPTY, stream.bytes());
    }

    public void testConstructorWithExpectedSize() {
        BytesStreamOutput s = new BytesStreamOutput(randomNonNegativeInt());
        assertEquals(0, s.size());
        assertEquals(0, s.position());
    }

    public void testConstructorWithMaximumSize() {
        int maximumSize = randomIntBetween(0, Integer.MAX_VALUE);
        BytesStreamOutput s = new BytesStreamOutput(randomNonNegativeInt(), BigArrays.NON_RECYCLING_INSTANCE, maximumSize);
        assertEquals(0, s.size());
        assertEquals(0, s.position());
        assertEquals(maximumSize, s.maximumSize);
    }

    public void testWriteByte() {
        byte i1 = randomByte();
        byte i2 = randomByte();
        stream.writeByte(i1);
        stream.writeByte(i2);
        assertEquals(2, stream.size());
        assertEquals(2, stream.position());
        assertEquals(i1, stream.bytes().get(0));
        assertEquals(i2, stream.bytes().get(1));
    }

    public void testWriteBytes() {
        int maxSize = randomIntBetween(10, 100);
        byte[] data = randomByteArrayOfLength(maxSize);
        stream.writeBytes(data, 0, data.length);
        assertEquals(data.length, stream.size());
        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], stream.bytes().get(i));
        }
    }

    public void testWriteBytesWithOffset() {
        int maxSize = randomIntBetween(10, 100);
        byte[] data = randomByteArrayOfLength(maxSize);
        int offset = randomIntBetween(0, maxSize / 2);
        int length = randomIntBetween(0, maxSize / 2);
        stream.writeBytes(data, offset, length); // should write 30, 40
        assertEquals(length, stream.size());
        for (int i = 0; i < stream.size(); i++) {
            assertEquals(stream.bytes().get(i), data[i + offset]);
        }
    }

    public void testHasCapacityWithPositiveLength() {
        int i1 = randomIntBetween(1, 100);
        int i2 = randomIntBetween(1, 100);
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE, i1 + i2);
        assertTrue(s.hasCapacity(i1));
        s.writeBytes(randomByteArrayOfLength(i1), 0, i1);
        assertTrue(s.hasCapacity(i2));
        s.writeBytes(randomByteArrayOfLength(i2), 0, i2);
        assertFalse(s.hasCapacity(1));
    }

    public void testHasCapacityWithNegativeLength() {
        assertThrows(IllegalArgumentException.class, () -> stream.hasCapacity(randomNegativeInt()));
    }

    public void testCapacityExceededThrows() {
        int maximumSize = randomIntBetween(0, 100);
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE, maximumSize);
        assertThrows(IllegalArgumentException.class, () -> s.writeBytes(randomByteArrayOfLength(maximumSize + 1), 0, maximumSize + 1));
    }

    public void testResetShrinksAndResetsCount() {
        int byteArrayLength = randomIntBetween(0, 100);
        stream.writeBytes(randomByteArrayOfLength(byteArrayLength), 0, byteArrayLength);
        stream.reset();
        assertEquals(0, stream.size());
        // After reset, should be able to write again
        byte b = randomByte();
        stream.writeByte(b);
        assertEquals(1, stream.size());
        assertEquals(b, stream.bytes().get(0));
    }

    public void testSeekAndSkip() {
        int byteArrayLength = randomIntBetween(0, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        stream.writeBytes(byteArray, 0, byteArrayLength);

        int seekPosition = randomIntBetween(byteArrayLength, byteArrayLength + 100);
        stream.seek(seekPosition);
        assertEquals(seekPosition, stream.size());

        int skipLength = randomIntBetween(0, 100);
        stream.skip(skipLength);
        assertEquals(seekPosition + skipLength, stream.size());
    }

    public void testCopyBytes() {
        int byteArrayLength = randomIntBetween(0, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        stream.writeBytes(byteArray, 0, byteArrayLength);
        BytesReference copy = stream.copyBytes();
        assertEquals(byteArrayLength, copy.length());
        for (int i = 0; i < byteArrayLength; i++) {
            assertEquals(byteArray[i], copy.get(i));
        }
    }

    public void testFlushAndCloseNoop() {
        stream.writeByte(randomByte());
        stream.flush(); // should do nothing
        stream.close(); // should do nothing
        assertEquals(1, stream.size());
    }
}
