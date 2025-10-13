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
        int length = randomIntBetween(0, maxSize);
        stream.writeBytes(data, 0, length);
        assertEquals(length, stream.size());
        for (int i = 0; i < length; i++) {
            assertEquals(data[i], stream.bytes().get(i));
        }
    }

    public void testWriteBytesWithOffset() {
        int maxSize = randomIntBetween(10, 100);
        byte[] data = randomByteArrayOfLength(maxSize);
        int offset = randomIntBetween(0, maxSize);
        int length = randomIntBetween(0, maxSize - offset);
        stream.writeBytes(data, offset, length);
        assertEquals(length, stream.size());
        for (int i = 0; i < stream.size(); i++) {
            assertEquals(stream.bytes().get(i), data[i + offset]);
        }
    }

    public void testWriteBytesWithLengthGreaterThanData() {
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        int size = randomIntBetween(0, 100);
        assertThrows(IndexOutOfBoundsException.class, () -> s.writeBytes(randomByteArrayOfLength(size), 0, size + 1));
    }

    public void testResetShrinksAndResetsCount() {
        int byteArrayLength = randomIntBetween(0, 100);
        stream.writeBytes(randomByteArrayOfLength(byteArrayLength), 0, byteArrayLength);
        assertEquals(byteArrayLength, stream.size());
        stream.reset();
        assertEquals(0, stream.size());
        // After reset, should be able to write again
        byte b = randomByte();
        stream.writeByte(b);
        assertEquals(1, stream.size());
        assertEquals(b, stream.bytes().get(0));
    }

    public void testSeek() {
        // Do some writing
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        int byteArrayLength = randomIntBetween(0, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        s.writeBytes(byteArray, 0, byteArrayLength);

        // Seek either over what we've already written, or what we have yet to write
        int seekPosition = randomIntBetween(0, 200);
        s.seek(seekPosition);
        assertEquals(seekPosition, s.size());
    }

    public void testSeekAtBoundaries() {
        // Do some arbitrary writing
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        int byteArrayLength = randomIntBetween(0, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        s.writeBytes(byteArray, 0, byteArrayLength);

        s.seek(0);
        assertEquals(0, s.size());
        s.seek(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, s.size());
    }

    public void testSeekWithOverflow() {
        // Do some arbitrary writing and then overflow
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        int byteArrayLength = randomIntBetween(0, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        s.writeBytes(byteArray, 0, byteArrayLength);
        assertThrows(
            IllegalArgumentException.class,
            () -> s.seek(randomLongBetween((long) Integer.MAX_VALUE + 1, (long) Integer.MAX_VALUE + 100))
        );
    }

    public void testSkip() {
        // Do some arbitrary writing
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        int byteArrayLength = randomIntBetween(0, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        s.writeBytes(byteArray, 0, byteArrayLength);

        int skipValue = randomIntBetween(0, 100);
        s.skip(skipValue);
        assertEquals(byteArrayLength + skipValue, s.size());
    }

    public void testSkipAtBoundaries() {
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        s.skip(0);
        assertEquals(0, s.size());
        s.skip(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, s.size());
    }

    public void testSkipWithOverflow() {
        // Do some arbitrary writing and then overflow
        BytesStreamOutput s = new BytesStreamOutput(0, BigArrays.NON_RECYCLING_INSTANCE);
        int byteArrayLength = randomIntBetween(1, 100);
        byte[] byteArray = randomByteArrayOfLength(byteArrayLength);
        s.writeBytes(byteArray, 0, byteArrayLength);

        int skipPosition = randomIntBetween(Integer.MAX_VALUE - byteArrayLength + 1, Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, () -> s.skip(skipPosition));
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
