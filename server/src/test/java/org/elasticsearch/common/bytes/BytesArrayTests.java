/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.bytes;

import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class BytesArrayTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) {
        return newBytesReference(length, randomInt(length));
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) {
        return newBytesReference(length, 0);
    }

    @Override
    protected BytesReference newBytesReference(byte[] content) {
        return new BytesArray(content);
    }

    private BytesReference newBytesReference(int length, int offset) {
        // randomly add some bytes at the end of the bytes reference
        int tail = randomBoolean() ? 0 : randomIntBetween(0, length + offset);
        final byte[] out = new byte[length + offset + tail];
        for (int i = 0; i < length + offset + tail; i++) {
            out[i] = (byte) random().nextInt(1 << 8);
        }
        BytesArray ref = new BytesArray(out, offset, length);
        assertEquals(length, ref.length());
        assertTrue(ref instanceof BytesArray);
        assertThat(ref.length(), Matchers.equalTo(length));
        return ref;
    }

    public void testArray() {
        int[] sizes = { 0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5)) };

        for (int i = 0; i < sizes.length; i++) {
            BytesArray pbr = (BytesArray) newBytesReference(sizes[i]);
            byte[] array = pbr.array();
            assertNotNull(array);
            assertEquals(sizes[i], pbr.length());
            assertThat(pbr.array().length, greaterThanOrEqualTo(pbr.arrayOffset() + pbr.length()));
            assertSame(array, pbr.array());
        }
    }

    public void testArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesArray pbr = (BytesArray) newBytesReferenceWithOffsetOfZero(length);
        assertEquals(0, pbr.arrayOffset());
    }

    public void testGetIntLE() {
        BytesReference ref = new BytesArray(new byte[] { 0x00, 0x12, 0x10, 0x12, 0x00, 0x01 }, 1, 5);
        assertThat(ref.getIntLE(0), equalTo(0x00121012));
        assertThat(ref.getIntLE(1), equalTo(0x01001210));
        Exception e = expectThrows(ArrayIndexOutOfBoundsException.class, () -> ref.getIntLE(2));
        assertThat(e.getMessage(), equalTo("Index 3 out of bounds for length 3"));
        /*
         * Wait. 3!? The array has length 6. Well, the var handle stuff
         * for arrays just subtracts three - because that's one more than
         * the number of bytes in an int. Get it? I'm not sure I do either....
         */
    }

    public void testGetLongLE() {
        // first 8 bytes = 888, second 8 bytes = Long.MAX_VALUE
        // tag::noformat
        byte[] array = new byte[] {
            0x78, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
            -0x1, -0x1, -0x1, -0x1, -0x1, -0x1, -0x1, 0x7F
        };
        // end::noformat
        BytesReference ref = new BytesArray(array, 0, array.length);
        assertThat(ref.getLongLE(0), equalTo(888L));
        assertThat(ref.getLongLE(8), equalTo(Long.MAX_VALUE));
        Exception e = expectThrows(ArrayIndexOutOfBoundsException.class, () -> ref.getLongLE(9));
        assertThat(e.getMessage(), equalTo("Index 9 out of bounds for length 9"));
    }

    public void testGetDoubleLE() {
        // first 8 bytes = 1.2, second 8 bytes = 1.4
        // tag::noformat
        byte[] array = new byte[] {
            0x33, 0x33, 0x33, 0x33, 0x33, 0x33, -0xD, 0x3F,
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, -0xA, 0x3F
        };
        // end::noformat
        BytesReference ref = new BytesArray(array, 0, array.length);
        assertThat(ref.getDoubleLE(0), equalTo(1.2));
        assertThat(ref.getDoubleLE(8), equalTo(1.4));
        Exception e = expectThrows(ArrayIndexOutOfBoundsException.class, () -> ref.getDoubleLE(9));
        assertThat(e.getMessage(), equalTo("Index 9 out of bounds for length 9"));
    }

}
