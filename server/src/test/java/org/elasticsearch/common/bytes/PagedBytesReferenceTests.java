/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteArray;
import org.hamcrest.Matchers;

import java.io.IOException;

public class PagedBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        ByteArray byteArray = bigarrays.newByteArray(length);
        for (int i = 0; i < length; i++) {
            byteArray.set(i, (byte) random().nextInt(1 << 8));
        }
        assertThat(byteArray.size(), Matchers.equalTo((long) length));
        BytesReference ref = BytesReference.fromByteArray(byteArray, length);
        assertThat(ref.length(), Matchers.equalTo(length));
        if (byteArray.hasArray()) {
            assertThat(ref, Matchers.instanceOf(BytesArray.class));
        } else {
            assertThat(ref, Matchers.instanceOf(PagedBytesReference.class));
        }
        return ref;
    }

    public void testToBytesRefMaterializedPages() throws IOException {
        // we need a length != (n * pagesize) to avoid page sharing at boundaries
        int length = 0;
        while ((length % PAGE_SIZE) == 0) {
            length = randomIntBetween(PAGE_SIZE, PAGE_SIZE * randomIntBetween(2, 5));
        }
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = new BytesArray(pbr.toBytesRef());
        BytesArray ba2 = new BytesArray(pbr.toBytesRef());
        assertNotNull(ba);
        assertNotNull(ba2);
        assertEquals(pbr.length(), ba.length());
        assertEquals(ba.length(), ba2.length());
        // ensure no single-page optimization
        assertNotSame(ba.array(), ba2.array());
    }

    public void testSinglePage() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            // verify that array() is cheap for small payloads
            if (sizes[i] <= PAGE_SIZE) {
                BytesRef page = getSinglePageOrNull(pbr);
                assertNotNull(page);
                byte[] array = page.bytes;
                assertNotNull(array);
                assertEquals(sizes[i], array.length);
                assertSame(array, page.bytes);
            } else {
                BytesRef page = getSinglePageOrNull(pbr);
                if (pbr.length() > 0) {
                    assertNull(page);
                }
            }
        }
    }

    public void testToBytes() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            byte[] bytes = BytesReference.toBytes(pbr);
            assertEquals(sizes[i], bytes.length);
            // verify that toBytes() is cheap for small payloads
            if (sizes[i] <= PAGE_SIZE) {
                assertSame(bytes, BytesReference.toBytes(pbr));
            } else {
                assertNotSame(bytes, BytesReference.toBytes(pbr));
            }
        }
    }

    public void testHasSinglePage() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(1, 3));
        BytesReference pbr = newBytesReference(length);
        // must return true for <= pagesize
        assertEquals(length <= PAGE_SIZE, getNumPages(pbr) == 1);
    }

    public void testEquals() {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        ByteArray ba1 = bigarrays.newByteArray(length, false);
        ByteArray ba2 = bigarrays.newByteArray(length, false);

        // copy contents
        for (long i = 0; i < length; i++) {
            ba2.set(i, ba1.get(i));
        }

        // get refs & compare
        BytesReference pbr = BytesReference.fromByteArray(ba1, length);
        BytesReference pbr2 = BytesReference.fromByteArray(ba2, length);
        assertEquals(pbr, pbr2);
        int offsetToFlip = randomIntBetween(0, length - 1);
        int value = ~Byte.toUnsignedInt(ba1.get(offsetToFlip));
        ba2.set(offsetToFlip, (byte)value);
        assertNotEquals(pbr, pbr2);
    }

}
