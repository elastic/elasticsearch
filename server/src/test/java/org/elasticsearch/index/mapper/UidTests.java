/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Base64;

public class UidTests extends ESTestCase {

    public static BytesRef[] splitUidIntoTypeAndId(BytesRef uid) {
        int loc = -1;
        final int limit = uid.offset + uid.length;
        for (int i = uid.offset; i < limit; i++) {
            if (uid.bytes[i] == Uid.DELIMITER_BYTE) { // 0x23 is equal to '#'
                loc = i;
                break;
            }
        }

        if (loc == -1) {
            return null;
        }

        int idStart = loc + 1;
        return new BytesRef[] {
            new BytesRef(uid.bytes, uid.offset, loc - uid.offset),
            new BytesRef(uid.bytes, idStart, limit - idStart)
        };
    }

    public void testIsURLBase64WithoutPadding() {
        assertTrue(Uid.isURLBase64WithoutPadding(""));
        assertFalse(Uid.isURLBase64WithoutPadding("a"));
        assertFalse(Uid.isURLBase64WithoutPadding("aa"));
        assertTrue(Uid.isURLBase64WithoutPadding("aw"));
        assertFalse(Uid.isURLBase64WithoutPadding("aaa"));
        assertTrue(Uid.isURLBase64WithoutPadding("aac"));
        assertTrue(Uid.isURLBase64WithoutPadding("aaaa"));
    }

    public void testEncodeUTF8Ids() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            final String id = TestUtil.randomRealisticUnicodeString(random(), 1, 10);
            BytesRef encoded = Uid.encodeId(id);
            assertEquals(id, doDecodeId(encoded));
            assertTrue(encoded.length <= 1 + new BytesRef(id).length);
        }
    }

    public void testEncodeNumericIds() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            String id = Long.toString(TestUtil.nextLong(random(), 0, 1L << randomInt(62)));
            if (randomBoolean()) {
                // prepend a zero to make sure leading zeros are not ignored
                id = "0" + id;
            }
            BytesRef encoded = Uid.encodeId(id);
            assertEquals(id, doDecodeId(encoded));
            assertEquals(1 + (id.length() + 1) / 2, encoded.length);
        }
    }

    public void testEncodeBase64Ids() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            final byte[] binaryId = new byte[TestUtil.nextInt(random(), 1, 10)];
            random().nextBytes(binaryId);
            final String id = Base64.getUrlEncoder().withoutPadding().encodeToString(binaryId);
            BytesRef encoded = Uid.encodeId(id);
            assertEquals(id, doDecodeId(encoded));
            assertTrue(encoded.length <= 1 + binaryId.length);
        }
    }

    private static String doDecodeId(BytesRef encoded) {

        if (randomBoolean()) {
            return Uid.decodeId(Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length));
        } else {
            if (randomBoolean()) {
                BytesRef slicedCopy = new BytesRef(randomIntBetween(encoded.length + 1, encoded.length + 100));
                slicedCopy.offset = randomIntBetween(1, slicedCopy.bytes.length - encoded.length);
                slicedCopy.length = encoded.length;
                System.arraycopy(encoded.bytes, encoded.offset, slicedCopy.bytes, slicedCopy.offset, encoded.length);
                assertArrayEquals(Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length),
                    Arrays.copyOfRange(slicedCopy.bytes, slicedCopy.offset, slicedCopy.offset + slicedCopy.length));
                encoded = slicedCopy;
            }
            return Uid.decodeId(encoded.bytes, encoded.offset, encoded.length);
        }
    }
}
