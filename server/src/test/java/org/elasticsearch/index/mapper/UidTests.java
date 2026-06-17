/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
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
        return new BytesRef[] { new BytesRef(uid.bytes, uid.offset, loc - uid.offset), new BytesRef(uid.bytes, idStart, limit - idStart) };
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

    public void testCompositeIdRoundTrip() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            final String slice = randomSlice();
            final String id = randomId();
            String composite = Uid.compositeId(slice, id);
            // The composite string is slice + "#" + id, and the plain id is recovered by splitting on the first '#'.
            assertEquals(slice + "#" + id, composite);
            assertEquals(id, Uid.idFromCompositeId(composite));
            // The composite is encoded/decoded with the standard id pipeline, so it round-trips through encodeId/decodeId.
            assertEquals(composite, Uid.decodeId(Uid.encodeId(composite)));
            assertEquals(id, Uid.idFromCompositeId(Uid.decodeId(Uid.encodeId(composite))));
        }
    }

    public void testIdFromCompositeIdSplitsOnFirstSeparator() {
        // The slice cannot contain '#', but the id may. The split must happen on the FIRST '#'.
        assertEquals("a#b", Uid.idFromCompositeId(Uid.compositeId("slice-1", "a#b")));
        assertEquals("a#b#c", Uid.idFromCompositeId(Uid.compositeId("s", "a#b#c")));
        assertEquals("", Uid.idFromCompositeId("slice#"));
    }

    public void testDifferentSlicesProduceDistinctTerms() {
        final String id = randomId();
        BytesRef a = Uid.encodeId(Uid.compositeId("slice-a", id));
        BytesRef b = Uid.encodeId(Uid.compositeId("slice-b", id));
        assertNotEquals(a, b);
        // Same plain id is recovered regardless of slice.
        assertEquals(id, Uid.idFromCompositeId(Uid.decodeId(a)));
        assertEquals(id, Uid.idFromCompositeId(Uid.decodeId(b)));
    }

    private static String randomSlice() {
        return randomAlphaOfLengthBetween(1, 32);
    }

    private static String randomId() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> Long.toString(randomNonNegativeLong()); // numeric encoding
            case 1 -> Base64.getUrlEncoder().withoutPadding().encodeToString(randomByteArrayOfLength(randomIntBetween(1, 12))); // base64
            case 2 -> randomAlphaOfLengthBetween(1, 16); // utf8 encoding
            default -> throw new AssertionError("unreachable");
        };
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
                assertArrayEquals(
                    Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length),
                    Arrays.copyOfRange(slicedCopy.bytes, slicedCopy.offset, slicedCopy.offset + slicedCopy.length)
                );
                encoded = slicedCopy;
            }
            return Uid.decodeId(encoded.bytes, encoded.offset, encoded.length);
        }
    }
}
