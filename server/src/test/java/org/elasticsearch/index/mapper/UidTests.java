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

import java.nio.charset.StandardCharsets;
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

    public void testEncodeDecodeSliceId() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            final String slice = randomSlice();
            final String id = randomId();
            BytesRef encoded = Uid.encodeSliceId(slice, id);
            // The first byte is the length of the slice in UTF-8 bytes.
            assertEquals(slice.getBytes(StandardCharsets.UTF_8).length, encoded.bytes[encoded.offset] & 0xff);
            // The composite uid must round-trip both the plain id and the slice.
            assertEquals(id, Uid.decodeSliceId(encoded));
            assertEquals(slice, Uid.decodeSlice(encoded));
            // The plain encoded id must be a suffix of the composite uid, so engine uniqueness is scoped by (slice, id).
            BytesRef plainId = Uid.encodeId(id);
            assertEquals(1 + slice.getBytes(StandardCharsets.UTF_8).length + plainId.length, encoded.length);
        }
    }

    public void testDecodeSliceIdHonoursOffset() {
        final String slice = randomSlice();
        final String id = randomId();
        BytesRef encoded = Uid.encodeSliceId(slice, id);
        // Copy the composite uid into the middle of a larger buffer to ensure the offset/length based decode is correct.
        final int prefix = randomIntBetween(1, 16);
        byte[] buffer = new byte[prefix + encoded.length + randomIntBetween(0, 16)];
        random().nextBytes(buffer);
        System.arraycopy(encoded.bytes, encoded.offset, buffer, prefix, encoded.length);
        assertEquals(id, Uid.decodeSliceId(buffer, prefix, encoded.length));
        assertEquals(id, Uid.decodeSliceId(new BytesRef(buffer, prefix, encoded.length)));
        assertEquals(slice, Uid.decodeSlice(new BytesRef(buffer, prefix, encoded.length)));
    }

    public void testEncodeDecodeSliceIdWithMultiByteSlice() {
        // A slice value whose UTF-8 encoding uses more than one byte per character must still round-trip exactly.
        final String slice = "sléçe";
        final String id = "the-id";
        BytesRef encoded = Uid.encodeSliceId(slice, id);
        assertEquals(slice.getBytes(StandardCharsets.UTF_8).length, encoded.bytes[encoded.offset] & 0xff);
        assertEquals(slice, Uid.decodeSlice(encoded));
        assertEquals(id, Uid.decodeSliceId(encoded));
    }

    public void testDifferentSlicesProduceDistinctUids() {
        final String id = randomId();
        BytesRef a = Uid.encodeSliceId("slice-a", id);
        BytesRef b = Uid.encodeSliceId("slice-b", id);
        assertNotEquals(a, b);
        // Same plain id is recovered regardless of slice.
        assertEquals(id, Uid.decodeSliceId(a));
        assertEquals(id, Uid.decodeSliceId(b));
    }

    private static String randomSlice() {
        // Non-empty, within the 128-byte limit enforced by Uid#encodeSliceId.
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
