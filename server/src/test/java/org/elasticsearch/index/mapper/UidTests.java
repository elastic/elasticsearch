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

    public void testCompoundIdRoundTrip() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            final String slice = randomSlice();
            final String id = randomId();
            BytesRef compound = Uid.encodeCompoundId(id, slice);
            BytesRef search = Uid.searchTerm(id);
            // The plain id and the slice round-trip from the compound term...
            assertEquals(id, Uid.decodeCompoundId(compound));
            assertEquals(slice, Uid.sliceFromCompoundId(compound));
            // ...and the plain id round-trips from the search term (empty-slice member of the same format).
            assertEquals(id, Uid.decodeCompoundId(search));
            assertEquals("", Uid.sliceFromCompoundId(search));
            // The plain encoded id is the prefix shared by both terms; the trailing byte holds the slice length.
            BytesRef plain = Uid.encodeId(id);
            assertEquals(plain.length + slice.getBytes(StandardCharsets.UTF_8).length + 1, compound.length);
            assertEquals(plain.length + 1, search.length);
        }
    }

    public void testSearchAndCompoundTermSpacesAreDisjoint() {
        // The hazard a bare search term would have: the WHOLE compound encodeId("12")++"34"++[2] is byte-identical to
        // bare encodeId("12333402") (numeric ids pack two digits/byte). So a bare-id search term could land on an
        // identity term. Tagging the search term with a trailing 0x00 makes it longer and disjoint.
        assertEquals(Uid.encodeId("12333402"), Uid.encodeCompoundId("12", "34"));
        assertNotEquals(Uid.searchTerm("12333402"), Uid.encodeCompoundId("12", "34"));

        for (int iter = 0; iter < 5000; ++iter) {
            String id1 = randomId();
            String id2 = randomId();
            String slice = randomSlice();
            // A search term can never byte-equal any compound term: last byte is 0x00 vs the slice length (>= 1).
            assertNotEquals(Uid.searchTerm(id1), Uid.encodeCompoundId(id2, slice));
        }
    }

    public void testCompoundSplitsOnTrailingLengthForAnyId() {
        // ids may contain '#' or other bytes; the trailing length byte makes the (id, slice) split unambiguous.
        for (String id : new String[] { "a#b", "a#b#c", "with space", "0", "00" }) {
            BytesRef compound = Uid.encodeCompoundId(id, "the-slice");
            assertEquals(id, Uid.decodeCompoundId(compound));
            assertEquals("the-slice", Uid.sliceFromCompoundId(compound));
        }
    }

    public void testSameIdDifferentSlicesAreDistinctCompoundTerms() {
        final String id = randomId();
        BytesRef a = Uid.encodeCompoundId(id, "slice-a");
        BytesRef b = Uid.encodeCompoundId(id, "slice-b");
        assertNotEquals(a, b);
        assertEquals(id, Uid.decodeCompoundId(a));
        assertEquals(id, Uid.decodeCompoundId(b));
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
