/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.fieldnames;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.simdjson.SimdJsonTestSupport.toBytes;

/**
 * Tests for {@link FieldNameTable}: hashing, scanAndHash, parent/child merging,
 * cache hit/miss behavior, and edge cases for various key lengths.
 */
public class FieldNameTableTests extends ESTestCase {

    // -- hashName consistency -----------------------------------------------

    public void testHashDeterministic() {
        byte[] buf = toBytes("fieldName");
        int len = "fieldName".length();
        int h1 = FieldNameHash.hashName(buf, 0, len);
        int h2 = FieldNameHash.hashName(buf, 0, len);
        assertEquals(h1, h2);
    }

    public void testHashNeverZero() {
        for (String name : new String[] { "", "a", "ab", "abcd", "abcdefgh", "abcdefghijklmnop", "x".repeat(50) }) {
            byte[] buf = toBytes(name);
            int len = name.getBytes(StandardCharsets.UTF_8).length;
            assertNotEquals("hash must never be 0 (reserved for empty slot)", 0, FieldNameHash.hashName(buf, 0, len));
        }
    }

    public void testHashDistinctForDifferentNames() {
        Set<Integer> hashes = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            String name = "field_" + i;
            byte[] buf = toBytes(name);
            int len = name.getBytes(StandardCharsets.UTF_8).length;
            hashes.add(FieldNameHash.hashName(buf, 0, len));
        }
        assertTrue("expected at least 195 distinct hashes out of 200 names, got " + hashes.size(), hashes.size() >= 195);
    }

    public void testHashWithOffset() {
        byte[] padded = toBytes("XXXXname");
        byte[] plain = toBytes("name");
        assertEquals(FieldNameHash.hashName(plain, 0, 4), FieldNameHash.hashName(padded, 4, 4));
    }

    public void testHashShortKeys() {
        // len 1..8: readSmall path (byte-at-a-time for 1-3, int reads for 4-8)
        for (int len = 1; len <= 8; len++) {
            byte[] buf = new byte[len];
            Arrays.fill(buf, 0, len, (byte) 'x');
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals(0, h);
        }
    }

    public void testHashMediumKeys() {
        // len 9..16: two overlapping readLE8 reads
        for (int len = 9; len <= 16; len++) {
            byte[] buf = new byte[len];
            Arrays.fill(buf, 0, len, (byte) 'y');
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals(0, h);
        }
    }

    public void testHashLongKeys() {
        // len > 16: loop + tail. Uses exact-length buffers to verify no over-read.
        for (int len : new int[] { 17, 32, 48, 100, 255 }) {
            byte[] buf = new byte[len];
            Arrays.fill(buf, 0, len, (byte) 'z');
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals(0, h);
        }
    }

    public void testHashLongKeysTailBoundary() {
        // After the loop, rem = len % 16 (for len > 16). Test every remainder value 1..16.
        // rem <= 8 uses readSmall; rem > 8 uses two overlapping readLE8 reads.
        for (int rem = 1; rem <= 16; rem++) {
            int len = 16 + rem;
            byte[] buf = new byte[len];
            for (int i = 0; i < len; i++) {
                buf[i] = (byte) ('a' + (i % 26));
            }
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals("hash must not be 0 for rem=" + rem, 0, h);
        }
    }

    public void testHashLongKeysTailConsistentWithExactBuffer() {
        // Verify that hashName on an exact-length buffer and a larger buffer produce the same hash.
        for (int rem = 1; rem <= 16; rem++) {
            int len = 16 + rem;
            byte[] exact = new byte[len];
            byte[] padded = new byte[len + 64];
            for (int i = 0; i < len; i++) {
                exact[i] = (byte) ('a' + (i % 26));
                padded[i] = exact[i];
            }
            assertEquals(
                "hash must be the same regardless of buffer size for rem=" + rem,
                FieldNameHash.hashName(exact, 0, len),
                FieldNameHash.hashName(padded, 0, len)
            );
        }
    }

    public void testHashDistinguishesTailBytes() {
        // Two names that differ only in the tail region (rem <= 8) must hash differently.
        byte[] a = "0123456789abcdefX".getBytes(StandardCharsets.UTF_8);
        byte[] b = "0123456789abcdefY".getBytes(StandardCharsets.UTF_8);
        assertEquals(17, a.length);
        assertNotEquals(
            "last-byte difference in rem=1 tail must produce different hashes",
            FieldNameHash.hashName(a, 0, a.length),
            FieldNameHash.hashName(b, 0, b.length)
        );
    }

    public void testHashSmallReadConsistency() {
        // Verify readSmall for len 1-3 (individual byte reads) produces consistent values
        // regardless of surrounding buffer content.
        for (int len = 1; len <= 3; len++) {
            byte[] clean = new byte[len];
            byte[] noisy = new byte[len + 32];
            Arrays.fill(noisy, (byte) 0xFF);
            for (int i = 0; i < len; i++) {
                clean[i] = (byte) ('a' + i);
                noisy[i] = clean[i];
            }
            assertEquals(
                "readSmall must ignore bytes past len for len=" + len,
                FieldNameHash.hashName(clean, 0, len),
                FieldNameHash.hashName(noisy, 0, len)
            );
        }
    }

    public void testHashSmallReadForLen4To8() {
        // Verify readSmall for len 4-8 (native-order int reads) is consistent
        for (int len = 4; len <= 8; len++) {
            byte[] exact = new byte[len];
            byte[] larger = new byte[len + 32];
            Arrays.fill(larger, (byte) 0xFF);
            for (int i = 0; i < len; i++) {
                exact[i] = (byte) (i + 1);
                larger[i] = exact[i];
            }
            assertEquals(
                "hash must be same regardless of trailing content for len=" + len,
                FieldNameHash.hashName(exact, 0, len),
                FieldNameHash.hashName(larger, 0, len)
            );
        }
    }

    public void testHashEmptyKey() {
        byte[] buf = new byte[0];
        int h = FieldNameHash.hashName(buf, 0, 0);
        assertNotEquals(0, h);
    }

    // -- hashWord (fused scan+hash from pre-loaded word) --------------------

    public void testHashWordMatchesHashNameForAllLengths0To8() {
        byte[] name = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        // Build an 8-byte word as LONG_LE would read it: name bytes in little-endian order
        long word = 0;
        for (int i = 0; i < 8; i++) {
            word |= (long) (name[i] & 0xFF) << (i * 8);
        }
        for (int len = 0; len <= 8; len++) {
            int expected = FieldNameHash.hashName(name, 0, len);
            int actual = FieldNameHash.hashWord(word, len);
            assertEquals("hashWord must match hashName for len=" + len, expected, actual);
        }
    }

    public void testHashWordWithRandomBytes() {
        for (int iter = 0; iter < 100; iter++) {
            byte[] name = new byte[8];
            for (int i = 0; i < 8; i++) {
                name[i] = (byte) randomIntBetween(1, 127);
            }
            long word = 0;
            for (int i = 0; i < 8; i++) {
                word |= (long) (name[i] & 0xFF) << (i * 8);
            }
            int len = randomIntBetween(0, 8);
            int expected = FieldNameHash.hashName(name, 0, len);
            int actual = FieldNameHash.hashWord(word, len);
            assertEquals("hashWord must match hashName for random input, len=" + len, expected, actual);
        }
    }

    public void testMaskWordProducesCorrectPrefix8() {
        byte[] name = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        long word = 0;
        for (int i = 0; i < 8; i++) {
            word |= (long) (name[i] & 0xFF) << (i * 8);
        }
        for (int len = 0; len <= 8; len++) {
            long expected = FrozenFieldNameTable.readPrefix8(name, 0, len);
            long actual = FieldNameHash.maskWord(word, len);
            assertEquals("maskWord must match readPrefix8 for len=" + len, expected, actual);
        }
    }

    // -- scanAndHash --------------------------------------------------------

    /**
     * scanAndHash operates on real JSON buffers where the field name sits after an opening quote
     * and is followed by a closing quote.
     */
    public void testScanAndHashSimpleField() {
        byte[] buf = makeScanBuffer("hello");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals("scanAndHash should not return -1 for a simple field", -1L, result);
        int len = (int) (result & 0xFFFFFFFFL);
        int hash = (int) (result >>> 32);
        assertEquals(5, len);
        assertEquals(FieldNameHash.hashName(buf, 0, 5), hash);
    }

    public void testScanAndHashReturnsMinusOneForBackslash() {
        byte[] buf = makeScanBufferRaw("hel\\lo\"");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertEquals(-1L, result);
    }

    public void testScanAndHashBackslashBeforeQuote() {
        byte[] buf = makeScanBufferRaw("abc\\\"def\"");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertEquals("backslash appears before quote, should return -1", -1L, result);
    }

    public void testScanAndHashEmptyFieldName() {
        byte[] buf = makeScanBuffer("");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        int len = (int) (result & 0xFFFFFFFFL);
        assertEquals(0, len);
    }

    public void testScanAndHashLongFieldName() {
        String name = "a_very_long_field_name_that_spans_multiple_eight_byte_words";
        byte[] buf = makeScanBuffer(name);
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        int len = (int) (result & 0xFFFFFFFFL);
        int hash = (int) (result >>> 32);
        assertEquals(name.length(), len);
        assertEquals(FieldNameHash.hashName(buf, 0, name.length()), hash);
    }

    public void testScanAndHashConsistentWithHashName() {
        for (String name : new String[] { "a", "ab", "abc", "abcd", "abcde", "abcdefgh", "twelve_bytes", "sixteen_bytes_xx" }) {
            byte[] buf = makeScanBuffer(name);
            long result = FieldNameHash.scanAndHash(buf, 0);
            assertNotEquals("scanAndHash failed for '" + name + "'", -1L, result);
            int len = (int) (result & 0xFFFFFFFFL);
            int hash = (int) (result >>> 32);
            assertEquals("length mismatch for '" + name + "'", name.length(), len);
            assertEquals("hash mismatch for '" + name + "'", FieldNameHash.hashName(buf, 0, len), hash);
        }
    }

    public void testScanAndHashScalarTailSweep() {
        // For each length 1..15, the closing quote lands at a different position relative
        // to the 8-byte word boundary. Lengths <= 7 always hit the scalar tail on an
        // exact-length buffer; lengths 8+ may hit the SIMD path or the scalar tail
        // depending on buffer size.
        for (int nameLen = 1; nameLen <= 15; nameLen++) {
            String name = "x".repeat(nameLen);
            byte[] buf = makeScanBuffer(name);
            long result = FieldNameHash.scanAndHash(buf, 0);
            assertNotEquals("failed for len=" + nameLen, -1L, result);
            int len = (int) (result & 0xFFFFFFFFL);
            int hash = (int) (result >>> 32);
            assertEquals("length for len=" + nameLen, nameLen, len);
            assertEquals("hash for len=" + nameLen, FieldNameHash.hashName(buf, 0, nameLen), hash);
        }
    }

    public void testScanAndHashBackslashInScalarTail() {
        // Place a backslash where it will be found by the scalar tail (name len < 7)
        byte[] buf = makeScanBufferRaw("ab\\c\"");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertEquals(-1L, result);
    }

    public void testScanAndHashQuoteAt8ByteBoundary() {
        // Name of exactly 7 chars: quote lands at position 7 (last byte of first 8-byte word).
        // With an exact-length buffer this is within the SIMD loop; verify correctness.
        String name = "abcdefg";
        byte[] buf = makeScanBuffer(name);
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        assertEquals(7, (int) (result & 0xFFFFFFFFL));
    }

    public void testScanAndHashQuoteJustPast8ByteBoundary() {
        // Name of exactly 8 chars: quote at position 8, second word needed.
        // On an exact-length buffer (9 bytes: 8 + quote), the second word read
        // exceeds bounds, so the scalar tail handles it.
        String name = "abcdefgh";
        byte[] buf = makeScanBuffer(name);
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        assertEquals(8, (int) (result & 0xFFFFFFFFL));
    }

    public void testScanAndHashMatchesHashNameForAllLengthsUpTo40() {
        // Comprehensive sweep: every length from 0 to 40, with distinct byte content.
        for (int nameLen = 0; nameLen <= 40; nameLen++) {
            byte[] nameBytes = new byte[nameLen];
            for (int i = 0; i < nameLen; i++) {
                nameBytes[i] = (byte) ('a' + (i % 26));
            }
            String name = new String(nameBytes, StandardCharsets.US_ASCII);
            byte[] buf = makeScanBuffer(name);
            long result = FieldNameHash.scanAndHash(buf, 0);
            assertNotEquals("failed for len=" + nameLen, -1L, result);
            int len = (int) (result & 0xFFFFFFFFL);
            int hash = (int) (result >>> 32);
            assertEquals("length for len=" + nameLen, nameLen, len);
            assertEquals("hash for len=" + nameLen, FieldNameHash.hashName(buf, 0, nameLen), hash);
        }
    }

    // -- Child: lookup, cache hit/miss --------------------------------------

    public void testChildLookupCacheHit() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        byte[] buf = "myfield".getBytes(StandardCharsets.UTF_8);
        String first = child.lookupName(buf, 0, buf.length);
        assertEquals("myfield", first);

        String second = child.lookupName(buf, 0, buf.length);
        assertSame("cache hit should return same String instance", first, second);
    }

    public void testChildLookupWithOffset() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        byte[] buf = "____myfield".getBytes(StandardCharsets.UTF_8);
        String name = child.lookupName(buf, 4, 7);
        assertEquals("myfield", name);

        String again = child.lookupName(buf, 4, 7);
        assertSame(name, again);
    }

    public void testChildLookupManyFields() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        String[] expected = new String[200];
        for (int i = 0; i < 200; i++) {
            byte[] buf = ("field_" + i).getBytes(StandardCharsets.UTF_8);
            expected[i] = child.lookupName(buf, 0, buf.length);
        }

        for (int i = 0; i < 200; i++) {
            byte[] buf = ("field_" + i).getBytes(StandardCharsets.UTF_8);
            String result = child.lookupName(buf, 0, buf.length);
            assertSame("cache hit expected for field_" + i, expected[i], result);
        }
    }

    public void testChildLookupLongKeyBeyondInlineThreshold() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        String longName = "a_long_field_name_exceeding_sixteen_bytes_for_sure";
        byte[] raw = longName.getBytes(StandardCharsets.UTF_8);
        assertTrue(raw.length > FieldNameTable.MAX_INLINE_BYTES);
        byte[] buf = toBytes(longName);

        String first = child.lookupName(buf, 0, raw.length);
        assertEquals(longName, first);

        String second = child.lookupName(buf, 0, raw.length);
        assertSame(first, second);
    }

    // -- Parent/child merge -------------------------------------------------

    public void testParentChildMerge() {
        FieldNameTable root = new FieldNameTable();

        FieldNameTable.Child child1 = root.makeChild();
        byte[] buf = "shared_field".getBytes(StandardCharsets.UTF_8);
        String fromChild1 = child1.lookupName(buf, 0, buf.length);
        child1.release();

        FieldNameTable.Child child2 = root.makeChild();
        String fromChild2 = child2.lookupName(buf, 0, buf.length);
        assertEquals(fromChild1, fromChild2);
        child2.release();
    }

    public void testMergeFromMultipleChildren() {
        FieldNameTable root = new FieldNameTable();

        FieldNameTable.Child child1 = root.makeChild();
        byte[] buf1 = "alpha".getBytes(StandardCharsets.UTF_8);
        child1.lookupName(buf1, 0, buf1.length);
        child1.release();

        FieldNameTable.Child child2 = root.makeChild();
        byte[] buf2 = "beta".getBytes(StandardCharsets.UTF_8);
        child2.lookupName(buf2, 0, buf2.length);
        // child2 should have inherited "alpha" from parent
        String alphaFromChild2 = child2.lookupName(buf1, 0, buf1.length);
        assertEquals("alpha", alphaFromChild2);
        child2.release();

        FieldNameTable.Child child3 = root.makeChild();
        assertEquals("alpha", child3.lookupName(buf1, 0, buf1.length));
        assertEquals("beta", child3.lookupName(buf2, 0, buf2.length));
        child3.release();
    }

    public void testChildReuseAfterRelease() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        byte[] buf = "reusable".getBytes(StandardCharsets.UTF_8);
        String first = child.lookupName(buf, 0, buf.length);
        child.release();

        String afterRelease = child.lookupName(buf, 0, buf.length);
        assertEquals(first, afterRelease);
    }

    public void testNoDirtyNoMerge() {
        FieldNameTable root = new FieldNameTable();

        FieldNameTable.Child child1 = root.makeChild();
        byte[] buf = "field".getBytes(StandardCharsets.UTF_8);
        child1.lookupName(buf, 0, buf.length);
        child1.release();

        FieldNameTable.Child child2 = root.makeChild();
        child2.lookupName(buf, 0, buf.length);
        assertFalse("no new names added, child should not be dirty", child2.dirty);
        child2.release();
    }

    // -- Inline vs external key storage boundary ----------------------------

    public void testInlineBoundaryExactly16Bytes() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        byte[] exact16 = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        assertEquals(16, exact16.length);

        String first = child.lookupName(exact16, 0, exact16.length);
        String second = child.lookupName(exact16, 0, exact16.length);
        assertSame(first, second);
    }

    public void testExternalKeyJustOver16Bytes() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        String name = "0123456789abcdefg";
        byte[] buf = toBytes(name);
        int len = name.length();
        assertEquals(17, len);

        String first = child.lookupName(buf, 0, len);
        String second = child.lookupName(buf, 0, len);
        assertSame(first, second);
    }

    // -- Tail byte matching (1, 2, 3 byte tails) ---------------------------

    public void testInlineKeyTailLengths() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        for (int len = 1; len <= FieldNameTable.MAX_INLINE_BYTES; len++) {
            byte[] buf = new byte[len];
            for (int j = 0; j < len; j++) {
                buf[j] = (byte) ('a' + (j % 26));
            }
            String name = new String(buf, StandardCharsets.UTF_8);
            String first = child.lookupName(buf, 0, len);
            assertEquals("len=" + len, name, first);
            String second = child.lookupName(buf, 0, len);
            assertSame("cache hit expected for len=" + len, first, second);
        }
    }

    // -- Non-dirty child picks up concurrent parent updates ------------------

    public void testNonDirtyChildRefreshesFromParent() {
        FieldNameTable root = new FieldNameTable();

        FieldNameTable.Child child1 = root.makeChild();
        byte[] buf = "newfield".getBytes(StandardCharsets.UTF_8);
        child1.lookupName(buf, 0, buf.length);
        child1.release(); // merges "newfield" into parent

        FieldNameTable.Child child2 = root.makeChild();
        // child2 starts with "newfield" from parent
        String found = child2.lookupName(buf, 0, buf.length);
        assertEquals("newfield", found);
        assertFalse("no new names added, should not be dirty", child2.dirty);

        // Now simulate another thread merging a new name while child2 is alive
        FieldNameTable.Child child3 = root.makeChild();
        byte[] buf2 = "othername".getBytes(StandardCharsets.UTF_8);
        child3.lookupName(buf2, 0, buf2.length);
        child3.release(); // merges "othername" into parent

        // child2 releases without being dirty — should refresh from parent
        child2.release();

        // After refresh, child2 should now have "othername"
        String other = child2.lookupName(buf2, 0, buf2.length);
        assertEquals("othername", other);
        // Since the name was already in the refreshed snapshot, the child should not be dirty
        assertFalse("name was inherited from parent, should not be dirty", child2.dirty);
    }

    // -- Linear probing collision testing ------------------------------------

    public void testHashCollisionResolution() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        // Insert many names into the same child to increase collision probability.
        String[] names = new String[500];
        for (int i = 0; i < names.length; i++) {
            names[i] = "collision_test_field_" + i;
        }
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.getBytes(StandardCharsets.UTF_8).length;
            child.lookupName(buf, 0, len);
        }

        // Verify all can be retrieved
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.getBytes(StandardCharsets.UTF_8).length;
            String result = child.lookupName(buf, 0, len);
            assertEquals(name, result);
        }
    }

    // -- Capacity limit: beyond MAX_COUNT no new names are cached -----------

    public void testBeyondMaxCountNewNamesNotCached() {
        FieldNameTable root = new FieldNameTable();
        FieldNameTable.Child child = root.makeChild();

        // Fill to MAX_COUNT.
        for (int i = 0; i < FieldNameTable.MAX_COUNT; i++) {
            String name = "fill_" + i;
            byte[] buf = toBytes(name);
            int len = name.getBytes(StandardCharsets.UTF_8).length;
            child.lookupName(buf, 0, len);
        }
        assertEquals(FieldNameTable.MAX_COUNT, child.count);

        // One more should still return the correct name but not increase count
        byte[] extra = toBytes("overflow_name");
        int extraLen = "overflow_name".length();
        String result = child.lookupName(extra, 0, extraLen);
        assertEquals("overflow_name", result);
        assertEquals("count should not increase past MAX_COUNT", FieldNameTable.MAX_COUNT, child.count);

        // Looking it up again should still work (creates a new String each time since not cached)
        String result2 = child.lookupName(extra, 0, extraLen);
        assertEquals("overflow_name", result2);
    }

    // -- Helpers ----

    /**
     * Creates a buffer for scanAndHash: the field name followed by a closing quote.
     * No trailing padding — the scalar tail in scanAndHash handles buffers of any size.
     */
    private static byte[] makeScanBuffer(String fieldName) {
        return makeScanBufferRaw(fieldName + "\"");
    }

    /**
     * Creates a buffer for scanAndHash from raw content (caller includes the closing quote
     * and any escape sequences). Uses exact length to verify no over-reads.
     */
    private static byte[] makeScanBufferRaw(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }
}
