/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

// Unit tests for FieldNameHash: wyhash consistency, hashWord/maskWord, and fused scanAndHash.
public class FieldNameHashTests extends ESTestCase {

    // ---- hashName consistency ----

    // Same bytes must always produce the same hash.
    public void testHashDeterministic() {
        byte[] buf = "fieldName".getBytes(UTF_8);
        int len = "fieldName".length();
        int h1 = FieldNameHash.hashName(buf, 0, len);
        int h2 = FieldNameHash.hashName(buf, 0, len);
        assertEquals(h1, h2);
    }

    // Zero is reserved for empty hash-table slots — hashName must never return it.
    public void testHashNeverZero() {
        for (String name : new String[] { "", "a", "ab", "abcd", "abcdefgh", "abcdefghijklmnop", "x".repeat(50) }) {
            byte[] buf = name.getBytes(UTF_8);
            int len = name.getBytes(UTF_8).length;
            assertNotEquals("hash must never be 0 (reserved for empty slot)", 0, FieldNameHash.hashName(buf, 0, len));
        }
    }

    // 200 sequential field names should collide rarely.
    public void testHashDistinctForDifferentNames() {
        Set<Integer> hashes = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            String name = "field_" + i;
            byte[] buf = name.getBytes(UTF_8);
            int len = name.getBytes(UTF_8).length;
            hashes.add(FieldNameHash.hashName(buf, 0, len));
        }
        assertTrue("expected at least 195 distinct hashes out of 200 names, got " + hashes.size(), hashes.size() >= 195);
    }

    // Hash is independent of leading padding bytes in the buffer.
    public void testHashWithOffset() {
        byte[] padded = "XXXXname".getBytes(UTF_8);
        byte[] plain = "name".getBytes(UTF_8);
        assertEquals(FieldNameHash.hashName(plain, 0, 4), FieldNameHash.hashName(padded, 4, 4));
    }

    // len 1..8: readSmall path (byte-at-a-time for 1-3, int reads for 4-8).
    public void testHashShortKeys() {
        for (int len = 1; len <= 8; len++) {
            byte[] buf = new byte[len];
            Arrays.fill(buf, 0, len, (byte) 'x');
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals(0, h);
        }
    }

    // len 9..16: two overlapping readLE8 reads.
    public void testHashMediumKeys() {
        for (int len = 9; len <= 16; len++) {
            byte[] buf = new byte[len];
            Arrays.fill(buf, 0, len, (byte) 'y');
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals(0, h);
        }
    }

    // len > 16: loop + tail; exact-length buffers verify no over-read.
    public void testHashLongKeys() {
        for (int len : new int[] { 17, 32, 48, 100, 255 }) {
            byte[] buf = new byte[len];
            Arrays.fill(buf, 0, len, (byte) 'z');
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals(0, h);
        }
    }

    // Every tail remainder 1..16 after the 16-byte loop must hash correctly.
    public void testHashLongKeysTailBoundary() {
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

    // hashName must not read past len even when the buffer is larger.
    public void testHashLongKeysTailConsistentWithExactBuffer() {
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

    // Names identical except in the tail region (rem=1) must hash differently.
    public void testHashDistinguishesTailBytes() {
        byte[] a = "0123456789abcdefX".getBytes(UTF_8);
        byte[] b = "0123456789abcdefY".getBytes(UTF_8);
        assertEquals(17, a.length);
        assertNotEquals(
            "last-byte difference in rem=1 tail must produce different hashes",
            FieldNameHash.hashName(a, 0, a.length),
            FieldNameHash.hashName(b, 0, b.length)
        );
    }

    // readSmall for len 1-3 must ignore bytes past len.
    public void testHashSmallReadConsistency() {
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

    // readSmall for len 4-8 (native-order int reads) must ignore trailing bytes.
    public void testHashSmallReadForLen4To8() {
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

    // Empty key still produces a non-zero hash.
    public void testHashEmptyKey() {
        byte[] buf = new byte[0];
        int h = FieldNameHash.hashName(buf, 0, 0);
        assertNotEquals(0, h);
    }

    // ---- hashWord (fused scan+hash from pre-loaded word) ----

    // hashWord(word, len) must match hashName for len 0..8.
    public void testHashWordMatchesHashNameForAllLengths0To8() {
        byte[] name = "abcdefgh".getBytes(UTF_8);
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

    // Random 8-byte words — hashWord must stay consistent with hashName.
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

    // maskWord must produce the same prefix8 as readPrefix8.
    public void testMaskWordProducesCorrectPrefix8() {
        byte[] name = "abcdefgh".getBytes(UTF_8);
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

    // ---- scanAndHash ----

    // Fused scan returns (hash << 32 | len) for a simple unescaped field name.
    public void testScanAndHashSimpleField() {
        byte[] buf = makeScanBuffer("hello");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals("scanAndHash should not return -1 for a simple field", -1L, result);
        int len = (int) (result & 0xFFFFFFFFL);
        int hash = (int) (result >>> 32);
        assertEquals(5, len);
        assertEquals(FieldNameHash.hashName(buf, 0, 5), hash);
    }

    // Backslash in the name — caller must fall back to escaped resolution (-1).
    public void testScanAndHashReturnsMinusOneForBackslash() {
        byte[] buf = makeScanBufferRaw("hel\\lo\"");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertEquals(-1L, result);
    }

    // Backslash immediately before the closing quote still returns -1.
    public void testScanAndHashBackslashBeforeQuote() {
        byte[] buf = makeScanBufferRaw("abc\\\"def\"");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertEquals("backslash appears before quote, should return -1", -1L, result);
    }

    // Empty field name (quote immediately after opening quote).
    public void testScanAndHashEmptyFieldName() {
        byte[] buf = makeScanBuffer("");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        int len = (int) (result & 0xFFFFFFFFL);
        assertEquals(0, len);
    }

    // Long unescaped name spanning multiple 8-byte SIMD words.
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

    // Representative name lengths — scanAndHash must agree with hashName.
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

    // Closing quote at every offset 1..15 relative to the 8-byte word boundary.
    public void testScanAndHashScalarTailSweep() {
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

    // Short name with backslash — scalar tail must detect escape and return -1.
    public void testScanAndHashBackslashInScalarTail() {
        byte[] buf = makeScanBufferRaw("ab\\c\"");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertEquals(-1L, result);
    }

    // 7-char name: closing quote at last byte of first 8-byte word.
    public void testScanAndHashQuoteAt8ByteBoundary() {
        String name = "abcdefg";
        byte[] buf = makeScanBuffer(name);
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        assertEquals(7, (int) (result & 0xFFFFFFFFL));
    }

    // 8-char name: closing quote in second word — scalar tail on exact-length buffer.
    public void testScanAndHashQuoteJustPast8ByteBoundary() {
        String name = "abcdefgh";
        byte[] buf = makeScanBuffer(name);
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        assertEquals(8, (int) (result & 0xFFFFFFFFL));
    }

    // Every length 0..40 with distinct byte content — comprehensive scanAndHash sweep.
    public void testScanAndHashMatchesHashNameForAllLengthsUpTo40() {
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

    // ---- Helpers ----

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
        return content.getBytes(UTF_8);
    }
}
