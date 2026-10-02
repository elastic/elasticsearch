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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

// Behavioral tests for FieldNameHash: wyhash invariants, hashWord/maskWord, and scanAndHash.
public class FieldNameHashTests extends ESTestCase {

    // ---- hashName ----

    // Same input bytes must always yield the same hash.
    public void testHashDeterministic() {
        for (int len = 1; len <= 255; len++) {
            byte[] buf = randomAlphaOfLength(len).getBytes(UTF_8);
            int h1 = FieldNameHash.hashName(buf, 0, len);
            int h2 = FieldNameHash.hashName(buf, 0, len);
            assertEquals(h1, h2);
        }
    }

    // Zero is reserved for empty hash-table slots — hashName must never return it.
    public void testHashNeverZero() {
        assertNotEquals(0, FieldNameHash.hashName(new byte[0], 0, 0));
        for (int len = 1; len <= 255; len++) {
            byte[] buf = randomAlphaOfLength(len).getBytes(UTF_8);
            assertEquals(len, buf.length);
            int h = FieldNameHash.hashName(buf, 0, len);
            assertNotEquals("hash must never be 0 (reserved for empty slot), len=" + len, 0, h);
        }
    }

    // Distinct field names should collide rarely (smoke check on hash spread).
    public void testHashDistinctForDifferentNames() {
        Set<Integer> hashes = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            byte[] buf = (randomAlphaOfLengthBetween(4, 24) + i).getBytes(UTF_8);
            hashes.add(FieldNameHash.hashName(buf, 0, buf.length));
        }
        assertTrue("expected at least 195 distinct hashes out of 200 names, got " + hashes.size(), hashes.size() >= 195);
    }

    // hashName depends only on buf[off, off+len), not on leading or trailing buffer padding.
    public void testHashNameIgnoresBytesBeyondLength() {
        for (int len = 1; len <= 255; len++) {
            byte[] exact = randomAlphaOfLength(len).getBytes(UTF_8);
            int expected = FieldNameHash.hashName(exact, 0, len);

            byte[] trailingPadded = Arrays.copyOf(exact, len + 64);
            Arrays.fill(trailingPadded, len, trailingPadded.length, (byte) 0xFF);
            assertEquals("hash must not read trailing bytes past len=" + len, expected, FieldNameHash.hashName(trailingPadded, 0, len));

            int leading = between(1, 32);
            byte[] offsetPadded = new byte[leading + len + 64];
            Arrays.fill(offsetPadded, (byte) 0xFF);
            System.arraycopy(exact, 0, offsetPadded, leading, len);
            assertEquals(
                "hash must not depend on leading padding, len=" + len + " off=" + leading,
                expected,
                FieldNameHash.hashName(offsetPadded, leading, len)
            );
        }
    }

    // A one-byte difference in otherwise identical names must change the hash.
    public void testHashDistinguishesDifferentNames() {
        for (int len = 1; len <= 255; len++) {
            String prefix = randomAlphaOfLength(len);
            byte[] a = (prefix + "X").getBytes(UTF_8);
            byte[] b = (prefix + "Y").getBytes(UTF_8);
            assertNotEquals(
                "different field names must hash differently",
                FieldNameHash.hashName(a, 0, a.length),
                FieldNameHash.hashName(b, 0, b.length)
            );
        }
    }

    // ---- hashWord ----

    // hashWord on a pre-loaded 8-byte word must match hashName for len 0..8.
    public void testHashWordMatchesHashNameForAllLengths0To8() {
        byte[] name = "abcdefgh".getBytes(UTF_8);
        long word = wordFromBytes(name);
        for (int len = 0; len <= 8; len++) {
            assertEquals(
                "hashWord must match hashName for len=" + len,
                FieldNameHash.hashName(name, 0, len),
                FieldNameHash.hashWord(word, len)
            );
        }
    }

    // hashWord/hashName agreement holds for random 8-byte word contents.
    public void testHashWordWithRandomBytes() {
        for (int iter = 0; iter < 100; iter++) {
            byte[] name = randomAlphaOfLength(8).getBytes(UTF_8);
            long word = wordFromBytes(name);
            int len = randomIntBetween(0, 8);
            assertEquals(
                "hashWord must match hashName for random input, len=" + len,
                FieldNameHash.hashName(name, 0, len),
                FieldNameHash.hashWord(word, len)
            );
        }
    }

    // maskWord must produce the same 8-byte prefix as FrozenFieldNameTable.readPrefix8.
    public void testMaskWordMatchesPrefix8() {
        byte[] name = "abcdefgh".getBytes(UTF_8);
        long word = wordFromBytes(name);
        for (int len = 0; len <= 8; len++) {
            assertEquals(
                "maskWord must match readPrefix8 for len=" + len,
                FrozenFieldNameTable.readPrefix8(name, 0, len),
                FieldNameHash.maskWord(word, len)
            );
        }
    }

    // ---- scanAndHash ----

    // scanAndHash on an unescaped name returns length and hash matching hashName.
    public void testScanAndHashSimpleField() {
        byte[] buf = makeScanBuffer("hello");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals("scanAndHash should not return -1 for a simple field", -1L, result);
        assertScanAndHashMatchesHashName(buf, 0, result, 5);
    }

    // Backslash in the field name forces the caller to resolve escapes (-1 sentinel).
    public void testScanAndHashReturnsMinusOneForBackslash() {
        assertEquals(-1L, FieldNameHash.scanAndHash(makeScanBufferRaw("hel\\lo\""), 0));
        assertEquals(-1L, FieldNameHash.scanAndHash(makeScanBufferRaw("abc\\\"def\""), 0));
        assertEquals(-1L, FieldNameHash.scanAndHash(makeScanBufferRaw("ab\\c\""), 0));
    }

    // Empty field name (quote immediately after opening quote) yields length 0.
    public void testScanAndHashEmptyFieldName() {
        byte[] buf = makeScanBuffer("");
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        assertEquals(0, (int) (result & 0xFFFFFFFFL));
    }

    // Long unescaped names spanning multiple 8-byte scan steps still match hashName.
    public void testScanAndHashLongFieldName() {
        String name = randomAlphaOfLengthBetween(48, 80);
        byte[] buf = makeScanBuffer(name);
        long result = FieldNameHash.scanAndHash(buf, 0);
        assertNotEquals(-1L, result);
        assertScanAndHashMatchesHashName(buf, 0, result, name.length());
    }

    // scanAndHash agrees with hashName for every unescaped name length 0..40.
    public void testScanAndHashMatchesHashNameForAllLengthsUpTo40() {
        for (int nameLen = 0; nameLen <= 40; nameLen++) {
            String name = randomAlphaOfLength(nameLen);
            byte[] buf = makeScanBuffer(name);
            long result = FieldNameHash.scanAndHash(buf, 0);
            assertNotEquals("scanAndHash failed for len=" + nameLen, -1L, result);
            assertScanAndHashMatchesHashName(buf, 0, result, nameLen);
        }
    }

    // Asserts the packed scanAndHash result matches hashName for the given length.
    private static void assertScanAndHashMatchesHashName(byte[] buf, int startIdx, long result, int expectedLen) {
        assertEquals("length mismatch", expectedLen, (int) (result & 0xFFFFFFFFL));
        assertEquals("hash mismatch", FieldNameHash.hashName(buf, startIdx, expectedLen), (int) (result >>> 32));
    }

    // Builds a little-endian 8-byte word from the first 8 name bytes (hashWord input).
    private static long wordFromBytes(byte[] name) {
        long word = 0;
        for (int i = 0; i < 8; i++) {
            word |= (long) (name[i] & 0xFF) << (i * 8);
        }
        return word;
    }

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
