/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IgnoredSourceNameUtf8Tests extends ESTestCase {

    private static final String TWO_BYTE_CHAR = "é";
    private static final String THREE_BYTE_CHAR = "€";
    // U+1F600, a supplementary code point: four UTF-8 bytes and two UTF-16 chars
    private static final String FOUR_BYTE_CHAR = new String(Character.toChars(0x1F600));

    public void testEmptyName() {
        assertByteLength("", randomValueBytes(), randomIntBetween(0, 10));
    }

    public void testAsciiNameIsOneBytePerChar() {
        // Cover lengths on both sides of the 8 byte word-at-a-time fast path.
        for (int length = 0; length <= 40; length++) {
            String name = randomAlphaOfLength(length);
            assertByteLength(name, randomValueBytes(), randomIntBetween(0, 10));
            assertThat(IgnoredSourceNameUtf8.byteLength(name.getBytes(StandardCharsets.UTF_8), 0, length, length), equalTo(length));
        }
    }

    public void testAsciiNameFollowedByNonAsciiValue() {
        // The fast path must stop at the end of the name even when the value that follows it is not ASCII.
        byte[] value = new byte[32];
        Arrays.fill(value, (byte) 0xFF);
        for (int length = 0; length <= 40; length++) {
            assertByteLength(randomAlphaOfLength(length), value, randomIntBetween(0, 10));
        }
    }

    public void testNonAsciiCharWidths() {
        assertByteLength(TWO_BYTE_CHAR, randomValueBytes(), 0);
        assertByteLength(THREE_BYTE_CHAR, randomValueBytes(), 0);
        assertByteLength(FOUR_BYTE_CHAR, randomValueBytes(), 0);
        assertThat(FOUR_BYTE_CHAR.length(), equalTo(2));
        assertThat(IgnoredSourceNameUtf8.byteLength(FOUR_BYTE_CHAR.getBytes(StandardCharsets.UTF_8), 0, 4, 2), equalTo(4));
    }

    public void testNonAsciiAfterAsciiPrefix() {
        // Non-ASCII chars at every position around the 8 byte boundary, so the fast path hands over to the char walk mid-name.
        for (int prefix = 0; prefix <= 20; prefix++) {
            String ascii = randomAlphaOfLength(prefix);
            for (String nonAscii : new String[] { TWO_BYTE_CHAR, THREE_BYTE_CHAR, FOUR_BYTE_CHAR }) {
                assertByteLength(ascii + nonAscii, randomValueBytes(), randomIntBetween(0, 10));
                assertByteLength(ascii + nonAscii + ascii, randomValueBytes(), randomIntBetween(0, 10));
            }
        }
    }

    public void testRandomUnicodeNames() {
        for (int i = 0; i < 1000; i++) {
            assertByteLength(randomRealisticUnicodeOfLengthBetween(0, 64), randomValueBytes(), randomIntBetween(0, 10));
        }
    }

    public void testNameStartingAtNonZeroOffset() {
        String name = randomAlphaOfLength(12) + THREE_BYTE_CHAR + randomAlphaOfLength(12);
        int offset = randomIntBetween(1, 16);
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[offset + nameBytes.length + 8];
        Arrays.fill(bytes, (byte) 0xFF);
        System.arraycopy(nameBytes, 0, bytes, offset, nameBytes.length);
        int end = bytes.length;
        assertThat(IgnoredSourceNameUtf8.byteLength(bytes, offset, end, name.length()), equalTo(nameBytes.length));
    }

    public void testNameEndingExactlyAtEnd() {
        String name = randomAlphaOfLength(randomIntBetween(1, 20)) + FOUR_BYTE_CHAR;
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        assertThat(IgnoredSourceNameUtf8.byteLength(nameBytes, 0, nameBytes.length, name.length()), equalTo(nameBytes.length));
    }

    public void testNameLongerThanEntry() {
        for (int length = 1; length <= 20; length++) {
            byte[] bytes = randomAlphaOfLength(length).getBytes(StandardCharsets.UTF_8);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IgnoredSourceNameUtf8.byteLength(bytes, 0, bytes.length, bytes.length + 1)
            );
            assertThat(e.getMessage(), containsString("name is longer than the entry"));
        }
    }

    public void testTruncatedMultiByteChar() {
        for (String nonAscii : new String[] { TWO_BYTE_CHAR, THREE_BYTE_CHAR, FOUR_BYTE_CHAR }) {
            byte[] full = (randomAlphaOfLength(randomIntBetween(0, 10)) + nonAscii).getBytes(StandardCharsets.UTF_8);
            int charCount = full.length - nonAscii.getBytes(StandardCharsets.UTF_8).length + nonAscii.length();
            // Cut the entry in the middle of the last char.
            int end = full.length - 1;
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IgnoredSourceNameUtf8.byteLength(full, 0, end, charCount)
            );
            assertThat(e.getMessage(), containsString("name is longer than the entry"));
        }
    }

    public void testContinuationByteWhereLeadByteIsExpected() {
        for (int prefix = 0; prefix <= 12; prefix++) {
            byte[] bytes = new byte[prefix + 4];
            Arrays.fill(bytes, (byte) 'a');
            bytes[prefix] = (byte) randomIntBetween(0x80, 0xBF);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> IgnoredSourceNameUtf8.byteLength(bytes, 0, bytes.length, bytes.length)
            );
            assertThat(e.getMessage(), containsString("invalid UTF-8 lead byte"));
        }
    }

    public void testCharCountIsNotMistakenForByteCount() {
        // A misframed entry whose header claims fewer chars than the name has must not read past the claimed chars.
        String name = "a" + THREE_BYTE_CHAR + "b";
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        assertThat(IgnoredSourceNameUtf8.byteLength(bytes, 0, bytes.length, 2), equalTo(4));
    }

    /**
     * Encodes {@code name} followed by {@code value} (with {@code padding} unrelated leading bytes) and checks that the byte length of
     * the name is found without looking at the value.
     */
    private static void assertByteLength(String name, byte[] value, int padding) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[padding + nameBytes.length + value.length];
        Arrays.fill(bytes, 0, padding, (byte) 0xFF);
        System.arraycopy(nameBytes, 0, bytes, padding, nameBytes.length);
        System.arraycopy(value, 0, bytes, padding + nameBytes.length, value.length);
        assertThat(IgnoredSourceNameUtf8.byteLength(bytes, padding, bytes.length, name.length()), equalTo(nameBytes.length));
    }

    private static byte[] randomValueBytes() {
        // Arbitrary bytes, which unlike the name are not necessarily valid UTF-8.
        return randomByteArrayOfLength(randomIntBetween(0, 40));
    }
}
