/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.CharArrays;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class CharArraysTests extends ESTestCase {

    public void testCharsToBytes() {
        final String originalValue = randomUnicodeOfCodepointLengthBetween(0, 32);
        final byte[] expectedBytes = originalValue.getBytes(StandardCharsets.UTF_8);
        final char[] valueChars = originalValue.toCharArray();

        final byte[] convertedBytes = CharArrays.toUtf8Bytes(valueChars);
        assertArrayEquals(expectedBytes, convertedBytes);
    }

    public void testBytesToUtf8Chars() {
        final String originalValue = randomUnicodeOfCodepointLengthBetween(0, 32);
        final byte[] bytes = originalValue.getBytes(StandardCharsets.UTF_8);
        final char[] expectedChars = originalValue.toCharArray();

        final char[] convertedChars = CharArrays.utf8BytesToChars(bytes);
        assertArrayEquals(expectedChars, convertedChars);
    }

    /**
     * The hand-rolled encoder must produce byte-for-byte the output of the JDK encoder
     * under {@code CodingErrorAction.REPLACE} (which {@code Charset.encode} applies),
     * including for unpaired and wrongly-ordered surrogates.
     */
    public void testToUtf8BytesEquivalenceWithJdkEncoder() {
        for (int iter = 0; iter < 1000; iter++) {
            final char[] chars = randomCharsPossiblyWithBrokenSurrogates();
            final ByteBuffer expected = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars.clone()));
            final byte[] expectedBytes = new byte[expected.remaining()];
            expected.get(expectedBytes);
            assertArrayEquals("input: " + Arrays.toString(chars), expectedBytes, CharArrays.toUtf8Bytes(chars));
        }
    }

    /** Malformed input must never decode to meaningful characters; each malformed byte decodes to U+FFFD. */
    public void testUtf8BytesToCharsMalformedInput() {
        assertEquals("\uFFFD", decodeToString((byte) 0x80)); // stray continuation byte
        assertEquals("\uFFFD\uFFFD", decodeToString((byte) 0xC0, (byte) 0x80)); // overlong NUL must not decode to NUL
        assertEquals("\uFFFD\uFFFD\uFFFD", decodeToString((byte) 0xE0, (byte) 0x80, (byte) 0x80)); // overlong
        assertEquals("\uFFFD\uFFFD\uFFFD", decodeToString((byte) 0xED, (byte) 0xA0, (byte) 0x80)); // encoded surrogate U+D800
        assertEquals("\uFFFD\uFFFD\uFFFD\uFFFD", decodeToString((byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80)); // > U+10FFFF
        assertEquals("\uFFFD\uFFFD", decodeToString((byte) 0xF5, (byte) 0x80)); // invalid lead byte
        assertEquals("\uFFFD\uFFFD", decodeToString((byte) 0xE2, (byte) 0x82)); // truncated sequence
        assertEquals("a\uFFFD\uFFFDb", decodeToString((byte) 'a', (byte) 0xE2, (byte) 0x82, (byte) 'b'));
    }

    /** Decoding arbitrary bytes must not throw, and the result must round-trip cleanly from then on. */
    public void testUtf8BytesToCharsOnRandomBytesIsStable() {
        for (int iter = 0; iter < 100; iter++) {
            final char[] decoded = CharArrays.utf8BytesToChars(randomByteArrayOfLength(randomIntBetween(0, 64)));
            assertArrayEquals(decoded, CharArrays.utf8BytesToChars(CharArrays.toUtf8Bytes(decoded)));
        }
    }

    public void testUtf8BytesToCharsSubrange() {
        for (int iter = 0; iter < 100; iter++) {
            final String value = randomUnicodeOfCodepointLengthBetween(0, 32);
            final byte[] payload = value.getBytes(StandardCharsets.UTF_8);
            final byte[] prefix = randomByteArrayOfLength(randomIntBetween(0, 8));
            final byte[] suffix = randomByteArrayOfLength(randomIntBetween(0, 8));
            final byte[] bytes = new byte[prefix.length + payload.length + suffix.length];
            System.arraycopy(prefix, 0, bytes, 0, prefix.length);
            System.arraycopy(payload, 0, bytes, prefix.length, payload.length);
            System.arraycopy(suffix, 0, bytes, prefix.length + payload.length, suffix.length);
            assertArrayEquals(value.toCharArray(), CharArrays.utf8BytesToChars(bytes, prefix.length, payload.length));
        }
    }

    private static String decodeToString(byte... utf8Bytes) {
        return new String(CharArrays.utf8BytesToChars(utf8Bytes));
    }

    /** Exhaustive single-char encode equivalence across the entire BMP, including all surrogate values. */
    public void testToUtf8BytesEquivalenceForEveryBmpChar() {
        for (int c = 0; c <= 0xFFFF; c++) {
            final char[] chars = { (char) c };
            final ByteBuffer expected = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars.clone()));
            final byte[] expectedBytes = new byte[expected.remaining()];
            expected.get(expectedBytes);
            assertArrayEquals("char: " + c, expectedBytes, CharArrays.toUtf8Bytes(chars));
            if (Character.isSurrogate((char) c) == false) {
                assertArrayEquals("char: " + c, chars, CharArrays.utf8BytesToChars(CharArrays.toUtf8Bytes(chars)));
            } else {
                // lone surrogates do not round-trip: REPLACE semantics encode them as '?'
                assertArrayEquals("char: " + c, new char[] { '?' }, CharArrays.utf8BytesToChars(CharArrays.toUtf8Bytes(chars)));
            }
        }
    }

    public void testUtf8RoundTripForSupplementaryCodePoints() {
        for (int iter = 0; iter < 1000; iter++) {
            final int cp = randomIntBetween(Character.MIN_SUPPLEMENTARY_CODE_POINT, Character.MAX_CODE_POINT);
            final char[] chars = Character.toChars(cp);
            assertArrayEquals("code point: " + cp, chars, CharArrays.utf8BytesToChars(CharArrays.toUtf8Bytes(chars)));
        }
    }

    private char[] randomCharsPossiblyWithBrokenSurrogates() {
        final char[] chars = new char[randomIntBetween(0, 64)];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = switch (randomIntBetween(0, 4)) {
                case 0 -> randomAlphaOfLength(1).charAt(0);
                case 1 -> (char) randomIntBetween(0, 0xFFFF);
                case 2 -> (char) randomIntBetween(Character.MIN_HIGH_SURROGATE, Character.MAX_HIGH_SURROGATE);
                case 3 -> (char) randomIntBetween(Character.MIN_LOW_SURROGATE, Character.MAX_LOW_SURROGATE);
                case 4 -> (char) randomIntBetween(0x80, 0x7FF);
                default -> throw new AssertionError("unreachable");
            };
        }
        return chars;
    }

    public void testCharsBeginsWith() {
        assertFalse(CharArrays.charsBeginsWith(randomAlphaOfLength(4), null));
        assertFalse(CharArrays.charsBeginsWith(null, null));
        assertFalse(CharArrays.charsBeginsWith(null, randomAlphaOfLength(4).toCharArray()));
        final String undesiredPrefix = randomAlphaOfLength(2);
        assertFalse(CharArrays.charsBeginsWith(undesiredPrefix, randomAlphaOfLengthNotBeginningWith(undesiredPrefix, 3, 8)));

        final String prefix = randomAlphaOfLengthBetween(2, 4);
        assertTrue(CharArrays.charsBeginsWith(prefix, prefix.toCharArray()));
        final char[] prefixedValue = prefix.concat(randomAlphaOfLengthBetween(1, 12)).toCharArray();
        assertTrue(CharArrays.charsBeginsWith(prefix, prefixedValue));

        final String modifiedPrefix = randomBoolean() ? prefix.substring(1) : prefix.substring(0, prefix.length() - 1);
        char[] nonMatchingValue;
        do {
            nonMatchingValue = modifiedPrefix.concat(randomAlphaOfLengthBetween(0, 12)).toCharArray();
        } while (new String(nonMatchingValue).startsWith(prefix));
        assertFalse(CharArrays.charsBeginsWith(prefix, nonMatchingValue));
        assertTrue(CharArrays.charsBeginsWith(modifiedPrefix, nonMatchingValue));
    }

    public void testConstantTimeEquals() {
        final String value = randomAlphaOfLengthBetween(0, 32);
        assertTrue(CharArrays.constantTimeEquals(value, value));
        assertTrue(CharArrays.constantTimeEquals(value.toCharArray(), value.toCharArray()));

        // we want a different string, so ensure the first character is different, but the same overall length
        final int length = value.length();
        final String other = length > 0 ? new String(randomAlphaOfLengthNotBeginningWith(value.substring(0, 1), length, length)) : "";
        final boolean expectedEquals = length == 0;

        assertThat("value: " + value + ", other: " + other, CharArrays.constantTimeEquals(value, other), is(expectedEquals));
        assertThat(CharArrays.constantTimeEquals(value.toCharArray(), other.toCharArray()), is(expectedEquals));
    }

    private char[] randomAlphaOfLengthNotBeginningWith(String undesiredPrefix, int min, int max) {
        char[] nonMatchingValue;
        do {
            nonMatchingValue = randomAlphaOfLengthBetween(min, max).toCharArray();
        } while (new String(nonMatchingValue).startsWith(undesiredPrefix));
        return nonMatchingValue;
    }
}
