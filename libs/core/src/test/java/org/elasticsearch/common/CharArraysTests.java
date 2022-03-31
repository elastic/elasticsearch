/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.core.CharArrays;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

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
