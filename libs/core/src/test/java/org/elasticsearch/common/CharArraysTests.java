/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

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

        final String other = randomAlphaOfLengthBetween(1, 32);
        assertFalse(CharArrays.constantTimeEquals(value, other));
        assertFalse(CharArrays.constantTimeEquals(value.toCharArray(), other.toCharArray()));
    }

    private char[] randomAlphaOfLengthNotBeginningWith(String undesiredPrefix, int min, int max) {
        char[] nonMatchingValue;
        do {
            nonMatchingValue = randomAlphaOfLengthBetween(min, max).toCharArray();
        } while (new String(nonMatchingValue).startsWith(undesiredPrefix));
        return nonMatchingValue;
    }
}
