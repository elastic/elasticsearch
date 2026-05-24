/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SecureStringTests extends ESTestCase {

    public void testCloseableCharsDoesNotModifySecureString() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        try (SecureString copy = secureString.clone()) {
            assertArrayEquals(password, copy.getChars());
            assertThat(copy.getChars(), not(sameInstance(password)));
        }
        assertSecureStringEqualToChars(password, secureString);
    }

    public void testClosingSecureStringDoesNotModifyCloseableChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertArrayEquals(password, copy.getChars());
        assertThat(copy.getChars(), not(sameInstance(password)));
        final char[] passwordCopy = Arrays.copyOf(password, password.length);
        assertArrayEquals(password, passwordCopy);
        secureString.close();
        assertNotEquals(password[0], passwordCopy[0]);
        assertArrayEquals(passwordCopy, copy.getChars());
    }

    public void testClosingChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertArrayEquals(password, copy.getChars());
        assertThat(copy.getChars(), not(sameInstance(password)));
        copy.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            copy.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, copy::getChars);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    public void testGetCloseableCharsAfterSecureStringClosed() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        secureString.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            secureString.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, secureString::clone);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    public void testEquals() {
        final List<String> strings = List.of(
            randomAlphaOfLengthBetween(8, 128),
            (char) 0 + randomAlphanumericOfLength(9),
            Character.MAX_VALUE + randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(11) + (char) 0,
            randomAlphanumericOfLength(12) + Character.MAX_VALUE,
            randomAlphanumericOfLength(3) + (char) 0 + randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(4) + Character.MAX_VALUE + randomAlphanumericOfLength(10)
        );
        for (var string : strings) {
            final SecureString secureString = new SecureString(string.toCharArray());

            verifyEquals(secureString, string, true);

            // Comparison has extra character in the middle
            final int split = randomIntBetween(1, string.length() - 1);
            final String altString = string.substring(0, split) + randomAlphanumericOfLength(1) + string.substring(split);
            verifyEquals(secureString, altString, false);

            // Comparison has extra characters at beginning
            verifyEquals(secureString, randomAlphanumericOfLength(1) + string, false);
            verifyEquals(secureString, randomAlphaOfLengthBetween(2, 8) + string, false);
            verifyEquals(secureString, "\0" + string, false);
            verifyEquals(secureString, (char) -1 + string, false);

            // Comparison has extra characters at end
            verifyEquals(secureString, string + randomAlphanumericOfLength(1), false);
            verifyEquals(secureString, string + randomAlphaOfLengthBetween(2, 8), false);
            verifyEquals(secureString, string + '\0', false);
            verifyEquals(secureString, string + (char) -1, false);

            // Comparison is missing characters at beginning
            verifyEquals(secureString, string.substring(1), false);
            verifyEquals(secureString, string.substring(randomIntBetween(2, string.length() - 2)), false);

            // Comparison is missing characters at end
            verifyEquals(secureString, string.substring(0, string.length() - 1), false);
            verifyEquals(secureString, string.substring(0, string.length() - randomIntBetween(2, string.length() - 2)), false);

            // Comparison has different character at beginning
            verifyEquals(
                secureString,
                randomValueOtherThan(string.substring(0, 1), () -> randomAlphanumericOfLength(1)) + string.substring(1),
                false
            );
            if (string.charAt(0) != 0) {
                verifyEquals(secureString, "\0" + string.substring(1), false);
            }
            if (string.charAt(0) != (char) -1) {
                verifyEquals(secureString, (char) -1 + string.substring(1), false);
            }
            // Comparison has different character at end
            verifyEquals(
                secureString,
                string.substring(0, string.length() - 1) + randomValueOtherThan(
                    string.substring(string.length() - 1),
                    () -> randomAlphanumericOfLength(1)
                ),
                false
            );
            if (string.endsWith("\0") == false) {
                verifyEquals(secureString, string.substring(0, string.length() - 1) + '\0', false);
            }
            if (string.charAt(string.length() - 1) != (char) -1) {
                verifyEquals(secureString, string.substring(0, string.length() - 1) + (char) -1, false);
            }

            assertThat(secureString.equals(""), is(false));
            final Object obj = null;
            // noinspection ConstantValue
            assertThat(secureString.equals(obj), is(false));

            final CharSequence cs = null;
            assertThat(secureString.equals(cs), is(false));
        }

    }

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    private void verifyEquals(SecureString secure, String string, boolean expected) {
        // Verify that we get the same result for different types of CharSequence
        assertThat(secure + " == " + string, secure.equals(string), is(expected));
        assertThat(secure.equals(new SecureString(string.toCharArray())), is(expected));
        assertThat(secure.equals(new StringBuilder(string)), is(expected));
        assertThat(secure.equals((Object) string), is(expected));
    }

    public void testStartsWith() {
        final String str = randomAlphanumericOfLength(16);
        final SecureString secStr = new SecureString(str.toCharArray());
        for (int i = 0; i <= str.length(); i++) {
            final String substr = str.substring(0, i);
            assertThat(secStr.startsWith(substr), is(true));
            assertThat(secStr.startsWith(new SecureString(substr.toCharArray())), is(true));
            assertThat(secStr.startsWith(new StringBuilder(substr)), is(true));

            if (i != 0) {
                assertThat(secStr.startsWith(randomValueOtherThan(substr, () -> randomAlphanumericOfLength(substr.length()))), is(false));
                if (i > 1) {
                    final int suffixLength = randomIntBetween(1, i - 1);
                    final String altStr = substr.substring(0, i - suffixLength) + randomValueOtherThan(
                        substr.substring(i - suffixLength),
                        () -> randomAlphanumericOfLength(suffixLength)
                    );
                    assertThat(altStr, hasLength(substr.length()));
                    assertThat(secStr.startsWith(altStr), is(false));
                }
            }
        }

        assertThat(secStr.startsWith(str + randomAlphanumericOfLength(1)), is(false));
    }

    public void testRegionMatches() {
        // Matching text
        assertRegionMatch(true, "abc", 0, "012abc789", 3, 3);
        assertRegionMatch(true, "abc", 0, "abc789", 0, 3);
        assertRegionMatch(true, "abc", 0, "012abc", 3, 3);
        assertRegionMatch(true, "XYabcZ", 2, "012abc789", 3, 3);
        assertRegionMatch(true, "XYabcZ", 2, "abc789", 0, 3);
        assertRegionMatch(true, "XYabcZ", 2, "012abc", 3, 3);
        assertRegionMatch(true, "XYabcZ", 2, "abc", 0, 3);

        // Bad region boundaries
        assertRegionMatch(false, "abc", -1, "abc", 0, 3);
        assertRegionMatch(false, "abc", 0, "abc", -1, 3);
        assertRegionMatch(false, "abc", 0, "abc", 0, 4);
        assertRegionMatch(false, "abc", 0, "ab", 0, 3);
        assertRegionMatch(false, "abc", 0, "Xab", 1, 3);

        // Mismatched text
        assertRegionMatch(false, "abc", 0, "012", 0, 3);
        assertRegionMatch(false, "012abc", 3, "012", 0, 3);
        assertRegionMatch(false, "abc012", 0, "012", 0, 3);
        assertRegionMatch(false, "012abc789", 3, "012", 0, 3);
        assertRegionMatch(false, "abc", 0, "xyz012", 3, 3);
        assertRegionMatch(false, "012abc", 3, "xyz012", 3, 3);
        assertRegionMatch(false, "abc012", 0, "xyz012", 3, 3);
        assertRegionMatch(false, "012abc789", 3, "xyz012", 3, 3);
        assertRegionMatch(false, "abc", 0, "012xyz", 0, 3);
        assertRegionMatch(false, "012abc", 3, "012xyz", 0, 3);
        assertRegionMatch(false, "abc012", 0, "012xyz", 0, 3);
        assertRegionMatch(false, "012abc789", 3, "012xyz", 0, 3);
        assertRegionMatch(false, "abc", 0, "abc012xyz", 3, 3);
        assertRegionMatch(false, "012abc", 3, "abc012xyz", 3, 3);
        assertRegionMatch(false, "abc012", 0, "abc012xyz", 3, 3);
        assertRegionMatch(false, "012abc789", 3, "abc012xyz", 3, 3);

        // Zero length always matches
        assertRegionMatch(
            true,
            randomAlphanumericOfLength(12),
            randomIntBetween(0, 12),
            randomAlphanumericOfLength(8),
            randomIntBetween(0, 8),
            0
        );

        // Random chars
        final String shared = randomAlphanumericOfLength(randomIntBetween(4, 64));
        final String rand1 = randomAlphaOfLengthBetween(0, 256);
        final String rand2 = randomAlphaOfLengthBetween(0, 256);
        assertRegionMatch(
            true,
            rand1 + shared + randomAlphaOfLengthBetween(0, 256),
            rand1.length(),
            rand2 + shared + randomAlphaOfLengthBetween(0, 256),
            rand2.length(),
            shared.length()
        );
        assertRegionMatch(
            false,
            rand1 + shared + randomAlphaOfLengthBetween(0, 256),
            rand1.length(),
            rand2 + randomValueOtherThan(shared, () -> randomAlphanumericOfLength(shared.length())) + randomAlphaOfLengthBetween(0, 256),
            rand2.length(),
            shared.length()
        );
    }

    private void assertRegionMatch(final boolean expected, String a, int offsetA, String b, int offsetB, int len) {
        // First check that our test strings match using the standard `String` version
        assert a.regionMatches(offsetA, b, offsetB, len) == expected
            : "Bad test case [" + a + "][" + offsetA + "] vs [" + b + "][" + offsetB + "] : " + len + " == " + expected;

        // Test a-vs-b and b-vs-a because these operations should be identical
        assertThat(new SecureString(a.toCharArray()).regionMatches(offsetA, b, offsetB, len), is(expected));
        assertThat(new SecureString(b.toCharArray()).regionMatches(offsetB, a, offsetA, len), is(expected));
    }

    private void assertSecureStringEqualToChars(char[] expected, SecureString secureString) {
        int pos = 0;
        for (int i : secureString.chars().toArray()) {
            if (pos >= expected.length) {
                fail("Index " + i + " greated than or equal to array length " + expected.length);
            } else {
                assertEquals(expected[pos++], (char) i);
            }
        }
    }
}
