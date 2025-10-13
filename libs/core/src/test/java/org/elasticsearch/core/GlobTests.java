/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GlobTests extends ESTestCase {

    public void testMatchNull() {
        assertThat(Glob.globMatch(null, null), is(false));
        assertThat(Glob.globMatch(randomAlphaOfLengthBetween(1, 10), null), is(false));
        assertThat(Glob.globMatch(null, randomAlphaOfLengthBetween(1, 10)), is(false));
    }

    public void testMatchLiteral() {
        assertMatch("", "");
        var str = randomAlphaOfLengthBetween(1, 12);
        assertMatch(str, str);

        str = randomAlphanumericOfLength(randomIntBetween(1, 12));
        assertMatch(str, str);

        str = randomAsciiStringNoAsterisks(randomIntBetween(1, 24));
        assertMatch(str, str);
    }

    public void testSingleAsterisk() {
        assertMatch("*", "");
        assertMatch("*", randomAlphaOfLengthBetween(1, 12));
        assertMatch("*", randomAlphanumericOfLength(randomIntBetween(1, 12)));
        assertMatch("*", randomAsciiString(randomIntBetween(1, 24), ch -> ch >= ' ' && ch <= '~'));
        assertMatch("*", "*".repeat(randomIntBetween(1, 5)));
    }

    public void testMultipleConsecutiveAsterisk() {
        var pattern = "*".repeat(randomIntBetween(2, 5));

        assertMatch(pattern, "");
        assertMatch(pattern, randomAlphaOfLengthBetween(1, 12));
        assertMatch(pattern, randomAlphanumericOfLength(randomIntBetween(1, 12)));
        assertMatch(pattern, randomAsciiString(randomIntBetween(1, 24)));
        assertMatch(pattern, "*".repeat(randomIntBetween(1, 5)));
    }

    public void testPrefixMatch() {
        assertMatch("123*", "123");
        assertMatch("123*", "123abc");
        assertMatch("123*", "123123123");
        assertNonMatch("123*", "12");
        assertNonMatch("123*", "124");
        assertNonMatch("123*", "23");
        assertNonMatch("123*", "23x");
        assertNonMatch("123*", "x23");
        assertNonMatch("123*", "12*");
        assertNonMatch("123*", "12-3");
        assertNonMatch("123*", "1.2.3");
        assertNonMatch("123*", "abc123");
        assertNonMatch("123*", "abc123def");

        var prefix = randomAsciiStringNoAsterisks(randomIntBetween(2, 12));
        var pattern = prefix + "*";
        assertMatch(pattern, prefix);
        assertMatch(pattern, prefix + randomAsciiString(randomIntBetween(1, 30)));
        assertNonMatch(
            pattern,
            randomValueOtherThanMany(s -> s.charAt(0) == prefix.charAt(0), () -> randomAsciiString(randomIntBetween(1, 30))) + prefix
        );
        assertNonMatch(pattern, prefix.substring(0, prefix.length() - 1));
        assertNonMatch(pattern, prefix.substring(1));
    }

    public void testSuffixMatch() {
        assertMatch("*123", "123");
        assertMatch("*123", "abc123");
        assertMatch("*123", "123123123");
        assertNonMatch("*123", "12");
        assertNonMatch("*123", "x12");
        assertNonMatch("*123", "23");
        assertNonMatch("*123", "x23");
        assertNonMatch("*123", "12*");
        assertNonMatch("*123", "1.2.3");
        assertNonMatch("*123", "123abc");
        assertNonMatch("*123", "abc123def");

        var suffix = randomAsciiStringNoAsterisks(randomIntBetween(2, 12));
        var pattern = "*" + suffix;
        assertMatch(pattern, suffix);
        assertMatch(pattern, randomAsciiString(randomIntBetween(1, 30)) + suffix);
        assertNonMatch(
            pattern,
            randomValueOtherThanMany(str -> str.endsWith(suffix), () -> suffix + "#" + randomAsciiString(randomIntBetween(1, 30)))
        );
        assertNonMatch(pattern, suffix.substring(0, suffix.length() - 1));
        assertNonMatch(pattern, suffix.substring(1));
    }

    public void testInfixStringMatch() {
        assertMatch("*123*", "abc123def");
        assertMatch("*123*", "abc123");
        assertMatch("*123*", "123def");
        assertMatch("*123*", "123");
        assertMatch("*123*", "123123123");
        assertMatch("*123*", "1.12.123.1234");
        assertNonMatch("*123*", "12");
        assertNonMatch("*123*", "23");
        assertNonMatch("*123*", "x23");
        assertNonMatch("*123*", "12*");
        assertNonMatch("*123*", "1.2.3");

        var infix = randomAsciiStringNoAsterisks(randomIntBetween(2, 12));
        var pattern = "*" + infix + "*";
        assertMatch(pattern, infix);
        assertMatch(pattern, randomAsciiString(randomIntBetween(1, 30)) + infix + randomAsciiString(randomIntBetween(1, 30)));
        assertMatch(pattern, randomAsciiString(randomIntBetween(1, 30)) + infix);
        assertMatch(pattern, infix + randomAsciiString(randomIntBetween(1, 30)));
        assertNonMatch(pattern, infix.substring(0, infix.length() - 1));
        assertNonMatch(pattern, infix.substring(1));
    }

    public void testInfixAsteriskMatch() {
        assertMatch("abc*xyz", "abcxyz");
        assertMatch("abc*xyz", "abc#xyz");
        assertMatch("abc*xyz", "abc*xyz");
        assertMatch("abc*xyz", "abcdefghijklmnopqrstuvwxyz");
        assertNonMatch("abc*xyz", "ABC.xyz");
        assertNonMatch("abc*xyz", "RabcSxyzT");
        assertNonMatch("abc*xyz", "RabcSxyz");
        assertNonMatch("abc*xyz", "abcSxyzT");

        assertMatch("123*321", "123321");
        assertMatch("123*321", "12345678987654321");
        assertNonMatch("123*321", "12321");

        var prefix = randomAsciiStringNoAsterisks(randomIntBetween(2, 12));
        var suffix = randomAsciiStringNoAsterisks(randomIntBetween(2, 12));
        var pattern = prefix + "*" + suffix;
        assertMatch(pattern, prefix + suffix);
        assertMatch(pattern, prefix + randomAsciiString(randomIntBetween(1, 30)) + suffix);
        assertNonMatch(pattern, prefix.substring(0, prefix.length() - 1) + suffix);
        assertNonMatch(pattern, prefix + suffix.substring(1));
    }

    public void testLiteralSubstringMatching() {
        assertMatch("start*middle*end", "startmiddleend");
        assertMatch("start*middle*end", "start.middle.end");
        assertMatch("start*middle*end", "start.middlX.middle.end");
        assertMatch("start*middle*end", "start.middlmiddle.end");
        assertMatch("start*middle*end", "start.middle.eend");
        assertMatch("start*middle*end", "start.middle.enend");
        assertMatch("start*middle*end", "start.middle.endend");

        assertNonMatch("start*middle*end", "startmiddlend");
        assertNonMatch("start*middle*end", "start.end");
        assertNonMatch("start*middle*end", "start+MIDDLE+end");
        assertNonMatch("start*middle*end", "start+mid+dle+end");
        assertNonMatch("start*middle*end", "start+mid+middle+en");
    }

    private static void assertMatch(String pattern, String str) {
        assertThat("Expect [" + str + "] to match '" + pattern + "'", Glob.globMatch(pattern, str), is(true));
    }

    private static void assertNonMatch(String pattern, String str) {
        assertThat("Expect [" + str + "] to not match '" + pattern + "'", Glob.globMatch(pattern, str), is(false));
    }

    @FunctionalInterface
    interface CharPredicate {
        boolean test(char c);
    }

    private String randomAsciiString(int length) {
        return randomAsciiString(length, ch -> ch >= ' ' && ch <= '~');
    }

    private String randomAsciiStringNoAsterisks(final int length) {
        return randomAsciiString(length, ch -> ch >= ' ' && ch <= '~' && ch != '*');
    }

    private String randomAsciiString(int length, CharPredicate validCharacters) {
        StringBuilder str = new StringBuilder(length);
        nextChar: for (int i = 0; i < length; i++) {
            for (int attempts = 0; attempts < 200; attempts++) {
                char ch = (char) randomIntBetween(0x1, 0x7f);
                if (validCharacters.test(ch)) {
                    str.append(ch);
                    continue nextChar;
                }
            }
            throw new IllegalStateException("Cannot find valid character for string");
        }
        assertThat(str.length(), equalTo(length));
        return str.toString();
    }

}
