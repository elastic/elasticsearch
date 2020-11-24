/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Locale;

public class StringMatcherTests extends ESTestCase {

    public void testEmptySet() throws Exception {
        final StringMatcher matcher = StringMatcher.of();
        for (int i = 0; i < 10; i++) {
            assertNoMatch(matcher, randomAlphaOfLengthBetween(i, 20));
        }
    }

    public void testSingleWildcard() throws Exception {
        final String prefix = randomAlphaOfLengthBetween(3, 5);
        final StringMatcher matcher = StringMatcher.of(prefix + "*");
        for (int i = 0; i < 10; i++) {
            assertMatch(matcher, prefix + randomAlphaOfLengthBetween(i, 20));
            assertNoMatch(matcher, randomAlphaOfLengthBetween(1, prefix.length() - 1));
            assertNoMatch(matcher, randomValueOtherThanMany(s -> s.startsWith(prefix), () -> randomAlphaOfLengthBetween(1, 8)));
        }
    }

    public void testSingleExactMatch() throws Exception {
        final String str = randomAlphaOfLengthBetween(3, 12);
        final StringMatcher matcher = StringMatcher.of(str);
        assertMatch(matcher, str);
        for (int i = 0; i < 10; i++) {
            assertNoMatch(matcher, randomValueOtherThanMany(s -> s.equals(str), () -> randomAlphaOfLengthBetween(1, 20)));
            assertNoMatch(matcher, randomAlphaOfLength(1) + str);
            assertNoMatch(matcher, str + randomAlphaOfLength(1));
        }
    }

    public void testSingleRegex() throws Exception {
        final String notStr = randomAlphaOfLengthBetween(3, 5);
        final StringMatcher matcher = StringMatcher.of("/~(" + notStr + ")/");
        assertNoMatch(matcher, notStr);
        for (int i = 0; i < 10; i++) {
            assertMatch(matcher, randomValueOtherThanMany(s -> s.equals(notStr), () -> randomAlphaOfLengthBetween(1, 20)));
            assertMatch(matcher, randomAlphaOfLength(1) + notStr);
            assertMatch(matcher, notStr + randomAlphaOfLength(1));
        }

    }

    public void testMultiplePatterns() throws Exception {
        final String prefix1 = randomAlphaOfLengthBetween(3, 5);
        final String prefix2 = randomAlphaOfLengthBetween(5, 8);
        final String prefix3 = randomAlphaOfLengthBetween(10, 12);
        final String suffix1 = randomAlphaOfLengthBetween(5, 10);
        final String suffix2 = randomAlphaOfLengthBetween(8, 12);
        final String exact1 = randomValueOtherThanMany(
            s -> s.startsWith(prefix1) || s.startsWith(prefix2) || s.startsWith(prefix3) || s.endsWith(suffix1) || s.endsWith(suffix2),
            () -> randomAlphaOfLengthBetween(5, 9));
        final String exact2 = randomValueOtherThanMany(
            s -> s.startsWith(prefix1) || s.startsWith(prefix2) || s.startsWith(prefix3) || s.endsWith(suffix1) || s.endsWith(suffix2),
            () -> randomAlphaOfLengthBetween(10, 12));
        final String exact3 = randomValueOtherThanMany(
            s -> s.startsWith(prefix1) || s.startsWith(prefix2) || s.startsWith(prefix3) || s.endsWith(suffix1) || s.endsWith(suffix2),
            () -> randomAlphaOfLengthBetween(15, 20));

        final StringMatcher matcher = StringMatcher.of(List.of(
            prefix1 + "*", prefix2 + "*", "/" + prefix3 + "@/", "*" + suffix1, "/@" + suffix2 + "/", exact1, exact2, exact3
        ));

        assertMatch(matcher, exact1);
        assertMatch(matcher, exact2);
        assertMatch(matcher, exact3);
        assertMatch(matcher, randomAlphaOfLength(3) + suffix1);
        assertMatch(matcher, randomAlphaOfLength(3) + suffix2);
        assertMatch(matcher, prefix1 + randomAlphaOfLengthBetween(1, 5));
        assertMatch(matcher, prefix2 + randomAlphaOfLengthBetween(1, 5));
        assertMatch(matcher, prefix3 + randomAlphaOfLengthBetween(1, 5));

        final char[] nonAlpha = "@/#$0123456789()[]{}<>;:%&".toCharArray();
        assertNoMatch(matcher, randomFrom(nonAlpha) + randomFrom(exact1, prefix1, suffix1) + randomFrom(nonAlpha));
        assertNoMatch(matcher, randomFrom(nonAlpha) + randomFrom(exact2, prefix2, suffix2) + randomFrom(nonAlpha));
        assertNoMatch(matcher, randomFrom(nonAlpha) + randomFrom(exact3, prefix3) + randomFrom(nonAlpha));
    }

    private void assertMatch(StringMatcher matcher, String str) {
        if (matcher.test(str) == false) {
            fail(String.format(Locale.ROOT, "Matcher [%s] failed to match [%s] but should", matcher, str));
        }
    }

    private void assertNoMatch(StringMatcher matcher, String str) {
        if (matcher.test(str)) {
            fail(String.format(Locale.ROOT, "Matcher [%s] matched [%s] but should not", matcher, str));
        }
    }


}
