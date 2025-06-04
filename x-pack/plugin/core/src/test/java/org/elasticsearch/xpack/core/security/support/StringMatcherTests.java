/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.sameInstance;

public class StringMatcherTests extends ESTestCase {

    public void testEmptySet() throws Exception {
        final StringMatcher matcher = StringMatcher.of();
        for (int i = 0; i < 10; i++) {
            assertNoMatch(matcher, randomAlphaOfLengthBetween(i, 20));
        }
    }

    public void testMatchAllWildcard() throws Exception {
        Supplier<String> randomPattern = () -> {
            final String s = randomAlphaOfLengthBetween(3, 5);
            return switch (randomIntBetween(1, 4)) {
                case 1 -> s;
                case 2 -> s + "*";
                case 3 -> "*" + s;
                case 4 -> "*" + s + "*";
                default -> throw new AssertionError();
            };
        };
        final List<String> patterns = Stream.of(randomList(0, 3, randomPattern), List.of("*"), randomList(0, 3, randomPattern))
            .flatMap(List::stream)
            .collect(Collectors.toList());
        final StringMatcher matcher = StringMatcher.of(patterns);
        for (int i = 0; i < 10; i++) {
            assertMatch(matcher, randomAlphaOfLengthBetween(i, 20));
        }

        assertThat(matcher.getPredicate(), sameInstance(Predicates.always()));
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

    public void testSingleQuestionMark() throws Exception {
        final String prefix = randomAlphaOfLengthBetween(3, 5);
        final StringMatcher matcher = StringMatcher.of(prefix + "?");

        assertMatch(matcher, prefix + randomAlphaOfLength(1));
        assertNoMatch(matcher, prefix + randomAlphaOfLengthBetween(2, 100));
        assertNoMatch(matcher, randomAlphaOfLengthBetween(1, prefix.length() - 1));
        assertNoMatch(matcher, randomValueOtherThanMany(s -> s.startsWith(prefix), () -> randomAlphaOfLengthBetween(1, 8)));
    }

    public void testUnicodeWildcard() throws Exception {
        // Lucene automatons don't work correctly on strings with high surrogates
        final String prefix = randomValueOtherThanMany(
            s -> StringMatcherTests.hasHighSurrogate(s) || s.contains("\\") || s.startsWith("/"),
            () -> randomRealisticUnicodeOfLengthBetween(3, 5)
        );
        final StringMatcher matcher = StringMatcher.of(prefix + "*");
        for (int i = 0; i < 10; i++) {
            assertMatch(matcher, prefix + randomRealisticUnicodeOfLengthBetween(i, 20));
            assertNoMatch(matcher, randomRealisticUnicodeOfLengthBetween(1, prefix.length() - 1));
            assertNoMatch(matcher, randomValueOtherThanMany(s -> s.startsWith(prefix), () -> randomUnicodeOfLengthBetween(1, 8)));
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
        final String prefix2 = randomValueOtherThanMany(s -> s.startsWith(prefix1), () -> randomAlphaOfLengthBetween(5, 8));
        final String prefix3 = randomValueOtherThanMany(s -> s.startsWith(prefix1), () -> randomAlphaOfLengthBetween(10, 12));
        final String suffix1 = randomAlphaOfLengthBetween(5, 10);
        final String suffix2 = randomAlphaOfLengthBetween(8, 12);
        final String exact1 = randomValueOtherThanMany(
            s -> s.startsWith(prefix1) || s.startsWith(prefix2) || s.startsWith(prefix3) || s.endsWith(suffix1) || s.endsWith(suffix2),
            () -> randomAlphaOfLengthBetween(5, 9)
        );
        final String exact2 = randomValueOtherThanMany(
            s -> s.startsWith(prefix1) || s.startsWith(prefix2) || s.startsWith(prefix3) || s.endsWith(suffix1) || s.endsWith(suffix2),
            () -> randomAlphaOfLengthBetween(10, 12)
        );
        final String exact3 = randomValueOtherThanMany(
            s -> s.startsWith(prefix1) || s.startsWith(prefix2) || s.startsWith(prefix3) || s.endsWith(suffix1) || s.endsWith(suffix2),
            () -> randomAlphaOfLengthBetween(15, 20)
        );

        final StringMatcher matcher = StringMatcher.of(
            List.of(prefix1 + "*", prefix2 + "?", "/" + prefix3 + "@/", "*" + suffix1, "/@" + suffix2 + "/", exact1, exact2, exact3)
        );

        final List<Character> nonAlpha = "@/#$0123456789()[]{}<>;:%&".chars()
            .mapToObj(c -> Character.valueOf((char) c))
            .collect(Collectors.toList());

        assertMatch(matcher, exact1);
        assertMatch(matcher, exact2);
        assertMatch(matcher, exact3);
        assertMatch(matcher, randomAlphaOfLength(3) + suffix1);
        assertMatch(matcher, randomAlphaOfLength(3) + suffix2);

        // Prefix 1 uses '*', it should match any number of trailing chars.
        assertMatch(matcher, prefix1 + randomAlphaOfLengthBetween(1, 5));
        assertMatch(matcher, prefix1 + randomFrom(nonAlpha));
        assertMatch(matcher, prefix1 + randomFrom(nonAlpha) + randomFrom(nonAlpha));

        // Prefix 2 uses a `?`, it should match any single trailing char, but not 2 chars.
        // (We don't test with a trailing alpha because it might match one of the matched suffixes)
        assertMatch(matcher, prefix2 + randomAlphaOfLength(1));
        assertMatch(matcher, prefix2 + randomFrom(nonAlpha));
        assertNoMatch(matcher, prefix2 + randomFrom(nonAlpha) + randomFrom(nonAlpha));
        assertNoMatch(matcher, prefix2 + randomAlphaOfLength(1) + randomFrom(nonAlpha));

        // Prefix 3 uses a regex with '@', it should match any number of trailing chars.
        assertMatch(matcher, prefix3 + randomAlphaOfLengthBetween(1, 5));
        assertMatch(matcher, prefix3 + randomFrom(nonAlpha));
        assertMatch(matcher, prefix3 + randomFrom(nonAlpha) + randomFrom(nonAlpha));

        for (String pattern : List.of(exact1, exact2, exact3, suffix1, suffix2, exact1, exact2, exact3)) {
            assertNoMatch(matcher, randomFrom(nonAlpha) + pattern + randomFrom(nonAlpha));
        }
    }

    public void testToString() throws Exception {
        // Replace any '/' characters because they're meaningful at the start, and just removing them all is simpler
        final String text1 = randomUnicodeOfLengthBetween(5, 80).replace('/', '.');
        final String text2 = randomUnicodeOfLength(20).replace('/', '.');
        final String text3 = randomUnicodeOfLength(50).replace('/', '.');
        final String text4 = randomAlphaOfLength(100);
        final String text5 = randomAlphaOfLength(100);

        for (String s1 : List.of(text1, text2, text3, text4, text5)) {
            assertThat(StringMatcher.of(s1).toString(), equalTo(s1));
            for (String s2 : List.of(text1, text2, text3, text4, text5)) {
                assertThat(StringMatcher.of(s1, s2).toString(), equalTo(s1 + "|" + s2));
            }
        }

        StringMatcher m = StringMatcher.of(text2, text3, text4, text5); // 270 chars
        assertThat(m.toString(), equalTo(text2 + "|" + text3 + "|" + text4.substring(0, 59) + "...|" + text5.substring(0, 59) + "..."));
    }

    public void testInvalidRegexPatterns() {
        final List<String> invalidPatterns = randomNonEmptySubsetOf(
            List.of(
                "/~(([.]|ilm-history-).*/",  // missing closing bracket
                "/~(([.]|ilm-history-).*", // missing ending slash,
                "/[0-9/", // missing closing square bracket
                "/a{0,3/", // missing closing curly bracket
                "/[]/", // empty character class
                "/a{}/" // empty number of occurrences
            )
        );
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> StringMatcher.of(invalidPatterns)
        );

        assertThat(
            e.getMessage(),
            containsString("The set of patterns [" + Strings.collectionToCommaDelimitedString(invalidPatterns) + "] is invalid")
        );
        assertThat(e.getCause(), isA(IllegalArgumentException.class));
    }

    private void assertMatch(StringMatcher matcher, String str) {
        if (matcher.test(str) == false) {
            fail(Strings.format("Matcher [%s] failed to match [%s] but should", matcher, str));
        }
    }

    private void assertNoMatch(StringMatcher matcher, String str) {
        if (matcher.test(str)) {
            fail(Strings.format("Matcher [%s] matched [%s] but should not", matcher, str));
        }
    }

    static boolean hasHighSurrogate(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isHighSurrogate(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

}
