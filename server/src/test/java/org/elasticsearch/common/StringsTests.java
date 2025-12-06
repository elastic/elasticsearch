/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.common.Strings.INVALID_CHARS;
import static org.elasticsearch.common.Strings.cleanTruncate;
import static org.elasticsearch.common.Strings.deleteAny;
import static org.elasticsearch.common.Strings.delimitedListToStringArray;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.common.Strings.isAllOrWildcard;
import static org.elasticsearch.common.Strings.isEmpty;
import static org.elasticsearch.common.Strings.padStart;
import static org.elasticsearch.common.Strings.stripDisallowedChars;
import static org.elasticsearch.common.Strings.substring;
import static org.elasticsearch.common.Strings.toLowercaseAscii;
import static org.elasticsearch.common.Strings.tokenizeByCommaToSet;
import static org.elasticsearch.common.Strings.trimLeadingCharacter;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

public class StringsTests extends ESTestCase {

    public void testHasLength() {
        assertFalse(hasLength((String) null));
        assertFalse(hasLength(""));
        assertTrue(hasLength(" "));
        assertTrue(hasLength("Hello"));

        assertTrue(hasLength("\0"));
    }

    public void testIsEmpty() {
        assertTrue(isEmpty(null));
        assertTrue(isEmpty(""));
        assertFalse(isEmpty(" "));
        assertFalse(isEmpty("Hello"));

        assertFalse(isEmpty("\0"));
    }

    public void testHasText() {
        assertFalse(hasText(null));
        assertFalse(hasText(""));
        assertFalse(hasText(" "));
        assertTrue(hasText("12345"));
        assertTrue(hasText(" 12345 "));

        String asciiWhitespace = IntStream.rangeClosed(0, 32)
            .filter(Character::isWhitespace)
            .mapToObj(Character::toString)
            .collect(Collectors.joining());
        assertFalse(hasText(asciiWhitespace));
        assertTrue(hasText("\ud855\udddd"));
    }

    public void testIsAllOrWildCardString() {
        assertThat(isAllOrWildcard("_all"), is(true));
        assertThat(isAllOrWildcard("*"), is(true));
        assertThat(isAllOrWildcard("foo"), is(false));
        assertThat(isAllOrWildcard(""), is(false));
        assertThat(isAllOrWildcard((String) null), is(false));
    }

    public void testSubstring() {
        assertNull(substring(null, 0, 1000));
        assertEquals("foo", substring("foo", 0, 1000));
        assertEquals("foo", substring("foo", 0, 3));
        assertEquals("oo", substring("foo", 1, 3));
        assertEquals("oo", substring("foo", 1, 100));
        assertEquals("f", substring("foo", 0, 1));
    }

    public void testCleanTruncate() {
        assertNull(cleanTruncate(null, 10));
        assertEquals("foo", cleanTruncate("foo", 10));
        assertEquals("foo", cleanTruncate("foo", 3));
        // Throws out high surrogates
        assertEquals("foo", cleanTruncate("foo\uD83D\uDEAB", 4));
        // But will keep the whole character
        assertEquals("foo\uD83D\uDEAB", cleanTruncate("foo\uD83D\uDEAB", 5));
        /*
         * Doesn't take care around combining marks. This example has its
         * meaning changed because that last codepoint is supposed to combine
         * backwards into the find "o" and be represented as the "o" with a
         * circle around it with a slash through it. As in "no 'o's allowed
         * here.
         */
        assertEquals("o", cleanTruncate("o\uD83D\uDEAB", 1));
        assertEquals("", cleanTruncate("foo", 0));
    }

    public void testTrimLeadingCharacter() {
        assertThat(trimLeadingCharacter("abcdef", 'g'), equalTo("abcdef"));
        assertThat(trimLeadingCharacter("aaabcdef", 'a'), equalTo("bcdef"));
    }

    public void testToStringToXContent() {
        final ToXContent toXContent;
        final boolean error;
        if (randomBoolean()) {
            if (randomBoolean()) {
                error = false;
                toXContent = (builder, params) -> builder.field("ok", "here").field("catastrophe", "");
            } else {
                error = true;
                toXContent = (builder, params) -> builder.startObject().field("ok", "here").field("catastrophe", "").endObject();
            }
        } else {
            if (randomBoolean()) {
                error = false;
                toXContent = (ToXContentObject) (builder, params) -> builder.startObject()
                    .field("ok", "here")
                    .field("catastrophe", "")
                    .endObject();
            } else {
                error = true;
                toXContent = (ToXContentObject) (builder, params) -> builder.field("ok", "here").field("catastrophe", "");
            }
        }

        String toString = Strings.toString(toXContent);
        if (error) {
            assertThat(toString, containsString("\"error\":\"error building toString out of XContent:"));
            assertThat(toString, containsString("\"stack_trace\":"));
        } else {
            assertThat(toString, containsString("\"ok\":\"here\""));
            assertThat(toString, containsString("\"catastrophe\":\"\""));
        }
    }

    public void testToStringToXContentWithOrWithoutParams() {
        ToXContent toXContent = (builder, params) -> builder.field("color_from_param", params.param("color", "red"));
        // Rely on the default value of "color" param when params are not passed
        assertThat(Strings.toString(toXContent), containsString("\"color_from_param\":\"red\""));
        // Pass "color" param explicitly
        assertThat(
            Strings.toString(toXContent, new ToXContent.MapParams(Collections.singletonMap("color", "blue"))),
            containsString("\"color_from_param\":\"blue\"")
        );
    }

    public void testDeleteAny() {
        assertNull(deleteAny((CharSequence) null, "abc"));
        assertNull(deleteAny((String) null, "abc"));
        assertThat(deleteAny(new StringBuilder("foo"), null), hasToString("foo"));
        assertThat(deleteAny("foo", null), equalTo("foo"));

        assertThat(deleteAny("abc\ndef\t", "az\n"), equalTo("bcdef\t"));

        String testStr = randomUnicodeOfLength(10);
        String delete = testStr.substring(testStr.length() - 1) + testStr.substring(0, 1);
        String expected = testStr.chars()
            .mapToObj(Character::toString)
            .filter(c -> delete.contains(c) == false)
            .collect(Collectors.joining());
        assertThat(deleteAny(testStr, delete), equalTo(expected));
        assertThat(deleteAny(new StringBuilder(testStr), delete), hasToString(expected));
    }

    public void testSplitStringToSet() {
        assertEquals(tokenizeByCommaToSet(null), Sets.newHashSet());
        assertEquals(tokenizeByCommaToSet(""), Sets.newHashSet());
        assertEquals(tokenizeByCommaToSet("a,b,c"), Sets.newHashSet("a", "b", "c"));
        assertEquals(tokenizeByCommaToSet("a, b, c"), Sets.newHashSet("a", "b", "c"));
        assertEquals(tokenizeByCommaToSet(" a ,  b, c  "), Sets.newHashSet("a", "b", "c"));
        assertEquals(tokenizeByCommaToSet("aa, bb, cc"), Sets.newHashSet("aa", "bb", "cc"));
        assertEquals(tokenizeByCommaToSet(" a "), Sets.newHashSet("a"));
        assertEquals(tokenizeByCommaToSet("   a   "), Sets.newHashSet("a"));
        assertEquals(tokenizeByCommaToSet("   aa   "), Sets.newHashSet("aa"));
        assertEquals(tokenizeByCommaToSet("   "), Sets.newHashSet());
    }

    public void testDelimitedListToStringArray() {
        String testStr;
        assertThat(delimitedListToStringArray(null, " ", "a"), emptyArray());
        // NOTE: current behaviour is to not delete anything if the delimiter is null
        assertThat(delimitedListToStringArray(testStr = randomAlphaOfLength(10), null, "a"), arrayContaining(testStr));
        assertThat(
            delimitedListToStringArray(testStr = randomAlphaOfLength(10), "", null),
            arrayContaining(testStr.chars().mapToObj(Character::toString).toArray())
        );
        assertThat(
            delimitedListToStringArray("bcdabceabcdf", "", "a"),
            arrayContaining("b", "c", "d", "", "b", "c", "e", "", "b", "c", "d", "f")
        );
        assertThat(
            delimitedListToStringArray("bcdabceabcdf", "", "da"),
            arrayContaining("b", "c", "", "", "b", "c", "e", "", "b", "c", "", "f")
        );
        assertThat(
            delimitedListToStringArray("abcdabceabcdf", "", "da"),
            arrayContaining("", "b", "c", "", "", "b", "c", "e", "", "b", "c", "", "f")
        );
        assertThat(delimitedListToStringArray("abcd,abce,abcdf", ",", "da"), arrayContaining("bc", "bce", "bcf"));
        assertThat(delimitedListToStringArray("abcd,abce,abcdf,", ",", "da"), arrayContaining("bc", "bce", "bcf", ""));
        assertThat(delimitedListToStringArray("abcd,abce,abcdf,bcad,a", ",a", "d"), arrayContaining("abc", "bce", "bcf,bca", ""));
    }

    public void testPadStart() {
        String testStr;
        assertThat(padStart("", 5, 'a'), equalTo("aaaaa"));
        assertThat(padStart(testStr = randomAlphaOfLength(6), 10, ' '), equalTo(" ".repeat(4) + testStr));
        assertThat(padStart(testStr = randomAlphaOfLength(6), 5, ' '), equalTo(testStr));
        assertThat(padStart(testStr = randomAlphaOfLength(6), 10, 'f'), equalTo("f".repeat(4) + testStr));
    }

    public void testToLowercaseAscii() {
        String testStr;
        assertThat(toLowercaseAscii(""), equalTo(""));
        assertThat(toLowercaseAscii(testStr = randomAlphaOfLength(5)), equalTo(testStr.toLowerCase(Locale.ROOT)));

        // all ascii characters
        testStr = IntStream.rangeClosed(0, 255).mapToObj(i -> Character.toString((char) i)).collect(Collectors.joining());
        assertThat(toLowercaseAscii(testStr), equalTo(lowercaseAsciiOnly(testStr)));

        // sling in some unicode too
        assertThat(toLowercaseAscii(testStr = randomUnicodeOfCodepointLength(20)), equalTo(lowercaseAsciiOnly(testStr)));
    }

    public void testStripDisallowedChars() {
        var validFileName = randomAlphaOfLength(INVALID_CHARS.size());
        var invalidChars = new ArrayList<>(INVALID_CHARS);
        Collections.shuffle(invalidChars, getRandom());

        var invalidFileName = new StringBuilder();

        // randomly build an invalid file name merging both sets of valid and invalid chars
        for (var i = 0; i < invalidChars.size(); i++) {
            if (randomBoolean()) {
                invalidFileName.append(validFileName.charAt(i)).append(invalidChars.get(i));
            } else {
                invalidFileName.append(invalidChars.get(i)).append(validFileName.charAt(i));
            }
        }

        assertNotEquals(validFileName, invalidFileName.toString());
        assertEquals(validFileName, stripDisallowedChars(invalidFileName.toString()));
    }

    public void testFormat1Decimals() {
        assertThat(Strings.format1Decimals(100.0 / 2, "%"), equalTo("50%"));
        assertThat(Strings.format1Decimals(100.0 / 3, "%"), equalTo("33.3%"));
    }

    private static String lowercaseAsciiOnly(String s) {
        // explicitly lowercase just ascii characters
        StringBuilder sb = new StringBuilder(s);
        for (int i = 0; i < sb.length(); i++) {
            char c = sb.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                sb.setCharAt(i, (char) (sb.charAt(i) + 32));
            }
        }
        return sb.toString();
    }
}
