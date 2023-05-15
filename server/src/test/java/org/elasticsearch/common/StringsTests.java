/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.common.Strings.INVALID_CHARS;
import static org.elasticsearch.common.Strings.cleanTruncate;
import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.elasticsearch.common.Strings.collectionToDelimitedStringWithLimit;
import static org.elasticsearch.common.Strings.deleteAny;
import static org.elasticsearch.common.Strings.delimitedListToStringArray;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.common.Strings.isAllOrWildcard;
import static org.elasticsearch.common.Strings.isEmpty;
import static org.elasticsearch.common.Strings.padStart;
import static org.elasticsearch.common.Strings.spaceify;
import static org.elasticsearch.common.Strings.stripDisallowedChars;
import static org.elasticsearch.common.Strings.substring;
import static org.elasticsearch.common.Strings.toLowercaseAscii;
import static org.elasticsearch.common.Strings.tokenizeByCommaToSet;
import static org.elasticsearch.common.Strings.trimLeadingCharacter;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StringsTests extends ESTestCase {

    public void testSpaceify() throws Exception {
        String[] lines = new String[] { randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5) };

        // spaceify always finishes with \n regardless of input
        StringBuilder sb = new StringBuilder();
        spaceify(4, String.join("\n", lines), sb);
        assertThat(sb.toString(), equalTo(Arrays.stream(lines).map(s -> " ".repeat(4) + s).collect(Collectors.joining("\n", "", "\n"))));

        sb = new StringBuilder();
        spaceify(0, String.join("\n", lines), sb);
        assertThat(sb.toString(), equalTo(Arrays.stream(lines).collect(Collectors.joining("\n", "", "\n"))));
    }

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

    public void testCollectionToDelimitedStringWithLimitZero() {
        final String delimiter = randomFrom("", ",", ", ", "/");
        final String prefix = randomFrom("", "[");
        final String suffix = randomFrom("", "]");

        final int count = between(0, 100);
        final List<String> strings = new ArrayList<>(count);
        while (strings.size() < count) {
            // avoid starting with a sequence of empty appends, it makes the assertions much messier
            final int minLength = strings.isEmpty() && delimiter.isEmpty() && prefix.isEmpty() && suffix.isEmpty() ? 1 : 0;
            strings.add(randomAlphaOfLength(between(minLength, 10)));
        }

        final StringBuilder stringBuilder = new StringBuilder();
        collectionToDelimitedStringWithLimit(strings, delimiter, prefix, suffix, 0, stringBuilder);
        final String completelyTruncatedDescription = stringBuilder.toString();

        if (count == 0) {
            assertThat(completelyTruncatedDescription, equalTo(""));
        } else if (count == 1) {
            assertThat(completelyTruncatedDescription, equalTo(prefix + strings.get(0) + suffix));
        } else {
            assertThat(
                completelyTruncatedDescription,
                equalTo(prefix + strings.get(0) + suffix + delimiter + "... (" + count + " in total, " + (count - 1) + " omitted)")
            );
        }
    }

    public void testCollectionToDelimitedStringWithLimitTruncation() {
        final String delimiter = randomFrom("", ",", ", ", "/");
        final String prefix = randomFrom("", "[");
        final String suffix = randomFrom("", "]");

        final int count = between(2, 100);
        final List<String> strings = new ArrayList<>(count);
        while (strings.size() < count) {
            // avoid empty appends, it makes the assertions much messier
            final int minLength = delimiter.isEmpty() && prefix.isEmpty() && suffix.isEmpty() ? 1 : 0;
            strings.add(randomAlphaOfLength(between(minLength, 10)));
        }

        final int fullDescriptionLength = collectionToDelimitedString(strings, delimiter, prefix, suffix).length();
        final int lastItemSize = prefix.length() + strings.get(count - 1).length() + suffix.length();
        final int truncatedLength = between(0, fullDescriptionLength - lastItemSize - 1);
        final StringBuilder stringBuilder = new StringBuilder();
        collectionToDelimitedStringWithLimit(strings, delimiter, prefix, suffix, truncatedLength, stringBuilder);
        final String truncatedDescription = stringBuilder.toString();

        assertThat(truncatedDescription, allOf(containsString("... (" + count + " in total,"), endsWith(" omitted)")));

        assertThat(
            truncatedDescription,
            truncatedDescription.length(),
            lessThanOrEqualTo(truncatedLength + (prefix + "0123456789" + suffix + delimiter + "... (999 in total, 999 omitted)").length())
        );
    }

    public void testCollectionToDelimitedStringWithLimitNoTruncation() {
        final String delimiter = randomFrom("", ",", ", ", "/");
        final String prefix = randomFrom("", "[");
        final String suffix = randomFrom("", "]");

        final int count = between(1, 100);
        final List<String> strings = new ArrayList<>(count);
        while (strings.size() < count) {
            strings.add(randomAlphaOfLength(between(0, 10)));
        }

        final String fullDescription = collectionToDelimitedString(strings, delimiter, prefix, suffix);
        for (String string : strings) {
            assertThat(fullDescription, containsString(prefix + string + suffix));
        }

        final int lastItemSize = prefix.length() + strings.get(count - 1).length() + suffix.length();
        final int minLimit = fullDescription.length() - lastItemSize;
        final int limit = randomFrom(between(minLimit, fullDescription.length()), between(minLimit, Integer.MAX_VALUE), Integer.MAX_VALUE);

        final StringBuilder stringBuilder = new StringBuilder();
        collectionToDelimitedStringWithLimit(strings, delimiter, prefix, suffix, limit, stringBuilder);
        assertThat(stringBuilder.toString(), equalTo(fullDescription));
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
