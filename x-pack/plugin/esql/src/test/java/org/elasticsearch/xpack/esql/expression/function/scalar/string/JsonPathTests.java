/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link JsonPath} that verify path parsing at the segment level.
 * These tests exercise the path syntax directly without involving the JSON extraction pipeline.
 */
public class JsonPathTests extends ESTestCase {

    // --- Dot notation ---

    public void testSimpleKey() {
        assertSegments("name", key("name"));
    }

    public void testNestedDotNotation() {
        assertSegments("user.address.city", key("user"), key("address"), key("city"));
    }

    public void testTwoLevelDotNotation() {
        assertSegments("a.b", key("a"), key("b"));
    }

    // --- Array index bracket notation ---

    public void testArrayIndex() {
        assertSegments("items[0]", key("items"), index(0));
    }

    public void testArrayIndexNested() {
        assertSegments("orders[1].item", key("orders"), index(1), key("item"));
    }

    public void testMultipleArrayIndices() {
        assertSegments("a[0][1]", key("a"), index(0), index(1));
    }

    public void testArrayIndexInMiddle() {
        assertSegments("a[0].b.c", key("a"), index(0), key("b"), key("c"));
    }

    // --- $ prefix ---

    public void testDollarDotPrefix() {
        assertSegments("$.name", key("name"));
    }

    public void testDollarDotNestedPath() {
        assertSegments("$.user.address.city", key("user"), key("address"), key("city"));
    }

    public void testDollarDotArrayIndex() {
        assertSegments("$.tags[0]", key("tags"), index(0));
    }

    public void testDollarDotMixedNesting() {
        assertSegments("$.orders[1].id", key("orders"), index(1), key("id"));
    }

    // --- Root accessor (empty segments) ---

    public void testBareDollar() {
        assertRootPath("$");
    }

    public void testDollarDotAlone() {
        assertRootPath("$.");
    }

    public void testEmptyString() {
        assertRootPath("");
    }

    // --- $[ prefix ---

    public void testDollarBracketArrayIndex() {
        assertSegments("$[1]", index(1));
    }

    public void testDollarBracketQuotedKey() {
        assertSegments("$['user.name']", key("user.name"));
    }

    public void testDollarBracketNestedPath() {
        assertSegments("$['a'].b", key("a"), key("b"));
    }

    // --- Bare leading bracket ---

    public void testBareLeadingBracketArrayIndex() {
        assertSegments("[0]", index(0));
    }

    public void testBareLeadingBracketQuotedKey() {
        assertSegments("['name']", key("name"));
    }

    // --- Quoted bracket notation for named keys ---

    public void testSingleQuotedKey() {
        assertSegments("['name']", key("name"));
    }

    public void testDoubleQuotedKey() {
        assertSegments("[\"name\"]", key("name"));
    }

    public void testQuotedKeyWithDot() {
        assertSegments("['user.name']", key("user.name"));
    }

    public void testQuotedKeyWithSpace() {
        assertSegments("['first name']", key("first name"));
    }

    public void testQuotedKeyWithBrackets() {
        assertSegments("['items[0]']", key("items[0]"));
    }

    public void testEmptyQuotedKeySingleQuote() {
        assertSegments("['']", key(""));
    }

    public void testEmptyQuotedKeyDoubleQuote() {
        assertSegments("[\"\"]", key(""));
    }

    public void testQuotedKeyNested() {
        assertSegments("a['b.c']", key("a"), key("b.c"));
    }

    public void testMixedDotAndBracketNotation() {
        assertSegments("store['user.name']", key("store"), key("user.name"));
    }

    public void testConsecutiveBracketNotation() {
        assertSegments("a['b.c']['d']", key("a"), key("b.c"), key("d"));
    }

    public void testBracketNotationAfterArrayIndex() {
        assertSegments("arr[0]['a.b']", key("arr"), index(0), key("a.b"));
    }

    public void testComplexMixedPath() {
        assertSegments("$.store['items'][0].name", key("store"), key("items"), index(0), key("name"));
    }

    // --- Dot / bracket equivalence (RFC 9535) ---

    public void testDotAndSingleQuotedBracketAreEquivalent() {
        assertThat(JsonPath.parse("a.b").segments(), equalTo(JsonPath.parse("a['b']").segments()));
    }

    public void testDotAndDoubleQuotedBracketAreEquivalent() {
        assertThat(JsonPath.parse("a.b").segments(), equalTo(JsonPath.parse("a[\"b\"]").segments()));
    }

    public void testNestedDotAndBracketAreEquivalent() {
        assertThat(JsonPath.parse("a.b.c").segments(), equalTo(JsonPath.parse("a['b']['c']").segments()));
    }

    public void testRandomDotAndBracketEquivalence() {
        for (int i = 0; i < 100; i++) {
            String k1 = randomAlphaOfLengthBetween(1, 10);
            String k2 = randomAlphaOfLengthBetween(1, 10);
            String dotPath = k1 + "." + k2;
            String bracketPath = k1 + "['" + k2 + "']";
            assertThat(
                "dot path [" + dotPath + "] and bracket path [" + bracketPath + "] should produce the same segments",
                JsonPath.parse(dotPath).segments(),
                equalTo(JsonPath.parse(bracketPath).segments())
            );
        }
    }

    // --- Randomized tests ---

    public void testRandomRoundTripDotAndBracketNotation() {
        for (int iter = 0; iter < 100; iter++) {
            List<JsonPath.Segment> expectedSegments = new ArrayList<>();
            StringBuilder pathBuilder = new StringBuilder();
            int segmentCount = randomIntBetween(1, 6);

            for (int i = 0; i < segmentCount; i++) {
                if (randomBoolean()) {
                    // Key segment — use dot notation
                    String name = randomAlphaOfLengthBetween(1, 10);
                    if (i > 0) {
                        pathBuilder.append('.');
                    }
                    pathBuilder.append(name);
                    expectedSegments.add(key(name));
                } else {
                    // Index segment — use bracket notation
                    int idx = randomIntBetween(0, 999);
                    pathBuilder.append('[').append(idx).append(']');
                    expectedSegments.add(index(idx));
                }
            }

            String path = pathBuilder.toString();
            JsonPath parsed = JsonPath.parse(path);
            assertThat("path [" + path + "]", parsed.segments(), equalTo(expectedSegments));
        }
    }

    public void testRandomRoundTripQuotedBracketNotation() {
        for (int iter = 0; iter < 100; iter++) {
            List<JsonPath.Segment> expectedSegments = new ArrayList<>();
            StringBuilder pathBuilder = new StringBuilder();
            int segmentCount = randomIntBetween(1, 6);

            for (int i = 0; i < segmentCount; i++) {
                if (randomBoolean()) {
                    // Key segment via quoted bracket notation (randomly single or double quoted)
                    String name = randomAlphaOfLengthBetween(1, 10);
                    char quote = randomBoolean() ? '\'' : '"';
                    pathBuilder.append('[').append(quote).append(name).append(quote).append(']');
                    expectedSegments.add(key(name));
                } else {
                    // Index segment
                    int idx = randomIntBetween(0, 999);
                    pathBuilder.append('[').append(idx).append(']');
                    expectedSegments.add(index(idx));
                }
            }

            String path = pathBuilder.toString();
            JsonPath parsed = JsonPath.parse(path);
            assertThat("path [" + path + "]", parsed.segments(), equalTo(expectedSegments));
        }
    }

    public void testRandomDollarPrefix() {
        for (int iter = 0; iter < 100; iter++) {
            // Build a simple path first (at least one key segment)
            String name = randomAlphaOfLengthBetween(1, 10);
            List<JsonPath.Segment> expectedSegments = List.of(key(name));

            // Randomly choose a $ prefix form
            String path = switch (randomIntBetween(0, 2)) {
                case 0 -> name;             // no prefix
                case 1 -> "$." + name;      // "$." prefix
                case 2 -> "$['" + name + "']"; // "$[" prefix
                default -> throw new AssertionError();
            };

            JsonPath parsed = JsonPath.parse(path);
            assertThat("path [" + path + "]", parsed.segments(), equalTo(expectedSegments));
        }
    }

    public void testRandomQuotedKeysWithSpecialCharacters() {
        // Characters that require bracket notation because they are special in dot notation
        String specialChars = ".[] '\"\\";
        for (int iter = 0; iter < 100; iter++) {
            // Build a key that contains at least one special character
            StringBuilder keyBuilder = new StringBuilder();
            int keyLength = randomIntBetween(1, 10);
            for (int i = 0; i < keyLength; i++) {
                if (randomBoolean()) {
                    keyBuilder.append(specialChars.charAt(randomIntBetween(0, specialChars.length() - 1)));
                } else {
                    keyBuilder.append(randomAlphaOfLength(1));
                }
            }
            String rawKey = keyBuilder.toString();

            // Escape the key for single-quoted bracket notation
            String escapedKey = rawKey.replace("\\", "\\\\").replace("'", "\\'");
            String path = "['" + escapedKey + "']";

            JsonPath parsed = JsonPath.parse(path);
            assertThat("path [" + path + "] for key [" + rawKey + "]", parsed.segments(), equalTo(List.of(key(rawKey))));
        }
    }

    public void testRandomEscapeSequences() {
        for (int iter = 0; iter < 100; iter++) {
            // Build a key that contains quotes and backslashes
            StringBuilder rawKeyBuilder = new StringBuilder();
            int keyLength = randomIntBetween(1, 8);
            for (int i = 0; i < keyLength; i++) {
                switch (randomIntBetween(0, 3)) {
                    case 0 -> rawKeyBuilder.append('\'');
                    case 1 -> rawKeyBuilder.append('"');
                    case 2 -> rawKeyBuilder.append('\\');
                    default -> rawKeyBuilder.append(randomAlphaOfLength(1));
                }
            }
            String rawKey = rawKeyBuilder.toString();

            // Test with single-quoted bracket notation
            String singleEscaped = rawKey.replace("\\", "\\\\").replace("'", "\\'");
            String singlePath = "['" + singleEscaped + "']";
            JsonPath singleParsed = JsonPath.parse(singlePath);
            assertThat("single-quoted path [" + singlePath + "]", singleParsed.segments(), equalTo(List.of(key(rawKey))));

            // Test with double-quoted bracket notation
            String doubleEscaped = rawKey.replace("\\", "\\\\").replace("\"", "\\\"");
            String doublePath = "[\"" + doubleEscaped + "\"]";
            JsonPath doubleParsed = JsonPath.parse(doublePath);
            assertThat("double-quoted path [" + doublePath + "]", doubleParsed.segments(), equalTo(List.of(key(rawKey))));
        }
    }

    public void testRandomWhitespaceInsideBrackets() {
        for (int iter = 0; iter < 100; iter++) {
            String ws = randomBlankSpace();

            if (randomBoolean()) {
                // Array index with random whitespace
                int idx = randomIntBetween(0, 999);
                String path = "a[" + ws + idx + ws + "]";
                JsonPath parsed = JsonPath.parse(path);
                assertThat("path [" + path + "]", parsed.segments(), equalTo(List.of(key("a"), index(idx))));
            } else {
                // Quoted key with random whitespace
                String name = randomAlphaOfLengthBetween(1, 10);
                String path = "a[" + ws + "'" + name + "'" + ws + "]";
                JsonPath parsed = JsonPath.parse(path);
                assertThat("path [" + path + "]", parsed.segments(), equalTo(List.of(key("a"), key(name))));
            }
        }
    }

    // --- Escaped quotes in bracket notation ---

    public void testEscapedSingleQuoteInKey() {
        assertSegments("['it\\'s']", key("it's"));
    }

    public void testEscapedDoubleQuoteInKey() {
        assertSegments("[\"say \\\"hi\\\"\"]", key("say \"hi\""));
    }

    public void testEscapedBackslashInKey() {
        assertSegments("['a\\\\b']", key("a\\b"));
    }

    // --- Error cases ---

    public void testConsecutiveDots() {
        assertInvalidPathMessage("a..b", "Invalid JSON path [a..b]: consecutive dots at position 1");
    }

    public void testTrailingDot() {
        assertInvalidPathMessage("a.", "Invalid JSON path [a.]: path cannot end with a dot at position 1");
    }

    public void testLeadingDot() {
        assertInvalidPathMessage(".a", "Invalid JSON path [.a]: path cannot start with a dot at position 0");
    }

    public void testEmptyBrackets() {
        assertInvalidPathMessage("a[]", "Invalid JSON path [a[]]: empty brackets at position 1");
    }

    public void testUnterminatedQuotedKey() {
        assertInvalidPathMessage("['unterminated", "Invalid JSON path [['unterminated]: unterminated quoted key at position 0");
    }

    public void testMissingClosingBracketAfterQuote() {
        assertInvalidPathMessage("['key'x", "Invalid JSON path [['key'x]: expected closing bracket after quoted key at position 0");
    }

    public void testNegativeArrayIndex() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JsonPath.parse("a[-1]"));
        assertEquals("array index out of bounds", e.getMessage());
    }

    public void testNonNumericArrayIndex() {
        assertInvalidPathMessage("a[abc]", "Invalid JSON path [a[abc]]: expected integer array index, got [abc] at position 1");
    }

    public void testDollarNotFollowedByDotOrBracket() {
        assertInvalidPathMessage("$name", "Invalid JSON path [$name]: expected [.] or [[] after [$] at position 1");
    }

    public void testUnterminatedBracketArrayIndex() {
        assertInvalidPathMessage("a[0", "Invalid JSON path [a[0]: missing closing bracket for array index at position 1");
    }

    public void testArrayIndexOverflow() {
        assertInvalidPathMessage(
            "a[99999999999]",
            "Invalid JSON path [a[99999999999]]: expected integer array index, got [99999999999] at position 1"
        );
    }

    public void testLeadingZeroInArrayIndex() {
        assertInvalidPathMessage("a[01]", "Invalid JSON path [a[01]]: leading zeros are not allowed in array index [01] at position 1");
    }

    public void testLeadingZerosInArrayIndex() {
        assertInvalidPathMessage("a[007]", "Invalid JSON path [a[007]]: leading zeros are not allowed in array index [007] at position 1");
    }

    public void testWhitespaceInArrayIndex() {
        assertSegments("a[ 0 ]", key("a"), index(0));
    }

    public void testStrayCloseBracket() {
        assertInvalidPathMessage("a]b", "Invalid JSON path [a]b]: expected [.] or [[] before []] at position 1");
    }

    public void testWhitespaceInQuotedKey() {
        assertSegments("a[ 'b' ]", key("a"), key("b"));
    }

    // --- Error position offset ---

    public void testErrorPositionOffsetShiftsPosition() {
        // "a..b" has a consecutive-dots error at position 1 (zero-based).
        // With an offset of 10, the reported position should be 11.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JsonPath.parse("a..b", 10));
        assertThat(e.getMessage(), equalTo("Invalid JSON path [a..b]: consecutive dots at position 11"));
    }

    public void testErrorPositionOffsetAccountsForDollarDotPrefix() {
        // "$.a..b" → normalizePath strips "$." (2 chars), so "a..b" with baseOffset = 10 + 2 = 12.
        // The consecutive-dots error is at normalized position 1, so reported position = 12 + 1 = 13.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JsonPath.parse("$.a..b", 10));
        assertThat(e.getMessage(), equalTo("Invalid JSON path [$.a..b]: consecutive dots at position 13"));
    }

    public void testErrorPositionOffsetZeroMatchesDefault() {
        // parse(path) and parse(path, 0) should produce identical error messages.
        String path = ".a";
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> JsonPath.parse(path));
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> JsonPath.parse(path, 0));
        assertThat(e1.getMessage(), equalTo(e2.getMessage()));
    }

    // --- Helper methods ---

    private static JsonPath.Segment key(String name) {
        return new JsonPath.Segment.Key(name);
    }

    private static JsonPath.Segment index(int i) {
        return new JsonPath.Segment.Index(i);
    }

    private static void assertSegments(String path, JsonPath.Segment... expectedSegments) {
        JsonPath parsed = JsonPath.parse(path);
        assertThat(parsed.segments(), equalTo(List.of(expectedSegments)));
    }

    private static void assertRootPath(String path) {
        JsonPath parsed = JsonPath.parse(path);
        assertThat(parsed.segments(), empty());
    }

    private static void assertInvalidPathMessage(String path, String expectedMessage) {
        expectThrows(IllegalArgumentException.class, equalTo(expectedMessage), () -> JsonPath.parse(path));
    }

    /** Generates a random string of 1-4 RFC 9535 blank characters (space, tab, newline, carriage return). */
    private String randomBlankSpace() {
        char[] blanks = { ' ', '\t', '\n', '\r' };
        int length = randomIntBetween(1, 4);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(blanks[randomIntBetween(0, blanks.length - 1)]);
        }
        return sb.toString();
    }
}
