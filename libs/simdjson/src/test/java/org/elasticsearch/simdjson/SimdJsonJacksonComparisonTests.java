/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Comparison tests that parse the same JSON with both Jackson (via {@link XContentParser}) and
 * {@link SimdJsonDirectWalker}, then assert that both produce identical event streams. This
 * catches subtle differences in number precision, string encoding, or structural interpretation.
 */
public class SimdJsonJacksonComparisonTests extends SimdJsonTestCase {

    // ---- Jackson/XContent walker (mirrors JsonDocumentEventComparison event format) ----

    // Walk a root object and record Jackson handler-style events.
    private List<String> walkWithJackson(String json) throws IOException {
        return walkWithJackson(json, false);
    }

    private List<String> walkWithJackson(String json, boolean allowDuplicateKeys) throws IOException {
        List<String> events = new ArrayList<>();
        byte[] bytes = json.getBytes(UTF_8);
        try (XContentParser p = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
            if (allowDuplicateKeys) {
                p.allowDuplicateKeys(true);
            }
            XContentParser.Token token = p.nextToken();
            assert token == XContentParser.Token.START_OBJECT : "expected START_OBJECT but got " + token;
            walkJacksonObject(p, events);
        }
        return events;
    }

    // Walk object fields inside an array element or nested structure.
    private void walkJacksonObject(XContentParser p, List<String> events) throws IOException {
        while (true) {
            XContentParser.Token token = p.nextToken();
            if (token == XContentParser.Token.END_OBJECT) return;
            assert token == XContentParser.Token.FIELD_NAME;
            String fieldName = p.currentName();

            token = p.nextToken();
            switch (token) {
                case START_OBJECT -> {
                    XContentParser.Token next = p.nextToken();
                    if (next == XContentParser.Token.END_OBJECT) {
                        events.add("startObject(" + fieldName + ")");
                        events.add("endObject()");
                    } else {
                        events.add("startObject(" + fieldName + ")");
                        walkJacksonObjectBody(p, next, events);
                        events.add("endObject()");
                    }
                }
                case START_ARRAY -> {
                    events.add("startArray(" + fieldName + ")");
                    walkJacksonArray(p, events);
                    events.add("endArray()");
                }
                case VALUE_STRING -> events.add("string(" + fieldName + "=" + p.text() + ")");
                case VALUE_NUMBER -> addJacksonNumberEvent(p, fieldName, events);
                case VALUE_BOOLEAN -> events.add("bool(" + fieldName + "=" + p.booleanValue() + ")");
                case VALUE_NULL -> events.add("null(" + fieldName + ")");
                default -> throw new AssertionError("Unexpected token: " + token);
            }
        }
    }

    // Continue walking after the first field name of a non-empty object.
    private void walkJacksonObjectBody(XContentParser p, XContentParser.Token current, List<String> events) throws IOException {
        XContentParser.Token token = current;
        while (true) {
            if (token == XContentParser.Token.END_OBJECT) return;
            assert token == XContentParser.Token.FIELD_NAME;
            String fieldName = p.currentName();

            token = p.nextToken();
            switch (token) {
                case START_OBJECT -> {
                    XContentParser.Token next = p.nextToken();
                    if (next == XContentParser.Token.END_OBJECT) {
                        events.add("startObject(" + fieldName + ")");
                        events.add("endObject()");
                    } else {
                        events.add("startObject(" + fieldName + ")");
                        walkJacksonObjectBody(p, next, events);
                        events.add("endObject()");
                    }
                }
                case START_ARRAY -> {
                    events.add("startArray(" + fieldName + ")");
                    walkJacksonArray(p, events);
                    events.add("endArray()");
                }
                case VALUE_STRING -> events.add("string(" + fieldName + "=" + p.text() + ")");
                case VALUE_NUMBER -> addJacksonNumberEvent(p, fieldName, events);
                case VALUE_BOOLEAN -> events.add("bool(" + fieldName + "=" + p.booleanValue() + ")");
                case VALUE_NULL -> events.add("null(" + fieldName + ")");
                default -> throw new AssertionError("Unexpected token: " + token);
            }
            token = p.nextToken();
        }
    }

    private void walkJacksonArray(XContentParser p, List<String> events) throws IOException {
        while (true) {
            XContentParser.Token token = p.nextToken();
            if (token == XContentParser.Token.END_ARRAY) return;

            switch (token) {
                case VALUE_STRING -> events.add("arrayElemString(" + p.text() + ")");
                case VALUE_NUMBER -> addJacksonArrayNumberEvent(p, events);
                case VALUE_BOOLEAN -> events.add("arrayElemBoolean(" + p.booleanValue() + ")");
                case VALUE_NULL -> events.add("arrayElemNull()");
                case START_OBJECT -> {
                    events.add("arrayElemStartObject()");
                    walkJacksonObject(p, events);
                    events.add("arrayElemEndObject()");
                }
                case START_ARRAY -> {
                    events.add("arrayElemStartArray()");
                    walkJacksonArray(p, events);
                    events.add("arrayElemEndArray()");
                }
                default -> throw new AssertionError("Unexpected token in array: " + token);
            }
        }
    }

    // Classify Jackson numbers the same way simdjson does (long / bigInteger / double + fitsFloat).
    private static void addJacksonNumberEvent(XContentParser p, String fieldName, List<String> events) throws IOException {
        XContentParser.NumberType numType = p.numberType();
        if (numType == XContentParser.NumberType.INT || numType == XContentParser.NumberType.LONG) {
            events.add("long(" + fieldName + "=" + p.longValue() + ",fitsInt=" + (numType == XContentParser.NumberType.INT) + ")");
        } else if (numType == XContentParser.NumberType.BIG_INTEGER) {
            events.add("bigInteger(" + fieldName + "=" + p.text() + ")");
        } else {
            double val = p.doubleValue();
            float fval = (float) val;
            boolean fitsFloat = (double) fval == val;
            events.add("double(" + fieldName + "=" + val + ",fitsFloat=" + fitsFloat + ")");
        }
    }

    private static void addJacksonArrayNumberEvent(XContentParser p, List<String> events) throws IOException {
        XContentParser.NumberType numType = p.numberType();
        if (numType == XContentParser.NumberType.INT || numType == XContentParser.NumberType.LONG) {
            events.add("arrayElemLong(" + p.longValue() + ",fitsInt=" + (numType == XContentParser.NumberType.INT) + ")");
        } else if (numType == XContentParser.NumberType.BIG_INTEGER) {
            events.add("arrayElemBigInteger(" + p.text() + ")");
        } else {
            double val = p.doubleValue();
            float fval = (float) val;
            boolean fitsFloat = (double) fval == val;
            events.add("arrayElemDouble(" + val + ",fitsFloat=" + fitsFloat + ")");
        }
    }

    // ---- Comparison helpers ----

    private void assertParsersAgree(String json) throws IOException {
        assertParsersAgree(json, false);
    }

    // Both backends must emit identical event streams for the same JSON input.
    private void assertParsersAgree(String json, boolean allowDuplicateKeys) throws IOException {
        List<String> jacksonEvents = walkWithJackson(json, allowDuplicateKeys);
        List<String> simdEvents = walkJson(json, true);
        assertEquals("Event streams differ for: " + json, jacksonEvents, simdEvents);
    }

    // ---- Basic structure and scalar types ----

    public void testEmptyObject() throws IOException {
        assertParsersAgree("{}");
    }

    public void testSingleStringField() throws IOException {
        assertParsersAgree("{\"name\":\"hello\"}");
    }

    public void testSingleIntField() throws IOException {
        assertParsersAgree("{\"count\":42}");
    }

    public void testSingleLongField() throws IOException {
        assertParsersAgree("{\"big\":9999999999}");
    }

    public void testSingleDoubleField() throws IOException {
        assertParsersAgree("{\"pi\":3.14}");
    }

    public void testBooleanFields() throws IOException {
        assertParsersAgree("{\"t\":true,\"f\":false}");
    }

    public void testNullField() throws IOException {
        assertParsersAgree("{\"x\":null}");
    }

    // Mixed scalars in one object.
    public void testMultipleFieldTypes() throws IOException {
        assertParsersAgree("{\"s\":\"val\",\"i\":42,\"d\":1.5,\"b\":true,\"n\":null}");
    }

    public void testNestedObject() throws IOException {
        assertParsersAgree("{\"outer\":{\"inner\":\"deep\"}}");
    }

    // Empty {} must emit startObject/endObject, not a separate emptyObject event.
    public void testEmptyNestedObject() throws IOException {
        assertParsersAgree("{\"empty\":{}}");
    }

    public void testDeeplyNested() throws IOException {
        assertParsersAgree("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":1}}}}}");
    }

    public void testIntArray() throws IOException {
        assertParsersAgree("{\"arr\":[1,2,3,4,5]}");
    }

    public void testStringArray() throws IOException {
        assertParsersAgree("{\"arr\":[\"a\",\"b\",\"c\"]}");
    }

    public void testMixedArray() throws IOException {
        assertParsersAgree("{\"arr\":[1,\"two\",true,null,3.14]}");
    }

    public void testEmptyArray() throws IOException {
        assertParsersAgree("{\"arr\":[]}");
    }

    public void testNestedArrays() throws IOException {
        assertParsersAgree("{\"arr\":[[1,2],[3,4]]}");
    }

    public void testObjectsInArray() throws IOException {
        assertParsersAgree("{\"arr\":[{\"x\":1},{\"y\":2}]}");
    }

    public void testEmptyObjectInArray() throws IOException {
        assertParsersAgree("{\"arr\":[{}]}");
    }

    // ---- String escapes ----

    public void testEscapedString() throws IOException {
        assertParsersAgree("{\"msg\":\"line1\\nline2\"}");
    }

    public void testEscapedQuote() throws IOException {
        assertParsersAgree("{\"msg\":\"say \\\"hi\\\"\"}");
    }

    public void testEscapedBackslash() throws IOException {
        assertParsersAgree("{\"path\":\"C:\\\\Users\\\\test\"}");
    }

    public void testUnicodeEscape() throws IOException {
        assertParsersAgree("{\"char\":\"\\u0041\"}");
    }

    // Invalid \\u hex: both parsers reject the document.
    public void testInvalidUnicodeEscape() {
        List<String> invalidDocuments = List.of("""
            {"x":"\\u00G0"}""", """
            {"x":"\\u00"}""", """
            {"x":"\\u0"}""", """
            {"x":"\\u"}""");
        for (String json : invalidDocuments) {
            expectThrows(XContentParseException.class, () -> walkWithJackson(json, false));
            expectThrows(JsonParsingException.class, () -> walkJson(json, true));
        }
    }

    // Leading zeroes in the integer part (e.g. "007", "00", "-01") are invalid per RFC 8259;
    // both parsers reject them.
    public void testLeadingZeroRejectedByBothParsers() {
        List<String> invalidDocuments = List.of(
            "{\"n\":00}",
            "{\"n\":01}",
            "{\"n\":-00}",
            "{\"n\":-01}",
            "{\"n\":007}",
            "{\"n\":00.5}",
            "{\"n\":01e5}",
            "{\"a\":[01]}",
            "{\"a\":[12, 01]}"
        );
        for (String json : invalidDocuments) {
            XContentParseException jacksonEx = expectThrows(XContentParseException.class, () -> walkWithJackson(json));
            JsonParsingException simdEx = expectThrows(JsonParsingException.class, () -> walkJson(json));
            assertTrue("Jackson message: " + jacksonEx.getMessage(), jacksonEx.getMessage().contains("Leading zeroes not allowed"));
            assertTrue("simdjson message: " + simdEx.getMessage(), simdEx.getMessage().contains("Leading zeroes not allowed"));
        }
    }

    // Malformed documents.
    public void testKnownInvalidDocumentsRejectedByBothParsers() {
        int[] paddingsAndOffsets = { 0, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65 };
        for (String json : SimdJsonTestDocuments.invalidDocumentsRejectedByBothParsers()) {
            expectThrows(XContentParseException.class, () -> walkWithJackson(json));
            expectThrows(JsonParsingException.class, () -> walkJson(json));

            for (int padding : paddingsAndOffsets) {
                expectThrows(JsonParsingException.class, () -> walkAndRecord(json, padding));
            }
            for (int offset : paddingsAndOffsets) {
                expectThrows(JsonParsingException.class, () -> walkAndRecordAtOffset(json, offset));
            }
        }
    }

    // Both parsers require at least one digit after '.' and after 'e'/'E', and phrase the
    // rejection identically enough that the message can be compared directly (unlike
    // testTrailingGarbageOrMissingDigitsRejectedByBothParsers below).
    public void testEmptyFractionOrExponentRejectedByBothParsers() {
        record Case(String json, String reason) {}
        List<Case> cases = List.of(
            new Case("{\"n\":1.}", "Decimal point not followed by a digit"),
            new Case("{\"n\":1.e5}", "Decimal point not followed by a digit"),
            new Case("{\"a\":[1.]}", "Decimal point not followed by a digit"),
            new Case("{\"n\":1e}", "Exponent indicator not followed by a digit"),
            new Case("{\"n\":1e+}", "Exponent indicator not followed by a digit"),
            new Case("{\"n\":1e-}", "Exponent indicator not followed by a digit"),
            new Case("{\"a\":[1e]}", "Exponent indicator not followed by a digit")
        );
        for (Case c : cases) {
            XContentParseException jacksonEx = expectThrows(XContentParseException.class, () -> walkWithJackson(c.json()));
            JsonParsingException simdEx = expectThrows(JsonParsingException.class, () -> walkJson(c.json()));
            assertTrue("Jackson message: " + jacksonEx.getMessage(), jacksonEx.getMessage().contains(c.reason()));
            assertTrue("simdjson message: " + simdEx.getMessage(), simdEx.getMessage().contains(c.reason()));
        }
    }

    // Both parsers reject a number with no integer-part digits, and any trailing content after
    // a number that isn't itself a structural character or whitespace - but with unrelated
    // message wording (e.g. Jackson reports these relative to what it expected next, like a
    // comma), so only rejection-by-both is checked here, not the message text.
    public void testTrailingGarbageOrMissingDigitsRejectedByBothParsers() {
        List<String> invalidDocuments = List.of(
            "{\"n\":-}",
            "{\"n\":-.5}",
            "{\"n\":-e5}",
            "{\"a\":[-]}",
            "{\"n\":1.2.3}",
            "{\"n\":1e5e6}",
            "{\"n\":1-2}",
            "{\"n\":12-3}",
            "{\"n\":1foo}",
            "{\"a\":[1.2.3]}",
            "{\"a\":[1foo]}"
        );
        for (String json : invalidDocuments) {
            expectThrows(XContentParseException.class, () -> walkWithJackson(json));
            expectThrows(JsonParsingException.class, () -> walkJson(json));
        }
    }

    // Both parsers prefix leading-zero messages with a "[line:column]" location (see
    // SimdJsonDirectWalker#computeLineAndColumn).
    public void testLineNumberMatchesBothParsers() {
        List<String> documents = List.of(
            "{\"n\":00}",                        // no newlines
            "{\n\"n\":00}",                      // one newline, right before the field
            "{\n\n\n\"n\":00}",                  // several consecutive newlines
            "{\n\"a\":1,\n\"n\":00\n}",          // newlines scattered across multiple tokens
            "{\n\"a\":[\n1,\n01\n]\n}",          // leading zero inside an array element
            "{\"a\":{\n\"b\":1\n},\n\"n\":00}"   // leading zero after a nested object
        );
        for (String json : documents) {
            assertLineNumbersMatch(json);
        }
    }

    // Same property as above, but with a randomized amount and placement of whitespace.
    public void testLineNumberMatchesBothParsersRandomized() {
        int iterations = randomIntBetween(20, 100);
        for (int i = 0; i < iterations; i++) {
            String json = withRandomWhitespace("{", "\"a\"", ":", "1", ",", "\"n\"", ":", "00", "}");
            assertLineNumbersMatch(json);
        }
    }

    // Joins tokens with a random run of insignificant JSON whitespace before, between, and
    // after each - JSON permits whitespace in all of these positions, so this should never
    // change parsing outcome, only line/column tracking.
    private String withRandomWhitespace(String... tokens) {
        StringBuilder sb = new StringBuilder(randomWhitespace());
        for (String token : tokens) {
            sb.append(token).append(randomWhitespace());
        }
        return sb.toString();
    }

    // A random run of insignificant JSON whitespace - space, tab, "\n", "\r", or "\r\n" -
    // including none. All 4 RFC 8259 whitespace characters are covered; "\r" is emitted as
    // either a lone CR or part of a CRLF pair, since both parsers treat each as one line break.
    private static final String[] WHITESPACE_UNITS = { " ", "\t", "\n", "\r", "\r\n" };

    private String randomWhitespace() {
        StringBuilder sb = new StringBuilder();
        int n = randomIntBetween(0, 6);
        for (int i = 0; i < n; i++) {
            sb.append(WHITESPACE_UNITS[randomIntBetween(0, WHITESPACE_UNITS.length - 1)]);
        }
        return sb.toString();
    }

    private void assertLineNumbersMatch(String json) {
        int jacksonLine = parseLocation(expectThrows(XContentParseException.class, () -> walkWithJackson(json)).getMessage())[0];
        int simdLine = parseLocation(expectThrows(JsonParsingException.class, () -> walkJson(json)).getMessage())[0];
        assertEquals("json=" + json, jacksonLine, simdLine);
    }

    // Absolute columns aren't compared directly: the parsers anchor to different bytes, and
    // the offset isn't even constant ({@code "00"} vs {@code "-00"}). Instead, this checks via
    // delta from each parser's own baseline that both advance by UTF-8 bytes, not code points.
    public void testColumnCountsUtf8BytesNotCodePointsInBothParsers() {
        int jacksonBaseline = columnFor("", true);
        int simdBaseline = columnFor("", false);

        // character(s), expected UTF-8 byte length
        Object[][] cases = {
            { "x", 1 },              // ASCII
            { "xyz", 3 },            // several ASCII characters
            { "\u00e9", 2 },         // é - 2-byte
            { "\u20ac", 3 },         // € - 3-byte
            { "\ud83d\ude00", 4 },   // 😀 - 4-byte, encoded as a surrogate pair in Java
            { "a\u00e9\u20ac\ud83d\ude00z", 1 + 2 + 3 + 4 + 1 } // mixed byte widths
        };
        for (Object[] c : cases) {
            String content = (String) c[0];
            int expectedBytes = (Integer) c[1];
            assertEquals("jackson, content=" + content, expectedBytes, columnFor(content, true) - jacksonBaseline);
            assertEquals("simdjson, content=" + content, expectedBytes, columnFor(content, false) - simdBaseline);
        }
    }

    // Property-based version of the above: for randomly generated content mixing ASCII and
    // multi-byte code points, both parsers' columns must advance by exactly the content's
    // UTF-8 byte length, never by its code point count.
    public void testColumnCountsUtf8BytesNotCodePointsRandomized() {
        int jacksonBaseline = columnFor("", true);
        int simdBaseline = columnFor("", false);

        int iterations = randomIntBetween(20, 100);
        for (int i = 0; i < iterations; i++) {
            String content = randomSafeUnicodeString(randomIntBetween(1, 12));
            int expectedBytes = content.getBytes(UTF_8).length;

            assertEquals("jackson, content=" + content, expectedBytes, columnFor(content, true) - jacksonBaseline);
            assertEquals("simdjson, content=" + content, expectedBytes, columnFor(content, false) - simdBaseline);
        }
    }

    // Code points spanning every UTF-8 byte width (1 through 4), all safe to embed unescaped
    // in a JSON string literal (no quote, backslash, or control characters).
    private static final int[] SAFE_CODE_POINTS = {
        'a',
        'Z',
        '5', // 1-byte
        0x00e9,
        0x00f1,
        0x00df, // 2-byte: é, ñ, ß
        0x20ac,
        0x4e2d,
        0x0439, // 3-byte: €, 中, й
        0x1f600,
        0x1f4a9,
        0x10000 // 4-byte: 😀, 💩, and the first supplementary-plane code point
    };

    private String randomSafeUnicodeString(int codePointCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePointCount; i++) {
            sb.appendCodePoint(SAFE_CODE_POINTS[randomIntBetween(0, SAFE_CODE_POINTS.length - 1)]);
        }
        return sb.toString();
    }

    // The reported column of the leading-zero error in {"a":"<content>","n":00}, for either
    // Jackson or simdjson.
    private int columnFor(String content, boolean useJackson) {
        String json = "{\"a\":\"" + content + "\",\"n\":00}";
        String message = useJackson
            ? expectThrows(XContentParseException.class, () -> walkWithJackson(json)).getMessage()
            : expectThrows(JsonParsingException.class, () -> walkJson(json)).getMessage();
        return parseLocation(message)[1];
    }

    // Parses the leading "[line:column]" prefix shared by both exception message formats.
    private static int[] parseLocation(String message) {
        assertTrue("no [line:column] prefix: " + message, message.startsWith("["));
        int colon = message.indexOf(':');
        int close = message.indexOf(']');
        return new int[] { Integer.parseInt(message.substring(1, colon)), Integer.parseInt(message.substring(colon + 1, close)) };
    }

    // Shared with SimdJsonDirectWalkerTests via SimdJsonTestDocuments.
    public void testExactBufferLengthDocumentsMatchJackson() throws IOException {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            assertParsersAgree(json);
        }
    }

    // ---- Simple numeric sign and zero ----

    public void testNegativeNumber() throws IOException {
        assertParsersAgree("{\"n\":-42}");
    }

    public void testNegativeDouble() throws IOException {
        assertParsersAgree("{\"n\":-3.14}");
    }

    public void testScientificNotation() throws IOException {
        assertParsersAgree("{\"n\":1.5e10}");
    }

    public void testZero() throws IOException {
        assertParsersAgree("{\"z\":0}");
    }

    // Many fields in a flat object (field-name table stress).
    public void testLargeObject() throws IOException {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < 50; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"field").append(i).append("\":").append(i);
        }
        sb.append("}");
        assertParsersAgree(sb.toString());
    }

    // Realistic nested document with arrays and null.
    public void testComplexDocument() throws IOException {
        assertParsersAgree(
            "{\"user\":{\"name\":\"Alice\",\"age\":30,\"active\":true,"
                + "\"tags\":[\"admin\",\"user\"],\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"},"
                + "\"scores\":[95.5,87.3,92.1],\"metadata\":null}}"
        );
    }

    public void testArrayOfMixedObjects() throws IOException {
        assertParsersAgree("{\"items\":[{\"type\":\"a\",\"val\":1},{\"type\":\"b\",\"val\":2.5},{\"type\":\"c\",\"val\":null}]}");
    }

    // Insignificant whitespace must not change events.
    public void testWhitespace() throws IOException {
        assertParsersAgree("{ \"a\" : 1 , \"b\" : 2 }");
    }

    public void testNewlinesAndTabs() throws IOException {
        assertParsersAgree("{\n\t\"a\":\t1,\n\t\"b\":\t2\n}");
    }

    // ESCF allows duplicate keys; simdjson must match Jackson in that mode.
    public void testDuplicateKeysMatchJacksonWhenAllowed() throws IOException {
        assertParsersAgree("{\"k\":1,\"k\":2,\"k\":3}", true);
    }

    // ---- Integer and big-integer boundaries ----

    public void testIntegerBoundaryMaxInt() throws IOException {
        assertParsersAgree("{\"n\":" + Integer.MAX_VALUE + "}");
    }

    public void testIntegerBoundaryMinInt() throws IOException {
        assertParsersAgree("{\"n\":" + Integer.MIN_VALUE + "}");
    }

    public void testIntegerBoundaryMaxIntPlusOne() throws IOException {
        assertParsersAgree("{\"n\":" + ((long) Integer.MAX_VALUE + 1) + "}");
    }

    public void testIntegerBoundaryMinIntMinusOne() throws IOException {
        assertParsersAgree("{\"n\":" + ((long) Integer.MIN_VALUE - 1) + "}");
    }

    public void testIntegerBoundaryMaxLong() throws IOException {
        assertParsersAgree("{\"n\":" + Long.MAX_VALUE + "}");
    }

    public void testIntegerBoundaryMinLong() throws IOException {
        assertParsersAgree("{\"n\":" + Long.MIN_VALUE + "}");
    }

    public void testIntegerBeyondLongMax() throws IOException {
        assertParsersAgree("{\"n\":9223372036854775808}"); // Long.MAX_VALUE + 1
    }

    public void testIntegerBeyondLongMin() throws IOException {
        assertParsersAgree("{\"n\":-9223372036854775809}"); // Long.MIN_VALUE - 1
    }

    public void testVeryLargeBigInteger() throws IOException {
        assertParsersAgree("{\"n\":99999999999999999999999999999}");
    }

    public void testBigIntegerInArray() throws IOException {
        assertParsersAgree("{\"arr\":[1,9223372036854775808,-9223372036854775809]}");
    }

    // ---- Double range and IEEE-754 edge cases ----

    public void testDoubleMaxValue() throws IOException {
        assertParsersAgree("{\"n\":" + Double.MAX_VALUE + "}");
    }

    public void testDoubleMinPositiveValue() throws IOException {
        assertParsersAgree("{\"n\":" + Double.MIN_VALUE + "}");
    }

    public void testDoubleMinNormal() throws IOException {
        assertParsersAgree("{\"n\":" + Double.MIN_NORMAL + "}");
    }

    public void testDoubleNearOverflow() throws IOException {
        assertParsersAgree("{\"n\":1.7976931348623157e308}");
    }

    public void testDoubleNearUnderflow() throws IOException {
        assertParsersAgree("{\"n\":5e-324}");
    }

    public void testScientificNotationPositiveExponent() throws IOException {
        assertParsersAgree("{\"n\":1.23e5}");
    }

    public void testScientificNotationNegativeExponent() throws IOException {
        assertParsersAgree("{\"n\":1.23e-5}");
    }

    public void testScientificNotationLargeExponent() throws IOException {
        assertParsersAgree("{\"n\":1e308}");
    }

    public void testScientificNotationSmallExponent() throws IOException {
        assertParsersAgree("{\"n\":1e-308}");
    }

    public void testScientificNotationExplicitPositiveSign() throws IOException {
        assertParsersAgree("{\"n\":1.5e+10}");
    }

    public void testDoubleZero() throws IOException {
        assertParsersAgree("{\"n\":0.0}");
    }

    public void testNegativeZeroDouble() throws IOException {
        assertParsersAgree("{\"n\":-0.0}");
    }

    public void testNegativeZeroInt() throws IOException {
        assertParsersAgree("{\"n\":-0}");
    }

    // 0.1 has no exact IEEE-754 representation; both parsers must agree on the same double.
    public void testDoublePrecisionClassic() throws IOException {
        assertParsersAgree("{\"n\":0.1}");
    }

    public void testDoublePrecisionOneThird() throws IOException {
        assertParsersAgree("{\"n\":0.3333333333333333}");
    }

    // 1.5 is exactly representable as both float and double (fitsFloat=true).
    public void testDoubleFitsFloat() throws IOException {
        assertParsersAgree("{\"n\":1.5}");
    }

    // 1.1 is not exactly representable as float (fitsFloat=false).
    public void testDoubleDoesNotFitFloat() throws IOException {
        assertParsersAgree("{\"n\":1.1}");
    }

    public void testIntegerLeadingZeroInDouble() throws IOException {
        assertParsersAgree("{\"n\":0.5}");
    }

    // Exponent form forces double path even for integer-looking values.
    public void testLargeIntegerAsDouble() throws IOException {
        assertParsersAgree("{\"n\":1e19}");
    }

    public void testSmallIntegerValues() throws IOException {
        assertParsersAgree("{\"a\":0,\"b\":1,\"c\":-1,\"d\":127,\"e\":-128}");
    }

    public void testNumericArrayBoundaries() throws IOException {
        assertParsersAgree(
            "{\"arr\":[" + Integer.MAX_VALUE + "," + Integer.MIN_VALUE + "," + Long.MAX_VALUE + "," + Long.MIN_VALUE + ",3.14,1e100]}"
        );
    }

    public void testManyDecimalPlaces() throws IOException {
        assertParsersAgree("{\"n\":3.141592653589793238462643383279}");
    }

    // Lexical trailing zeros must not change the parsed double.
    public void testTrailingZerosInDecimal() throws IOException {
        assertParsersAgree("{\"n\":1.50000000000000}");
    }

    // ---- Scientific notation edge cases ----

    public void testScientificIntegerLikeZeroExponent() throws IOException {
        assertParsersAgree("{\"n\":1e0}");
    }

    // Integer-looking value written with exponent (double path, not long).
    public void testScientificIntegerLikePositiveExponent() throws IOException {
        assertParsersAgree("{\"n\":5e2}");
    }

    public void testScientificOverflowToInfinity() throws IOException {
        assertParsersAgree("{\"n\":1e309}");
    }

    public void testScientificUnderflowToZero() throws IOException {
        assertParsersAgree("{\"n\":1e-400}");
    }

    public void testScientificNegativeOverflow() throws IOException {
        assertParsersAgree("{\"n\":-1e309}");
    }

    public void testScientificManyMantissaDigits() throws IOException {
        assertParsersAgree("{\"n\":1.23456789012345678901234567890e10}");
    }

    public void testScientificVerySmallPositive() throws IOException {
        assertParsersAgree("{\"n\":2.2250738585072014e-308}");
    }

    public void testScientificNegativeSmallExponent() throws IOException {
        assertParsersAgree("{\"n\":-1.5e-3}");
    }

    public void testScientificCapitalE() throws IOException {
        assertParsersAgree("{\"n\":1.5E10}");
    }

    public void testScientificInArray() throws IOException {
        assertParsersAgree("{\"arr\":[1e0,5e2,1e308,1e-308,1e309,1e-400]}");
    }

    // Same numeric value in different lexical forms: Jackson and simdjson must agree field-by-field.
    public void testEquivalentNumericFormsSameParserAgreement() throws IOException {
        assertParsersAgree("{\"i\":500,\"d\":500.0,\"e\":5e2,\"z\":500e0}");
    }

    // Locks in long vs double classification per encoding (500 is long; 5e2/500.0 are double).
    public void testIntegerDecimalScientificClassification() throws IOException {
        assertNumericClassification("{\"n\":500}", "long(n=500,fitsInt=true)");
        assertNumericClassification("{\"n\":500.0}", "double(n=500.0,fitsFloat=true)");
        assertNumericClassification("{\"n\":5e2}", "double(n=500.0,fitsFloat=true)");
        assertNumericClassification("{\"n\":500e0}", "double(n=500.0,fitsFloat=true)");
    }

    private void assertNumericClassification(String json, String expectedEvent) throws IOException {
        List<String> jacksonEvents = walkWithJackson(json);
        List<String> simdEvents = walkJson(json, true);
        assertEquals("Unexpected Jackson events for " + json, List.of(expectedEvent), jacksonEvents);
        assertEquals("Jackson and simdjson differ for " + json, jacksonEvents, simdEvents);
    }

    // ---- Random fuzz (decimal doubles only; no explicit scientific notation) ----

    public void testRandomDocumentsMatchJackson() throws IOException {
        for (int i = 0; i < 100; i++) {
            String doc = generateRandomDocument(3, 0);
            assertParsersAgree(doc);
        }
    }

    public void testRandomDeepDocumentsMatchJackson() throws IOException {
        for (int i = 0; i < 50; i++) {
            String doc = generateRandomDocument(8, 0);
            assertParsersAgree(doc);
        }
    }

    public void testRandomWideDocumentsMatchJackson() throws IOException {
        for (int i = 0; i < 20; i++) {
            StringBuilder sb = new StringBuilder("{");
            int fieldCount = randomIntBetween(10, 30);
            for (int f = 0; f < fieldCount; f++) {
                if (f > 0) sb.append(",");
                sb.append("\"f").append(f).append("\":");
                sb.append(generateRandomValue(2, 0));
            }
            sb.append("}");
            assertParsersAgree(sb.toString());
        }
    }

    private String generateRandomDocument(int maxFields, int depth) {
        StringBuilder sb = new StringBuilder("{");
        int fieldCount = randomIntBetween(1, maxFields);
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"f").append(i).append("_").append(randomAlphaOfLengthBetween(1, 8)).append("\":");
            sb.append(generateRandomValue(maxFields, depth));
        }
        sb.append("}");
        return sb.toString();
    }

    private String generateRandomValue(int maxFields, int depth) {
        int type;
        if (depth >= 5) {
            type = randomIntBetween(0, 4);
        } else {
            type = randomIntBetween(0, 7);
        }

        return switch (type) {
            case 0 -> "\"" + randomAlphaOfLengthBetween(0, 20) + "\"";
            case 1 -> String.valueOf(randomIntBetween(-1000000, 1000000));
            case 2 -> String.valueOf(randomDoubleBetween(-1000.0, 1000.0, true));
            case 3 -> randomBoolean() ? "true" : "false";
            case 4 -> "null";
            case 5 -> generateRandomDocument(Math.max(1, maxFields - 1), depth + 1);
            case 6 -> generateRandomArray(maxFields, depth + 1);
            case 7 -> "{}";
            default -> "null";
        };
    }

    private String generateRandomArray(int maxFields, int depth) {
        StringBuilder sb = new StringBuilder("[");
        int elemCount = randomIntBetween(0, 5);
        for (int i = 0; i < elemCount; i++) {
            if (i > 0) sb.append(",");
            sb.append(generateRandomValue(maxFields, depth));
        }
        sb.append("]");
        return sb.toString();
    }

}
