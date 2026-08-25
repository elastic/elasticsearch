/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.simdjson.SimdJsonTestSupport.walkJson;

/**
 * Comparison tests that parse the same JSON with both Jackson (via {@link XContentParser}) and
 * {@link SimdJsonDirectWalker}, then assert that both produce identical event streams. This
 * catches subtle differences in number precision, string encoding, or structural interpretation.
 */
public class SimdJsonJacksonComparisonTests extends ESTestCase {

    // ---- Jackson/XContent walker ----

    private List<String> walkWithJackson(String json) throws IOException {
        List<String> events = new ArrayList<>();
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        try (XContentParser p = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
            XContentParser.Token token = p.nextToken();
            assert token == XContentParser.Token.START_OBJECT : "expected START_OBJECT but got " + token;
            walkJacksonObject(p, events);
        }
        return events;
    }

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

    // ---- Comparison helper ----

    private void assertParsersAgree(String json) throws IOException {
        List<String> jacksonEvents = walkWithJackson(json);
        List<String> simdEvents = walkJson(json, true);
        assertEquals("Event streams differ for: " + json, jacksonEvents, simdEvents);
    }

    // ---- Specific document tests ----

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

    public void testMultipleFieldTypes() throws IOException {
        assertParsersAgree("{\"s\":\"val\",\"i\":42,\"d\":1.5,\"b\":true,\"n\":null}");
    }

    public void testNestedObject() throws IOException {
        assertParsersAgree("{\"outer\":{\"inner\":\"deep\"}}");
    }

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

    public void testLargeObject() throws IOException {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < 50; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"field").append(i).append("\":").append(i);
        }
        sb.append("}");
        assertParsersAgree(sb.toString());
    }

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

    public void testWhitespace() throws IOException {
        assertParsersAgree("{ \"a\" : 1 , \"b\" : 2 }");
    }

    public void testNewlinesAndTabs() throws IOException {
        assertParsersAgree("{\n\t\"a\":\t1,\n\t\"b\":\t2\n}");
    }

    // ---- Numeric edge cases ----

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

    public void testDoublePrecisionClassic() throws IOException {
        // 0.1 cannot be exactly represented in IEEE 754
        assertParsersAgree("{\"n\":0.1}");
    }

    public void testDoublePrecisionOneThird() throws IOException {
        assertParsersAgree("{\"n\":0.3333333333333333}");
    }

    public void testDoubleFitsFloat() throws IOException {
        // 1.5 is exactly representable as both float and double
        assertParsersAgree("{\"n\":1.5}");
    }

    public void testDoubleDoesNotFitFloat() throws IOException {
        // This value differs between float and double precision
        assertParsersAgree("{\"n\":1.1}");
    }

    public void testIntegerLeadingZeroInDouble() throws IOException {
        assertParsersAgree("{\"n\":0.5}");
    }

    public void testLargeIntegerAsDouble() throws IOException {
        // Integer-valued but beyond long range when written with exponent
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

    public void testTrailingZerosInDecimal() throws IOException {
        assertParsersAgree("{\"n\":1.50000000000000}");
    }

    // ---- Scientific notation edge cases ----

    public void testScientificIntegerLikeZeroExponent() throws IOException {
        assertParsersAgree("{\"n\":1e0}");
    }

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

    // ---- Random document generation ----

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
