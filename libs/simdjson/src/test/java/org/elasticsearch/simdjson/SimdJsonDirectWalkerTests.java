/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.elasticsearch.simdjson.internal.parsers.BitIndexes;

import java.math.BigInteger;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

// Unit tests for SimdJsonDirectWalker event emission (simdjson-only, no Jackson comparison).
public class SimdJsonDirectWalkerTests extends SimdJsonTestCase {

    // ---- Scalars and root object ----

    // Root {} emits no handler events (emptyObject is only for nested {}).
    public void testEmptyObject() {
        List<String> events = walkJson("{}");
        assertEquals(List.of(), events);
    }

    // Each scalar JSON type maps to one handler event string.
    public void testSingleStringField() {
        List<String> events = walkJson("{\"a\":\"hello\"}");
        assertEquals(List.of("string(a=hello)"), events);
    }

    public void testSingleIntField() {
        List<String> events = walkJson("{\"n\":42}");
        assertEquals(List.of("long(n=42,fitsInt=true)"), events);
    }

    // Values beyond int range still emit long with fitsInt=false.
    public void testSingleLongField() {
        List<String> events = walkJson("{\"n\":9999999999}");
        assertEquals(List.of("long(n=9999999999,fitsInt=false)"), events);
    }

    // Decimal point forces double classification.
    public void testSingleDoubleField() {
        List<String> events = walkJson("{\"d\":3.14}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(d=3.14,"));
    }

    public void testBooleanTrue() {
        List<String> events = walkJson("{\"b\":true}");
        assertEquals(List.of("bool(b=true)"), events);
    }

    public void testBooleanFalse() {
        List<String> events = walkJson("{\"b\":false}");
        assertEquals(List.of("bool(b=false)"), events);
    }

    public void testNullField() {
        List<String> events = walkJson("{\"n\":null}");
        assertEquals(List.of("null(n)"), events);
    }

    // Field order is preserved; each type maps to one handler event.
    public void testMultipleFields() {
        List<String> events = walkJson("{\"a\":1,\"b\":\"x\",\"c\":true}");
        assertEquals(3, events.size());
        assertEquals("long(a=1,fitsInt=true)", events.get(0));
        assertEquals("string(b=x)", events.get(1));
        assertEquals("bool(c=true)", events.get(2));
    }

    // ---- Nesting and depth limits ----

    public void testNestedObject() {
        List<String> events = walkJson("{\"o\":{\"inner\":1}}");
        assertEquals(List.of("startObject(o)", "long(inner=1,fitsInt=true)", "endObject()"), events);
    }

    // Walker uses emptyObject() for {} (Jackson comparison mode normalizes to start/end).
    public void testEmptyNestedObject() {
        List<String> events = walkJson("{\"o\":{}}");
        assertEquals(List.of("emptyObject(o)"), events);
    }

    // 10 levels of nesting — startObject/endObject pairs must balance.
    public void testDeeplyNested() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < 10; i++) {
            sb.append("\"l").append(i).append("\":{");
        }
        sb.append("\"v\":1");
        for (int i = 0; i < 10; i++) {
            sb.append("}");
        }
        sb.append("}");

        List<String> events = walkJson(sb.toString());
        int startCount = 0;
        int endCount = 0;
        for (String event : events) {
            if (event.startsWith("startObject(")) startCount++;
            if (event.equals("endObject()")) endCount++;
        }
        assertEquals(10, startCount);
        assertEquals(10, endCount);
        assertTrue(events.contains("long(v=1,fitsInt=true)"));
    }

    // MAX_DEPTH is 64; 65 nested objects must fail.
    public void testMaxDepthExceeded() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < 65; i++) {
            sb.append("\"l").append(i).append("\":{");
        }
        sb.append("\"v\":1");
        for (int i = 0; i < 65; i++) {
            sb.append("}");
        }
        sb.append("}");

        expectThrows(JsonParsingException.class, () -> walkJson(sb.toString()));
    }

    // ---- Arrays ----

    public void testSimpleIntArray() {
        List<String> events = walkJson("{\"a\":[1,2,3]}");
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemLong(1,fitsInt=true)",
                "arrayElemLong(2,fitsInt=true)",
                "arrayElemLong(3,fitsInt=true)",
                "endArray()"
            ),
            events
        );
    }

    // Mixed scalar types in one array.
    public void testMixedArray() {
        List<String> events = walkJson("{\"a\":[1,\"s\",true,null,3.14]}");
        assertEquals(7, events.size());
        assertEquals("startArray(a)", events.get(0));
        assertEquals("arrayElemLong(1,fitsInt=true)", events.get(1));
        assertEquals("arrayElemString(s)", events.get(2));
        assertEquals("arrayElemBoolean(true)", events.get(3));
        assertEquals("arrayElemNull()", events.get(4));
        assertTrue(events.get(5).startsWith("arrayElemDouble(3.14,"));
        assertEquals("endArray()", events.get(6));
    }

    // Array of arrays — nested start/end array events.
    public void testNestedArrayInArray() {
        List<String> events = walkJson("{\"a\":[[1,2],[3]]}");
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemStartArray()",
                "arrayElemLong(1,fitsInt=true)",
                "arrayElemLong(2,fitsInt=true)",
                "arrayElemEndArray()",
                "arrayElemStartArray()",
                "arrayElemLong(3,fitsInt=true)",
                "arrayElemEndArray()",
                "endArray()"
            ),
            events
        );
    }

    // Object as array element uses arrayElemStartObject/EndObject wrappers.
    public void testObjectInArray() {
        List<String> events = walkJson("{\"a\":[{\"k\":\"v\"}]}");
        assertEquals(List.of("startArray(a)", "arrayElemStartObject()", "string(k=v)", "arrayElemEndObject()", "endArray()"), events);
    }

    // ---- Escapes, signs, and scientific notation ----

    // \\n in a string value is decoded to a real newline.
    public void testEscapedStringField() {
        List<String> events = walkJson("{\"a\":\"hello\\nworld\"}");
        assertEquals(1, events.size());
        assertEquals("string(a=hello\nworld)", events.get(0));
    }

    public void testNegativeNumber() {
        List<String> events = walkJson("{\"n\":-42}");
        assertEquals(List.of("long(n=-42,fitsInt=true)"), events);
    }

    // ---- Integer field values across digit-count boundaries ----
    //
    // These values (and their array-element and buffer-padding variants) are also exercised via
    // SimdJsonTestDocuments, shared with SimdJsonJacksonComparisonTests; the checks here pin the
    // exact emitted event, which the shared, Jackson-agreement-only checks don't.

    // The 0/1 boolean-flag shape that dominates many real payloads.
    public void testZeroField() {
        assertEquals(List.of("long(n=0,fitsInt=true)"), walkJson("{\"n\":0}"));
    }

    public void testSingleDigitField() {
        assertEquals(List.of("long(n=9,fitsInt=true)"), walkJson("{\"n\":9}"));
    }

    public void testNegativeSingleDigitField() {
        assertEquals(List.of("long(n=-5,fitsInt=true)"), walkJson("{\"n\":-5}"));
    }

    // "-0" as an integer has no sign: Java's long negation of 0 is 0.
    public void testNegativeZeroIntegerField() {
        assertEquals(List.of("long(n=0,fitsInt=true)"), walkJson("{\"n\":-0}"));
    }

    public void testTwoDigitField() {
        assertEquals(List.of("long(n=10,fitsInt=true)"), walkJson("{\"n\":10}"));
        assertEquals(List.of("long(n=99,fitsInt=true)"), walkJson("{\"n\":99}"));
    }

    public void testNegativeTwoDigitField() {
        assertEquals(List.of("long(n=-99,fitsInt=true)"), walkJson("{\"n\":-99}"));
    }

    // Just above the two-digit values above.
    public void testThreeDigitField() {
        assertEquals(List.of("long(n=100,fitsInt=true)"), walkJson("{\"n\":100}"));
    }

    public void testTenDigitFieldFitsInt() {
        assertEquals(List.of("long(n=1234567890,fitsInt=true)"), walkJson("{\"n\":1234567890}"));
    }

    public void testTenDigitFieldExceedsIntRange() {
        assertEquals(List.of("long(n=9876543210,fitsInt=false)"), walkJson("{\"n\":9876543210}"));
    }

    // A short integer prefix immediately followed by '.'/'e'/'E' must still be classified as a
    // double, not misread as a short integer.
    public void testSingleDigitBeforeDecimalPoint() {
        List<String> events = walkJson("{\"n\":1.5}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=1.5,"));
    }

    public void testSingleDigitBeforeExponent() {
        List<String> events = walkJson("{\"n\":1e2}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=100.0,"));
    }

    public void testTwoDigitBeforeDecimalPoint() {
        List<String> events = walkJson("{\"n\":12.5}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=12.5,"));
    }

    // Same digit-count boundaries as array elements.
    public void testSmallDigitArrayElements() {
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemLong(0,fitsInt=true)",
                "arrayElemLong(9,fitsInt=true)",
                "arrayElemLong(10,fitsInt=true)",
                "arrayElemLong(99,fitsInt=true)",
                "arrayElemLong(100,fitsInt=true)",
                "arrayElemLong(1234567890,fitsInt=true)",
                "arrayElemLong(-5,fitsInt=true)",
                "arrayElemLong(-99,fitsInt=true)",
                "endArray()"
            ),
            walkJson("{\"a\":[0,9,10,99,100,1234567890,-5,-99]}")
        );
    }

    public void testSmallDigitDoubleArrayElements() {
        List<String> events = walkJson("{\"a\":[1.5,12.5,2e5]}");
        assertEquals(5, events.size());
        assertTrue(events.get(1).startsWith("arrayElemDouble(1.5,"));
        assertTrue(events.get(2).startsWith("arrayElemDouble(12.5,"));
        assertTrue(events.get(3).startsWith("arrayElemDouble(200000.0,"));
    }

    // ---- Digit-count boundary at 19 (handleLargeNumber: long vs. BigInteger fallback) ----

    // 19 digits fits a signed long (both sign boundaries).
    public void testNineteenDigitFieldFitsLong() {
        assertEquals(List.of("long(n=" + Long.MAX_VALUE + ",fitsInt=false)"), walkJson("{\"n\":" + Long.MAX_VALUE + "}"));
        assertEquals(List.of("long(n=" + Long.MIN_VALUE + ",fitsInt=false)"), walkJson("{\"n\":" + Long.MIN_VALUE + "}"));
    }

    // 19+ digits that overflow a signed long fall back to BigInteger.
    public void testLargeDigitFieldOverflowsToBigInteger() {
        assertEquals(
            "Long.MAX_VALUE + 1: 19 digits, positive, overflows a signed long",
            List.of("bigInteger(n=9223372036854775808)"),
            walkJson("{\"n\":9223372036854775808}")
        );
        assertEquals(
            "Long.MIN_VALUE - 1: 19 digits, negative, overflows a signed long",
            List.of("bigInteger(n=-9223372036854775809)"),
            walkJson("{\"n\":-9223372036854775809}")
        );
        assertEquals(
            "20 digits: always BigInteger regardless of value",
            List.of("bigInteger(n=99999999999999999999)"),
            walkJson("{\"n\":99999999999999999999}")
        );
        for (int i = 0; i < 20; i++) {
            boolean negative = randomBoolean();
            String digits = randomNumericOfLength(randomIntBetween(20, 40));
            String sign = negative ? "-" : "";
            String expected = new BigInteger(sign + digits).toString();
            assertEquals(
                "digitCount=" + digits.length() + ", negative=" + negative + ": always BigInteger regardless of value",
                List.of("bigInteger(n=" + expected + ")"),
                walkJson("{\"n\":" + sign + digits + "}")
            );
        }
    }

    // Same digitCount-at-19 boundaries as array elements.
    public void testDigitCountNineteenBoundaryArrayElements() {
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemLong(" + Long.MAX_VALUE + ",fitsInt=false)",
                "arrayElemLong(" + Long.MIN_VALUE + ",fitsInt=false)",
                "arrayElemBigInteger(9223372036854775808)",
                "arrayElemBigInteger(-9223372036854775809)",
                "arrayElemBigInteger(99999999999999999999)",
                "endArray()"
            ),
            walkJson(
                "{\"a\":[" + Long.MAX_VALUE + "," + Long.MIN_VALUE + ",9223372036854775808,-9223372036854775809,99999999999999999999]}"
            )
        );
    }

    // ---- Leading zeros in the integer part are rejected (RFC 8259: "0" or [1-9][0-9]*) ----

    // A lone "0" is legal, whether or not it's followed by a fraction/exponent.
    public void testLoneZeroIsNotALeadingZero() {
        assertEquals(List.of("long(n=0,fitsInt=true)"), walkJson("{\"n\":0}"));
        assertEquals(List.of("long(n=0,fitsInt=true)"), walkJson("{\"n\":-0}"));
        assertTrue(walkJson("{\"n\":0.5}").get(0).startsWith("double(n=0.5,"));
        assertTrue(walkJson("{\"n\":-0.5}").get(0).startsWith("double(n=-0.5,"));
        assertTrue(walkJson("{\"n\":0e5}").get(0).startsWith("double(n=0.0,"));
        assertTrue(walkJson("{\"n\":0e05}").get(0).startsWith("double(n=0.0,"));
        assertTrue(walkJson("{\"n\":1e05}").get(0).startsWith("double(n=100000.0,"));
        assertTrue(walkJson("{\"n\":1E06}").get(0).startsWith("double(n=1000000.0,"));
    }

    // Verifies that json is rejected specifically for a leading zero, not some other parse error.
    private void assertLeadingZeroRejected(String json) {
        JsonParsingException e = expectThrows(JsonParsingException.class, () -> walkJson(json));
        assertTrue("message: " + e.getMessage(), e.getMessage().contains("Leading zero"));
    }

    // Two digits starting with '0': caught by handleNumber's 2-digit fast path.
    public void testLeadingZeroRejectedAtTwoDigits() {
        assertLeadingZeroRejected("{\"n\":00}");
        assertLeadingZeroRejected("{\"n\":01}");
        assertLeadingZeroRejected("{\"n\":-00}");
        assertLeadingZeroRejected("{\"n\":-01}");
    }

    // Three or more digits starting with '0': caught by the general/SWAR path.
    public void testLeadingZeroRejectedAtThreeOrMoreDigits() {
        assertLeadingZeroRejected("{\"n\":007}");
        assertLeadingZeroRejected("{\"n\":-0123}");
        assertLeadingZeroRejected("{\"n\":00000000000000000009}"); // digitCount > 19 too
    }

    // A leading zero is rejected regardless of what follows the integer part.
    public void testLeadingZeroRejectedBeforeFractionOrExponent() {
        assertLeadingZeroRejected("{\"n\":00.5}");
        assertLeadingZeroRejected("{\"n\":01.5}");
        assertLeadingZeroRejected("{\"n\":01e5}");
        assertLeadingZeroRejected("{\"n\":-01.5}");
    }

    // Leading zeros are legal in the fraction and exponent, since the rule only applies to the
    // integer part.
    public void testLeadingZeroAllowedInFractionAndExponent() {
        assertTrue(walkJson("{\"n\":1.007}").get(0).startsWith("double(n=1.007,"));
        assertTrue(walkJson("{\"n\":1e007}").get(0).startsWith("double(n=1.0E7,"));
    }

    // Same leading-zero rejections as array elements.
    public void testLeadingZeroRejectedAsArrayElement() {
        assertLeadingZeroRejected("{\"a\":[00]}");
        assertLeadingZeroRejected("{\"a\":[01]}");
        assertLeadingZeroRejected("{\"a\":[12, 01]}");
        assertLeadingZeroRejected("{\"a\":[-01]}");
        assertLeadingZeroRejected("{\"a\":[13, -01]}");
        assertLeadingZeroRejected("{\"a\":[007]}");
        assertLeadingZeroRejected("{\"a\":[00.5]}");
        assertLeadingZeroRejected("{\"a\":[01e5]}");
    }

    // ---- computeLineAndColumn: [line:column] location in the leading-zero message ----

    // Verifies the "[line:column]" location prefix of the leading-zero exception message.
    private void assertLeadingZeroLocation(String json, int expectedLine, int expectedColumn) {
        JsonParsingException e = expectThrows(JsonParsingException.class, () -> walkJson(json));
        String prefix = "[" + expectedLine + ":" + expectedColumn + "]";
        assertTrue("message: " + e.getMessage(), e.getMessage().startsWith(prefix));
    }

    public void testLineAndColumnOnSingleLine() {
        assertLeadingZeroLocation("{\"n\":00}", 1, 6);
    }

    // Each '\n' starts a new line; the column resets relative to it.
    public void testLineAndColumnAfterNewlines() {
        assertLeadingZeroLocation("{\n\"n\":00}", 2, 5);
        assertLeadingZeroLocation("{\n\n\"n\":00}", 3, 5);
    }

    // "\r", "\n", and "\r\n" each count as exactly one line break, matching Jackson: a lone
    // "\r" starts a new line just like "\n" does, but a "\r\n" pair only starts one, not two.
    public void testLineAndColumnAcrossCarriageReturns() {
        assertLeadingZeroLocation("{\r\"n\":00}", 2, 5);
        assertLeadingZeroLocation("{\r\n\"n\":00}", 2, 5);
        assertLeadingZeroLocation("{\r\r\"n\":00}", 3, 5);
        assertLeadingZeroLocation("{\n\r\n\"n\":00}", 3, 5);
    }

    // "x" is one UTF-8 byte and "é" is two, both a single code point; the column counts
    // bytes, so replacing "x" with "é" in the same position advances it by one.
    public void testColumnCountsUtf8BytesNotCodePoints() {
        assertLeadingZeroLocation("{\"a\":\"x\",\"n\":00}", 1, 14);
        assertLeadingZeroLocation("{\"a\":\"\u00e9\",\"n\":00}", 1, 15);
    }

    // ---- Other malformed-number shapes are rejected, not silently mis-parsed ----

    // Verifies json is rejected with a message containing expectedReason, not some other error .
    private void assertInvalidNumberRejected(String json, String expectedReason) {
        for (int padding : new int[] { 0, 32, 64, 128 }) {
            JsonParsingException e = expectThrows(JsonParsingException.class, () -> walkAndRecord(json, padding));
            assertTrue("padding=" + padding + " message: " + e.getMessage(), e.getMessage().contains(expectedReason));
        }
    }

    // RFC 8259 requires at least one digit after the decimal point ("frac = '.' 1*DIGIT").
    public void testDecimalPointNotFollowedByDigitRejected() {
        assertInvalidNumberRejected("{\"n\":1.}", "Decimal point not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1.,\"m\":2}", "Decimal point not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1,\"m\":2.}", "Decimal point not followed by a digit");
        // The fraction is checked before the exponent is even considered.
        assertInvalidNumberRejected("{\"n\":1.e5}", "Decimal point not followed by a digit");
        assertInvalidNumberRejected("{\"a\":[1.]}", "Decimal point not followed by a digit");
        assertInvalidNumberRejected("{\"a\":[1.0, 2.]}", "Decimal point not followed by a digit");
    }

    // RFC 8259 requires at least one digit after 'e'/'E' (and its optional sign):
    // "exp = ('e' / 'E') ['-' / '+'] 1*DIGIT".
    public void testExponentIndicatorNotFollowedByDigitRejected() {
        assertInvalidNumberRejected("{\"n\":1e}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1e+}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1e-}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1E}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1E+}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"n\":1E-}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"a\":[1e]}", "Exponent indicator not followed by a digit");
        assertInvalidNumberRejected("{\"a\":[1e1, 1e]}", "Exponent indicator not followed by a digit");
    }

    // A number must be immediately followed by a structural character or whitespace; anything
    // else (e.g. a second '.', a stray '-', or trailing letters) is rejected rather than being
    // silently dropped when scanning ahead for the next comma/brace/bracket.
    public void testTrailingGarbageAfterNumberRejected() {
        assertInvalidNumberRejected("{\"n\":1.2.3}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"n\":1.2.3,\"m\":4}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"n\":1e5e6}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"n\":1-2}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"n\":12-3}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"n\":1foo}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"a\":[1.2.3]}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"a\":[1foo]}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"a\":[12-3]}", "Unexpected character after number");
        // The terminator is checked before the digitCount>=19 BigInteger dispatch too.
        assertInvalidNumberRejected("{\"n\":123456789012345678901foo}", "Unexpected character after number");
        assertInvalidNumberRejected("{\"a\":[123456789012345678901foo]}", "Unexpected character after number");
    }

    // checkTerminator's accept branch: whitespace (not just a structural char) immediately after
    // a number is fine, for every RFC 8259 whitespace byte, both after an object field value and
    // an array element.
    public void testWhitespaceAfterNumberAccepted() {
        for (String ws : new String[] { " ", "\t", "\n", "\r" }) {
            assertEquals(List.of("long(n=1,fitsInt=true)"), walkJson("{\"n\":1" + ws + "}"));
            assertEquals(List.of("startArray(a)", "arrayElemLong(1,fitsInt=true)", "endArray()"), walkJson("{\"a\":[1" + ws + "]}"));
        }
    }

    // RFC 8259 requires at least one digit in the integer part ("int = '0' / [1-9] *DIGIT");
    // it's never empty, even when '-' is immediately followed by '.' or 'e'/'E'.
    public void testNoIntegerDigitsRejected() {
        assertInvalidNumberRejected("{\"n\":-}", "No digits found");
        assertInvalidNumberRejected("{\"n\":-.5}", "No digits found");
        assertInvalidNumberRejected("{\"n\":-e5}", "No digits found");
        assertInvalidNumberRejected("{\"a\":[-]}", "No digits found");
        assertInvalidNumberRejected("{\"a\":[-,1]}", "No digits found");
        assertInvalidNumberRejected("{\"a\":[-e5]}", "No digits found");
    }

    public void testNegativeDouble() {
        List<String> events = walkJson("{\"n\":-3.14}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=-3.14,"));
    }

    // Exponent form produces double event (not long), with either sign.
    public void testScientificNotation() {
        List<String> events = walkJson("{\"n\":1.5e10}");
        assertEquals(1, events.size());
        assertTrue(events.get(0).startsWith("double(n=1.5E10,"));

        assertTrue(walkJson("{\"n\":1.5e-5}").get(0).startsWith("double(n=1.5E-5,"));
    }

    // Root must be an object; top-level arrays are rejected.
    public void testDocumentStartingWithArray() {
        expectThrows(JsonParsingException.class, () -> walkJson("[1,2]"));
    }

    // ---- Parser/walker integration edge cases ----

    // Empty BitIndexes (no structurals) must fail before value parsing.
    public void testEmptyBitIndexesThrows() {
        byte[] buffer = new byte[0];
        BitIndexes bitIndexes = new BitIndexes(64);
        bitIndexes.reset();
        bitIndexes.setReadWindow(0, 0);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);
        RecordingHandler handler = new RecordingHandler();

        expectThrows(JsonParsingException.class, () -> walker.walkDocument(buffer, bitIndexes, handler));
    }

    // Repeated walks must resolve the same field name String from FrozenFieldNameTable.
    public void testFieldNameCaching() {
        String json = "{\"field\":1}";
        byte[] buffer = json.getBytes(UTF_8);
        int len = buffer.length;

        SimdJsonParser parser = newParser(len);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);
        RecordingHandler handler1 = new RecordingHandler();
        walker.walkDocument(buffer, parser.bitIndexes(), handler1);

        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);
        RecordingHandler handler2 = new RecordingHandler();
        walker.walkDocument(buffer, parser.bitIndexes(), handler2);

        String name1 = handler1.events.get(0).substring("long(".length(), handler1.events.get(0).indexOf('='));
        String name2 = handler2.events.get(0).substring("long(".length(), handler2.events.get(0).indexOf('='));
        assertEquals("field", name1);
        assertEquals("field", name2);
    }

    // ---- Exact buffer length and padding invariance ----

    public void testWalksDocumentsWithExactBufferLength() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> expected = walkJson(json);
            assertEquals("exact buffer length walk for: " + json, expected, walkAndRecord(json, 0).events);
        }
    }

    public void testTrailingBufferPaddingDoesNotChangeEvents() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> tight = walkAndRecord(json, 0).events;
            for (int padding : new int[] { 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65 }) {
                assertEquals("padding must not change events for: " + json, tight, walkAndRecord(json, padding).events);
            }
        }
    }

    public void testNonZeroStartOffsetDoesNotChangeEvents() {
        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            List<String> expected = walkJson(json);
            for (int offset : new int[] { 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65 }) {
                assertEquals("offset=" + offset + " walk for: " + json, expected, walkAndRecordAtOffset(json, offset).events);
            }
        }
    }

    public void testEmptyArray() {
        List<String> events = walkJson("{\"a\":[]}");
        assertEquals(List.of("startArray(a)", "endArray()"), events);
    }

    public void testEmptyString() {
        List<String> events = walkJson("{\"a\":\"\"}");
        assertEquals(List.of("string(a=)"), events);
    }

    public void testUnicodeStringValue() {
        List<String> events = walkJson("{\"a\":\"caf\u00e9\"}");
        assertEquals(List.of("string(a=caf\u00e9)"), events);
    }

    public void testObjectsInNestedArray() {
        List<String> events = walkJson("{\"a\":[{\"x\":1},{\"y\":2}]}");
        assertEquals(
            List.of(
                "startArray(a)",
                "arrayElemStartObject()",
                "long(x=1,fitsInt=true)",
                "arrayElemEndObject()",
                "arrayElemStartObject()",
                "long(y=2,fitsInt=true)",
                "arrayElemEndObject()",
                "endArray()"
            ),
            events
        );
    }

    // Truncated JSON must not complete a successful walk.
    public void testTruncatedJsonMustNotWalkSuccessfully() {
        byte[] buffer = "{\"a\":1".getBytes(UTF_8);
        try (SimdJsonParser parser = newParser(buffer.length)) {
            parser.stage1(buffer, buffer.length);
            parser.prepareDocumentWindow(0, buffer.length);
            FrozenFieldNameTable parent = new FrozenFieldNameTable();
            SimdJsonDirectWalker walker = new SimdJsonDirectWalker(parent.makeChild());
            RecordingHandler handler = new RecordingHandler();
            expectThrows(Exception.class, () -> walker.walkDocument(buffer, parser, handler));
        } catch (JsonParsingException e) {
            // stage 1 rejection is acceptable
        }
    }

}
