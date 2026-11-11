/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.parsers;

import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

// Unit tests for the exponent suffix parser (the part after 'e'/'E' in floats).
public class ExponentParserTests extends ESTestCase {

    private final ExponentParser parser = new ExponentParser();

    // ---- Basic exponent values ----

    public void testPositiveExponent() {
        byte[] buff = exponentBytes("10");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(10L, result.exponent());
        assertEquals(2, result.currentIdx());
    }

    public void testPositiveExponentLargeValue() {
        byte[] buff = exponentBytes("999");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(999L, result.exponent());
        assertEquals(3, result.currentIdx());
    }

    public void testNegativeExponent() {
        byte[] buff = exponentBytes("-5");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(-5L, result.exponent());
        assertEquals(2, result.currentIdx());
    }

    public void testNegativeExponentLargeValue() {
        byte[] buff = exponentBytes("-300");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(-300L, result.exponent());
        assertEquals(4, result.currentIdx());
    }

    public void testPlusSignExponent() {
        byte[] buff = exponentBytes("+42");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(42L, result.exponent());
        assertEquals(3, result.currentIdx());
    }

    public void testZeroExponent() {
        byte[] buff = exponentBytes("0");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(0L, result.exponent());
        assertEquals(1, result.currentIdx());
    }

    public void testSingleDigit() {
        byte[] buff = exponentBytes("5");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(5L, result.exponent());
        assertEquals(1, result.currentIdx());
    }

    // Leading zeros in exponent digits are ignored (007 -> 7).
    public void testLeadingZeros() {
        byte[] buff = exponentBytes("007");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(7L, result.exponent());
        assertEquals(3, result.currentIdx());
    }

    // ---- Digit-count boundaries (18 = max safe, 19+ clamps) ----

    // Sentinel used when exponent digit runs overflow long parsing.
    private static final long CLAMPED_EXPONENT_MAGNITUDE = 999999999999999999L;

    public void testMaxDigitCountWithoutClamp() {
        String digits = "999999999999999999";
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(CLAMPED_EXPONENT_MAGNITUDE, result.exponent());
        assertEquals(18, result.currentIdx());
    }

    public void testClampAtNineteenDigits() {
        String digits = "9999999999999999999";
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(CLAMPED_EXPONENT_MAGNITUDE, result.exponent());
        assertEquals(19, result.currentIdx());
    }

    // ---- Overflow and composition with fractional exponent ----

    // Absurdly long exponent digit runs clamp to a large sentinel value.
    public void testOverflowClampedToLargeValue() {
        String digits = "9".repeat(20);
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(CLAMPED_EXPONENT_MAGNITUDE, result.exponent());
        assertEquals(20, result.currentIdx());
    }

    public void testNegativeOverflowClampedToLargeValue() {
        String digits = "-" + "9".repeat(20);
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(-CLAMPED_EXPONENT_MAGNITUDE, result.exponent());
        assertEquals(21, result.currentIdx());
    }

    // Leading zeros prevent premature clamping on long digit strings.
    public void testOverflowWithLeadingZerosNotClamped() {
        String digits = "0".repeat(15) + "123";
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(123L, result.exponent());
        assertEquals(18, result.currentIdx());
    }

    // Exponent magnitudes that appear in double fast-path range checks.
    public void testRealisticDoubleExponentMagnitudes() {
        assertEquals(308L, parser.parse(exponentBytes("308"), 0, 0).exponent());
        assertEquals(-324L, parser.parse(exponentBytes("-324"), 0, 0).exponent());
    }

    // Exponent suffix may start mid-buffer after 'e'/'E' in a larger JSON document.
    public void testParseWithNonZeroOffset() {
        int prefixLen = 4;
        byte[] buff = exponentBuffer(prefixLen, "42");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, prefixLen, 0);
        assertEquals(42L, result.exponent());
        assertEquals(prefixLen + 2, result.currentIdx());
    }

    // Exponent from fractional part is added to parsed exponent digits.
    public void testExponentAddedToExistingValue() {
        byte[] buff = exponentBytes("10");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 5);
        assertEquals(15L, result.exponent());
        assertEquals(2, result.currentIdx());
    }

    public void testNegativeExponentSubtractedFromExistingValue() {
        byte[] buff = exponentBytes("-3");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 10);
        assertEquals(7L, result.exponent());
        assertEquals(2, result.currentIdx());
    }

    public void testNoDigitAfterSignThrows() {
        for (byte sign : List.of((byte) '+', (byte) '-')) {
            char delimiter = randomFrom(',', '}', ']', ' ', '\n', '\t');
            byte[] buff = new byte[] { sign, (byte) delimiter };
            expectThrows(JsonParsingException.class, () -> parser.parse(buff, 0, 0));
        }
    }

    public void testIsExponentIndicator() {
        assertTrue(ExponentParser.isExponentIndicator((byte) 'e'));
        assertTrue(ExponentParser.isExponentIndicator((byte) 'E'));
        assertFalse(ExponentParser.isExponentIndicator((byte) 'x'));
        assertFalse(ExponentParser.isExponentIndicator((byte) '0'));
        assertFalse(
            ExponentParser.isExponentIndicator(randomValueOtherThanMany(b -> b == (byte) 'e' || b == (byte) 'E', ESTestCase::randomByte))
        );
    }

    // Infra

    // Exponent digit suffix at buffer start (no leading prefix bytes).
    private byte[] exponentBytes(String digits) {
        return exponentBuffer(0, digits);
    }

    // Exponent digit run plus optional prefix and trailing JSON delimiter. ExponentParser reads
    // one byte past the last digit to detect the end of the run.
    private byte[] exponentBuffer(int prefixLen, String digits) {
        char delimiter = randomFrom(',', '}', ']', ' ', '\n', '\t');
        byte[] buff = new byte[prefixLen + digits.length() + 1];
        Arrays.fill(buff, 0, prefixLen, (byte) 'x');
        System.arraycopy(digits.getBytes(UTF_8), 0, buff, prefixLen, digits.length());
        buff[prefixLen + digits.length()] = (byte) delimiter;
        return buff;
    }
}
