/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.test.ESTestCase;

public class ExponentParserTests extends ESTestCase {

    private final ExponentParser parser = new ExponentParser();

    private byte[] exponentBytes(String digits) {
        byte[] buff = new byte[digits.length() + 1];
        for (int i = 0; i < digits.length(); i++) {
            buff[i] = (byte) digits.charAt(i);
        }
        buff[digits.length()] = (byte) ' ';
        return buff;
    }

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

    public void testLeadingZeros() {
        byte[] buff = exponentBytes("007");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(7L, result.exponent());
        assertEquals(3, result.currentIdx());
    }

    public void testOverflowClampedToLargeValue() {
        String digits = "9".repeat(20);
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(999999999999999999L, result.exponent());
        assertEquals(20, result.currentIdx());
    }

    public void testOverflowWithLeadingZerosNotClamped() {
        String digits = "0".repeat(15) + "123";
        byte[] buff = exponentBytes(digits);
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 0);
        assertEquals(123L, result.exponent());
        assertEquals(18, result.currentIdx());
    }

    public void testExponentAddedToExistingValue() {
        byte[] buff = exponentBytes("10");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 5);
        assertEquals(15L, result.exponent());
    }

    public void testNegativeExponentSubtractedFromExistingValue() {
        byte[] buff = exponentBytes("-3");
        ExponentParser.ExponentParsingResult result = parser.parse(buff, 0, 10);
        assertEquals(7L, result.exponent());
    }

    public void testNoDigitAfterSignThrows() {
        byte[] buff = new byte[] { '+', ' ' };
        expectThrows(JsonParsingException.class, () -> parser.parse(buff, 0, 0));
    }

    public void testIsExponentIndicator() {
        assertTrue(ExponentParser.isExponentIndicator((byte) 'e'));
        assertTrue(ExponentParser.isExponentIndicator((byte) 'E'));
        assertFalse(ExponentParser.isExponentIndicator((byte) 'x'));
        assertFalse(ExponentParser.isExponentIndicator((byte) '0'));
    }
}
