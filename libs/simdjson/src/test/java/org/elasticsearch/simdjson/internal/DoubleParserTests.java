/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class DoubleParserTests extends ESTestCase {

    private final DoubleParser parser = new DoubleParser();

    private double parseNumber(String numberStr) {
        boolean negative = numberStr.startsWith("-");
        String abs = negative ? numberStr.substring(1) : numberStr;

        long digits = 0;
        int digitCount = 0;
        long exponent = 0;
        boolean seenDot = false;
        int fractionalDigits = 0;
        int exponentPartStart = -1;

        for (int i = 0; i < abs.length(); i++) {
            char c = abs.charAt(i);
            if (c == '.') {
                seenDot = true;
            } else if (c == 'e' || c == 'E') {
                exponentPartStart = i + 1;
                break;
            } else {
                digits = digits * 10 + (c - '0');
                digitCount++;
                if (seenDot) {
                    fractionalDigits++;
                }
            }
        }

        exponent = -fractionalDigits;

        if (exponentPartStart > 0) {
            String expStr = abs.substring(exponentPartStart);
            exponent += Long.parseLong(expStr);
        }

        byte[] buf = numberStr.getBytes(StandardCharsets.UTF_8);
        int startIdx = 0;
        int digitsStartIdx = negative ? 1 : 0;

        return parser.parse(buf, startIdx, negative, digitsStartIdx, digitCount, digits, exponent);
    }

    public void testZero() {
        assertEquals(0.0, parseNumber("0.0"), 0.0);
    }

    public void testNegativeZero() {
        double result = parseNumber("-0.0");
        assertEquals(-0.0, result, 0.0);
        assertTrue(Double.doubleToRawLongBits(result) < 0);
    }

    public void testOne() {
        assertEquals(1.0, parseNumber("1.0"), 0.0);
    }

    public void testSimpleFraction() {
        assertEquals(3.14, parseNumber("3.14"), 0.0);
    }

    public void testSmallInteger() {
        assertEquals(42.0, parseNumber("42.0"), 0.0);
    }

    public void testExponentPositive() {
        assertEquals(Double.parseDouble("1.5e10"), parseNumber("1.5e10"), 0.0);
    }

    public void testExponentNegative() {
        assertEquals(Double.parseDouble("1.5e-10"), parseNumber("1.5e-10"), 0.0);
    }

    public void testExponentCapitalE() {
        assertEquals(Double.parseDouble("1.5E10"), parseNumber("1.5E10"), 0.0);
    }

    public void testExponentWithPlus() {
        assertEquals(Double.parseDouble("1.5e+10"), parseNumber("1.5e+10"), 0.0);
    }

    public void testMaxFiniteDouble() {
        assertEquals(Double.parseDouble("1.7976931348623157e308"), parseNumber("1.7976931348623157e308"), 0.0);
    }

    public void testVerySmallSubnormal() {
        assertEquals(Double.parseDouble("5e-324"), parseNumber("5e-324"), 0.0);
    }

    public void testSmallestNormal() {
        assertEquals(Double.parseDouble("2.2250738585072014e-308"), parseNumber("2.2250738585072014e-308"), 0.0);
    }

    public void testNegativeValues() {
        assertEquals(Double.parseDouble("-3.14"), parseNumber("-3.14"), 0.0);
        assertEquals(Double.parseDouble("-1e10"), parseNumber("-1e10"), 0.0);
    }

    public void testLargeInteger() {
        assertEquals(Double.parseDouble("99999999999999999.0"), parseNumber("99999999999999999.0"), 0.0);
    }

    public void testTrailingZeros() {
        assertEquals(Double.parseDouble("1.50000"), parseNumber("1.50000"), 0.0);
    }

    public void testLeadingFractionalZeros() {
        assertEquals(Double.parseDouble("0.001"), parseNumber("0.001"), 0.0);
    }

    public void testRoundTripConsistency() {
        for (int i = 0; i < 50; i++) {
            double original = randomDoubleBetween(Double.MIN_NORMAL, Double.MAX_VALUE / 2, true);
            String str = Double.toString(original);
            double parsed = parseNumber(str);
            assertEquals(Double.parseDouble(str), parsed, 0.0);
        }
    }

    public void testManyDecimalPlaces() {
        assertEquals(Double.parseDouble("3.141592653589793"), parseNumber("3.141592653589793"), 0.0);
    }
}
