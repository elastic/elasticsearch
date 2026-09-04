/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.parsers;

import org.elasticsearch.simdjson.SimdJsonDirectWalker;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Unit tests for DoubleParser (simdjson-style f64 parsing).
 * Oracle: {@link Double#parseDouble(String)}.
 */
public class DoubleParserTests extends ESTestCase {

    private final DoubleParser parser = new DoubleParser();

    // ---- Zeros and simple decimals ----

    // Unsigned zero lexical forms — testZero and testNegativeZero share this list.
    private static final List<String> ZERO_LEXICAL_FORMS = List.of(
        "0.0",
        "0.00",
        "00.0",
        "000.00",
        ".0",
        ".00",
        // Digit span > 19, but significant digits remain 0.
        "0000000000000000000000.00",
        "0.00000000000000000000000",
        "0e0",
        "0.0e10",
        "0e-400"
    );

    public void testZero() {
        for (String form : ZERO_LEXICAL_FORMS) {
            assertPositiveZeroLexical(form);
        }
    }

    // Negative zero must preserve sign bit for every form above.
    public void testNegativeZero() {
        for (String form : ZERO_LEXICAL_FORMS) {
            assertNegativeZeroLexical(form);
        }
    }

    public void testOne() {
        assertEquals(1.0, parseNumber("1.0"), 0.0);
        assertEquals(1.0, parseNumber("01.0"), 0.0);
        assertEquals(1.0, parseNumber("1.00"), 0.0);
    }

    public void testSimpleFraction() {
        assertEquals(3.14, parseNumber("3.14"), 0.0);
        assertEquals(3.14, parseNumber("3.140"), 0.0);
        assertEquals(3.14, parseNumber("03.14"), 0.0);
    }

    public void testSmallInteger() {
        assertEquals(42.0, parseNumber("42.0"), 0.0);
        assertEquals(42.0, parseNumber("042.0"), 0.0);
        assertEquals(42.0, parseNumber("42.00"), 0.0);
    }

    // ---- Scientific notation ----

    public void testExponentPositive() {
        assertAgreesWithOracle("1.5e10");
        assertAgreesWithOracle("1.5e100");
        assertAgreesWithOracle("1.5e1000");
        assertAgreesWithOracle("1.5e10000");
        assertAgreesWithOracle("1.5e100000");
    }

    public void testExponentNegative() {
        assertAgreesWithOracle("1.5e-10");
        assertAgreesWithOracle("1.5e-100");
        assertAgreesWithOracle("1.5e-1000");
        assertAgreesWithOracle("1.5e-10000");
        assertAgreesWithOracle("1.5e-100000");
    }

    public void testExponentCapitalE() {
        assertAgreesWithOracle("1.5E10");
        assertAgreesWithOracle("1.5E100");
        assertAgreesWithOracle("1.5E1000");
        assertAgreesWithOracle("1.5E10000");
        assertAgreesWithOracle("1.5E100000");
        assertAgreesWithOracle("1.5E-10");
        assertAgreesWithOracle("1.5E-100");
        assertAgreesWithOracle("1.5E-1000");
        assertAgreesWithOracle("1.5E-10000");
        assertAgreesWithOracle("1.5E-100000");
    }

    public void testExponentWithPlus() {
        assertAgreesWithOracle("1.5e+10");
        assertAgreesWithOracle("1.5e+100");
        assertAgreesWithOracle("1.5e+1000");
        assertAgreesWithOracle("1.5e+10000");
        assertAgreesWithOracle("1.5E+10");
        assertAgreesWithOracle("1.5E+100");
        assertAgreesWithOracle("1.5E+1000");
        assertAgreesWithOracle("1.5E+10000");
    }

    // ---- Range extremes ----

    public void testMaxFiniteDouble() {
        assertAgreesWithOracle("1.7976931348623157e308");
    }

    public void testVerySmallSubnormal() {
        assertAgreesWithOracle("5e-324");
    }

    public void testSmallestNormal() {
        assertAgreesWithOracle("2.2250738585072014e-308");
    }

    public void testNegativeValues() {
        assertAgreesWithOracle("-3.14");
        assertAgreesWithOracle("-1e10");
    }

    public void testLargeInteger() {
        assertAgreesWithOracle("99999999999999999.0");
    }

    // Lexical form must not change the parsed bit pattern.
    public void testTrailingZeros() {
        assertAgreesWithOracle("1.50000");
    }

    public void testLeadingFractionalZeros() {
        assertAgreesWithOracle("0.001");
    }

    public void testManyDecimalPlaces() {
        assertAgreesWithOracle("3.141592653589793");
    }

    // ---- Overflow and underflow (±Infinity, ±0.0) ----

    // Exponent above 308 → +Infinity.
    public void testPositiveOverflowToInfinity() {
        double result = parseNumber("1e309");
        assertPositiveInfinity(result);
        assertSameBits(Double.parseDouble("1e309"), result);
    }

    public void testNegativeOverflowToInfinity() {
        double result = parseNumber("-1e309");
        assertNegativeInfinity(result);
        assertSameBits(Double.parseDouble("-1e309"), result);
    }

    // Exponent below -342 → +0.0.
    public void testPositiveUnderflowToZero() {
        double result = parseNumber("1e-400");
        assertPositiveZero(result);
        assertSameBits(Double.parseDouble("1e-400"), result);
    }

    public void testNegativeUnderflowToNegativeZero() {
        double result = parseNumber("-1e-400");
        assertNegativeZero(result);
        assertSameBits(Double.parseDouble("-1e-400"), result);
    }

    public void testOverflowAtExponentThreshold() {
        assertAgreesWithOracle("1e308");
    }

    // Dot in mantissa pushes digit span past 19 even though sig digits are fewer.
    public void testNearOverflowLongDecimalMantissa() {
        assertAgreesWithOracle("9.999999999999999999e308");
    }

    public void testUnderflowAtExponentThreshold() {
        assertAgreesWithOracle("1e-342");
    }

    // ---- Subnormal boundary: rounds up to Double.MIN_NORMAL ----

    public void testSubnormalBoundaryRoundsToMinNormal() {
        double result = parseNumber("2.2250738585072013e-308");
        assertSameBits(Double.MIN_NORMAL, result);
        assertSameBits(Double.parseDouble("2.2250738585072013e-308"), result);
    }

    // ---- Exact integer boundary (2^53) ----

    public void testMaxExactlyRepresentableInteger() {
        assertAgreesWithOracle("9007199254740991.0");
    }

    public void testFirstNonExactlyRepresentableInteger() {
        assertAgreesWithOracle("9007199254740992.0");
    }

    // ---- Long mantissas (>19 significant digits) ----

    public void testLongDecimalMantissaWithExponent() {
        assertAgreesWithOracle("1.234567890123456789012345678901234567890e10");
    }

    public void testLongIntegerWithTrailingFractionalZeros() {
        assertAgreesWithOracle("100000000000000000000.000000");
    }

    public void testLongMantissaWithoutExponent() {
        assertAgreesWithOracle("3.141592653589793238462643383279502884197");
    }

    // ---- Same oracle value, different lexical forms ----

    public void testSameOracleValueViaCompactAndExpandedForms() {
        String compact = "1e20";
        // Expanded form: 21 significant digits before the decimal point.
        String expanded = "100000000000000000000.0";
        double oracle = Double.parseDouble(compact);
        assertSameBits(oracle, parseNumber(compact));
        assertSameBits(oracle, parseNumber(expanded));
    }

    public void testSameOracleValueScientificVsExpandedPi() {
        String compact = "3.141592653589793e0";
        // Expanded form: >19 significant digits after the decimal point.
        String expanded = "3.141592653589793238462643383279502884197";
        double oracle = Double.parseDouble(compact);
        assertSameBits(oracle, parseNumber(compact));
        assertSameBits(oracle, parseNumber(expanded));
    }

    // ---- Oracle sweeps ----

    public void testOracleSweepSubnormals() {
        for (int i = 0; i < 1000; i++) {
            double original = randomDoubleBetween(Double.MIN_VALUE, Double.MIN_NORMAL, false);
            assertAgreesWithOracle(Double.toString(original));
        }
    }

    public void testOracleSweepNearOverflow() {
        List<String> samples = List.of(
            "1.7976931348623155e308",
            "1.7976931348623157e308",
            "1e308",
            "9.999999999999999e307",
            "1.0e309",
            "-1.0e309"
        );
        for (String sample : samples) {
            assertAgreesWithOracle(sample);
        }
    }

    public void testOracleSweepNearUnderflow() {
        List<String> samples = List.of(
            "5e-324",
            "1e-323",
            "2.2250738585072014e-308",
            "2.2250738585072013e-308",
            "1e-342",
            "1e-400",
            "-1e-400"
        );
        for (String sample : samples) {
            assertAgreesWithOracle(sample);
        }
    }

    // Mix of compact scientific, long mantissas, and random Double.toString samples.
    public void testOracleSweepMixedLexicalForms() {
        List<String> samples = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            double original = randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE / 2, true);
            samples.add(Double.toString(original));
        }
        samples.add("1.234567890123456789012345678901234567890e5");
        samples.add("9999999999999999999.9999999999999999999");
        samples.add("0.000000000000000000000000000000000000012345678901234567890123456789");

        for (String sample : samples) {
            assertAgreesWithOracle(sample);
        }
    }

    // Infra

    /**
     * Arguments mirroring {@link SimdJsonDirectWalker#handleFloatingPoint}.
     */
    private record WalkerNumberArgs(
        byte[] buffer,
        int startIdx,
        boolean negative,
        int digitsStartIdx,
        int digitCount,
        long digits,
        long exponent
    ) {}

    // Build walker-style args from a decimal string, then invoke DoubleParser.
    private double parseNumber(String numberStr) {
        return parseWalkerArgs(buildWalkerArgs(numberStr));
    }

    // Invoke parser.parse with pre-built walker args (production call shape).
    private double parseWalkerArgs(WalkerNumberArgs args) {
        return parser.parse(args.buffer, args.startIdx, args.negative, args.digitsStartIdx, args.digitCount, args.digits, args.exponent);
    }

    // Mirrors SimdJsonDirectWalker.handleFloatingPoint digit/exponent accumulation.
    // Appends a random JSON delimiter so slow-path re-parsing can read past the exponent
    // without running off the end of the buffer.
    private WalkerNumberArgs buildWalkerArgs(String numberStr) {
        String delimiter = randomFrom(",", "}", "]", " ", "\n", "\t");
        byte[] buffer = numberStr.concat(delimiter).getBytes(UTF_8);
        int startIdx = 0;
        boolean negative = buffer[startIdx] == '-';
        int digitsStartIdx = negative ? startIdx + 1 : startIdx;
        int pos = digitsStartIdx;

        long digits = 0;
        while (pos < buffer.length && buffer[pos] >= '0' && buffer[pos] <= '9') {
            digits = digits * 10 + (buffer[pos] - '0');
            pos++;
        }

        long exponent = 0;
        int digitCountEnd = pos;
        if (pos < buffer.length && buffer[pos] == '.') {
            pos++;
            int fracStart = pos;
            while (pos < buffer.length && buffer[pos] >= '0' && buffer[pos] <= '9') {
                digits = digits * 10 + (buffer[pos] - '0');
                pos++;
            }
            exponent = fracStart - pos;
            digitCountEnd = pos;
        }

        if (pos < buffer.length && (buffer[pos] == 'e' || buffer[pos] == 'E')) {
            pos++;
            boolean expNeg = false;
            if (pos < buffer.length && buffer[pos] == '-') {
                expNeg = true;
                pos++;
            } else if (pos < buffer.length && buffer[pos] == '+') {
                pos++;
            }
            long exp = 0;
            while (pos < buffer.length && buffer[pos] >= '0' && buffer[pos] <= '9') {
                exp = exp * 10 + (buffer[pos] - '0');
                pos++;
            }
            exponent += expNeg ? -exp : exp;
        }

        int digitCount = digitCountEnd - digitsStartIdx;
        return new WalkerNumberArgs(buffer, startIdx, negative, digitsStartIdx, digitCount, digits, exponent);
    }

    // Bit-exact check vs Double.parseDouble: parsing is deterministic (not approximate), and
    // epsilon/== miss signed zero and last-bit rounding differences.
    private static void assertSameBits(double expected, double actual) {
        assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
    }

    private static void assertPositiveInfinity(double value) {
        assertTrue(Double.isInfinite(value));
        assertFalse(Double.isNaN(value));
        assertTrue(value > 0);
        assertEquals(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY), Double.doubleToRawLongBits(value));
    }

    private static void assertNegativeInfinity(double value) {
        assertTrue(Double.isInfinite(value));
        assertFalse(Double.isNaN(value));
        assertTrue(value < 0);
        assertEquals(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY), Double.doubleToRawLongBits(value));
    }

    private static void assertPositiveZero(double value) {
        assertEquals(0.0, value, 0.0);
        assertEquals(0L, Double.doubleToRawLongBits(value));
    }

    private static void assertNegativeZero(double value) {
        assertEquals(-0.0, value, 0.0);
        assertTrue(Double.doubleToRawLongBits(value) < 0);
    }

    private void assertAgreesWithOracle(String numberStr) {
        assertSameBits(Double.parseDouble(numberStr), parseNumber(numberStr));
    }

    private void assertPositiveZeroLexical(String numberStr) {
        double parsed = parseNumber(numberStr);
        assertSameBits(Double.parseDouble(numberStr), parsed);
        assertPositiveZero(parsed);
    }

    private void assertNegativeZeroLexical(String unsignedForm) {
        String numberStr = "-" + unsignedForm;
        double parsed = parseNumber(numberStr);
        assertSameBits(Double.parseDouble(numberStr), parsed);
        assertNegativeZero(parsed);
    }
}
