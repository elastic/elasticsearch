/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
/*
 * @(#)FastDoubleParser.java
 * Copyright 2021. Werner Randelshofer, Switzerland. MIT License.
 */

//package ch.randelshofer.fastdoubleparser;
package com.elasticsearch.jdk.boot.math;

/**
 * This is a C++ to Java port of Daniel Lemire's fast_double_parser.
 * <p>
 * The code has been changed, so that it parses the same syntax as
 * {@link Double#parseDouble(String)}.
 * <p>
 * References:
 * <dl>
 *     <dt>Daniel Lemire, fast_double_parser, 4x faster than strtod.
 *     Apache License 2.0 or Boost Software License.</dt>
 *     <dd><a href="https://github.com/lemire/fast_double_parser">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, fast_float number parsing library: 4x faster than strtod.
 *     Apache License 2.0.</dt>
 *     <dd><a href="https://github.com/fastfloat/fast_float">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, Number Parsing at a Gigabyte per Second,
 *     Software: Practice and Experience 51 (8), 2021.
 *     arXiv.2101.11408v3 [cs.DS] 24 Feb 2021</dt>
 *     <dd><a href="https://arxiv.org/pdf/2101.11408.pdf">arxiv.org</a></dd>
 * </dl>
 */
public class FastDoubleParser {
    private final static long MINIMAL_NINETEEN_DIGIT_INTEGER = 1000_00000_00000_00000L;
    private final static int MINIMAL_EIGHT_DIGIT_INTEGER = 10_000_000;
    /**
     * Special value in {@link #CHAR_TO_HEX_MAP} for
     * the decimal point character.
     */
    private static final byte DECIMAL_POINT_CLASS = -4;
    /**
     * Special value in {@link #CHAR_TO_HEX_MAP} for
     * characters that are neither a hex digit nor
     * a decimal point character..
     */
    private static final byte OTHER_CLASS = -1;
    /**
     * A table of 128 entries or of entries up to including
     * character 'p' would suffice.
     * <p>
     * However for some reason, performance is best,
     * if this table has exactly 256 entries.
     */
    private static final byte[] CHAR_TO_HEX_MAP = new byte[256];

    static {
        for (char ch = 0; ch < CHAR_TO_HEX_MAP.length; ch++) {
            CHAR_TO_HEX_MAP[ch] = OTHER_CLASS;
        }
        for (char ch = '0'; ch <= '9'; ch++) {
            CHAR_TO_HEX_MAP[ch] = (byte) (ch - '0');
        }
        for (char ch = 'A'; ch <= 'F'; ch++) {
            CHAR_TO_HEX_MAP[ch] = (byte) (ch - 'A' + 10);
        }
        for (char ch = 'a'; ch <= 'f'; ch++) {
            CHAR_TO_HEX_MAP[ch] = (byte) (ch - 'a' + 10);
        }
        for (char ch = '.'; ch <= '.'; ch++) {
            CHAR_TO_HEX_MAP[ch] = DECIMAL_POINT_CLASS;
        }
    }

    /**
     * Prevents instantiation.
     */
    private FastDoubleParser() {

    }

    private static boolean isDigit(char c) {
        return '0' <= c && c <= '9';
    }

    private static NumberFormatException nan() {
        return new NumberFormatException("NaN value");
    }

    private static NumberFormatException newNumberFormatException(CharSequence str, int startIndex, int endIndex) {
        if (str.length() > 1024) {
            // str can be up to Integer.MAX_VALUE characters long
            return new NumberFormatException("For input string of length " + str.length());
        } else {
            return new NumberFormatException("For input string: \"" + str.subSequence(startIndex, endIndex) + "\"");
        }
    }

    /**
     * Convenience method for calling {@link #parseDouble(CharSequence, int, int)}.
     *
     * @param str the string to be parsed
     * @return the parsed double value
     * @throws NumberFormatException if the string can not be parsed
     */
    public static double parseDouble(CharSequence str) throws NumberFormatException {
        return parseDouble(str, 0, str.length());
    }

    /**
     * Returns a Double object holding the double value represented by the
     * argument string {@code str}.
     * <p>
     * This method can be used as a drop in for method
     * {@link Double#valueOf(String)}. (Assuming that the API of this method
     * has not changed since Java SE 16).
     * <p>
     * Leading and trailing whitespace characters in {@code str} are ignored.
     * Whitespace is removed as if by the {@link String#trim()} method;
     * that is, characters in the range [U+0000,U+0020].
     * <p>
     * The rest of {@code str} should constitute a FloatValue as described by the
     * lexical syntax rules shown below:
     * <blockquote>
     * <dl>
     * <dt><i>FloatValue:</i>
     * <dd><i>[Sign]</i> {@code NaN}
     * <dd><i>[Sign]</i> {@code Infinity}
     * <dd><i>[Sign] DecimalFloatingPointLiteral</i>
     * <dd><i>[Sign] HexFloatingPointLiteral</i>
     * <dd><i>SignedInteger</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexFloatingPointLiteral</i>:
     * <dd><i>HexSignificand BinaryExponent</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexSignificand:</i>
     * <dd><i>HexNumeral</i>
     * <dd><i>HexNumeral</i> {@code .}
     * <dd>{@code 0x} <i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * <dd>{@code 0X} <i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexSignificand:</i>
     * <dd><i>HexNumeral</i>
     * <dd><i>HexNumeral</i> {@code .}
     * <dd>{@code 0x} <i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * <dd>{@code 0X} <i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * </dl>
     *
     * <dl>
     * <dt><i>BinaryExponent:</i>
     * <dd><i>BinaryExponentIndicator SignedInteger</i>
     * </dl>
     *
     * <dl>
     * <dt><i>BinaryExponentIndicator:</i>
     * <dd>{@code p}
     * <dd>{@code P}
     * </dl>
     *
     * <dl>
     * <dt><i>DecimalFloatingPointLiteral:</i>
     * <dd><i>Digits {@code .} [Digits] [ExponentPart]</i>
     * <dd><i>{@code .} Digits [ExponentPart]</i>
     * <dd><i>Digits ExponentPart</i>
     * </dl>
     *
     * <dl>
     * <dt><i>ExponentPart:</i>
     * <dd><i>ExponentIndicator SignedInteger</i>
     * </dl>
     *
     * <dl>
     * <dt><i>ExponentIndicator:</i>
     * <dd><i>(one of)</i>
     * <dd><i>e E</i>
     * </dl>
     *
     * <dl>
     * <dt><i>SignedInteger:</i>
     * <dd><i>[Sign] Digits</i>
     * </dl>
     *
     * <dl>
     * <dt><i>Sign:</i>
     * <dd><i>(one of)</i>
     * <dd><i>+ -</i>
     * </dl>
     *
     * <dl>
     * <dt><i>Digits:</i>
     * <dd><i>Digit {Digit}</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexNumeral:</i>
     * <dd>{@code 0} {@code x} <i>HexDigits</i>
     * <dd>{@code 0} {@code X} <i>HexDigits</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexDigits:</i>
     * <dd><i>HexDigit {HexDigit}</i>
     * </dl>
     *
     * <dl>
     * <dt><i>HexDigit:</i>
     * <dd><i>(one of)</i>
     * <dd>{@code 0 1 2 3 4 5 6 7 8 9 a b c d e f A B C D E F}
     * </dl>
     * </blockquote>
     *
     * @param str    the string to be parsed
     * @param offset The index of the first character to parse
     * @param length The number of characters to parse
     * @return the parsed double value
     * @throws NumberFormatException if the string can not be parsed
     */
    public static double parseDouble(CharSequence str, int offset, int length) throws NumberFormatException {
        final int endIndex = offset + length;

        // Skip leading whitespace
        // -------------------
        int index = skipWhitespace(str, offset, endIndex);
        if (index == endIndex) {
            throw new NumberFormatException("empty String");
        }
        char ch = str.charAt(index);

        // Parse optional sign
        // -------------------
        final boolean isNegative = ch == '-';
        if (isNegative || ch == '+') {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            if (ch == 0) {
                throw newNumberFormatException(str, offset, endIndex);
            }
        }

        // Parse NaN or Infinity
        // ---------------------
        if (ch == 'N') {
            return parseNaN(str, index, endIndex);
        } else if (ch == 'I') {
            return parseInfinity(str, index, endIndex, isNegative);
        }

        // Parse optional leading zero
        // ---------------------------
        final boolean hasLeadingZero = ch == '0';
        if (hasLeadingZero) {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            if (ch == 'x' || ch == 'X') {
                return parseRestOfHexFloatingPointLiteral(str, index + 1, offset, endIndex, isNegative);
            }
        }

        return parseRestOfDecimalFloatLiteral(str, index, offset, endIndex, isNegative, hasLeadingZero);
    }

    private static double parseInfinity(CharSequence str, int startIndex, int endIndex, boolean negative) {
        if (startIndex + 7 < endIndex
            // && str.charAt(index) == 'I'
            && str.charAt(startIndex + 1) == 'n'
            && str.charAt(startIndex + 2) == 'f'
            && str.charAt(startIndex + 3) == 'i'
            && str.charAt(startIndex + 4) == 'n'
            && str.charAt(startIndex + 5) == 'i'
            && str.charAt(startIndex + 6) == 't'
            && str.charAt(startIndex + 7) == 'y') {
            startIndex = skipWhitespace(str, startIndex + 8, endIndex);
            if (startIndex < endIndex) {
                throw newNumberFormatException(str, startIndex, endIndex);
            }
            return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        } else {
            throw newNumberFormatException(str, startIndex, endIndex);
        }
    }

    private static double parseNaN(CharSequence str, int startIndex, int endIndex) {
        if (startIndex + 2 < endIndex
            // && str.charAt(index) == 'N'
            && str.charAt(startIndex + 1) == 'a'
            && str.charAt(startIndex + 2) == 'N') {

            startIndex = skipWhitespace(str, startIndex + 3, endIndex);
            if (startIndex < endIndex) {
                throw newNumberFormatException(str, startIndex, endIndex);
            }

            return Double.NaN;
        } else {
            throw newNumberFormatException(str, startIndex, endIndex);
        }
    }

    /**
     * Parses the following rules
     * (more rules are defined in {@link #parseDouble(CharSequence)}):
     * <dl>
     * <dt><i>RestOfDecimalFloatingPointLiteral</i>:
     * <dd><i>[Digits] {@code .} [Digits] [ExponentPart]</i>
     * <dd><i>{@code .} Digits [ExponentPart]</i>
     * <dd><i>[Digits] ExponentPart</i>
     * </dl>
     *
     * @param str            the input string
     * @param index          index to the first character of RestOfHexFloatingPointLiteral
     * @param startIndex     the start index of h
     * @param endIndex       the end index of the string
     * @param isNegative     if the resulting number is negative
     * @param hasLeadingZero if the digit '0' has been consumed
     * @return a double representation
     */
    private static double parseRestOfDecimalFloatLiteral(
        CharSequence str,
        int index,
        int startIndex,
        int endIndex,
        boolean isNegative,
        boolean hasLeadingZero
    ) {
        // Parse digits
        // ------------
        // Note: a multiplication by a constant is cheaper than an
        // arbitrary integer multiplication.
        long digits = 0;// digits is treated as an unsigned long
        int exponent = 0;
        final int indexOfFirstDigit = index;
        int virtualIndexOfPoint = -1;
        final int digitCount;
        char ch = 0;
        for (; index < endIndex; index++) {
            ch = str.charAt(index);
            if (isDigit(ch)) {
                // This might overflow, we deal with it later.
                digits = 10 * digits + ch - '0';
            } else if (ch == '.') {
                if (virtualIndexOfPoint != -1) {
                    throw newNumberFormatException(str, startIndex, endIndex);
                }
                virtualIndexOfPoint = index;
            } else {
                break;
            }
        }
        final int indexAfterDigits = index;
        if (virtualIndexOfPoint == -1) {
            digitCount = indexAfterDigits - indexOfFirstDigit;
            virtualIndexOfPoint = indexAfterDigits;
        } else {
            digitCount = indexAfterDigits - indexOfFirstDigit - 1;
            exponent = virtualIndexOfPoint - index + 1;
        }

        // Parse exponent number
        // ---------------------
        long exp_number = 0;
        final boolean hasExponent = (ch == 'e') || (ch == 'E');
        if (hasExponent) {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            boolean neg_exp = ch == '-';
            if (neg_exp || ch == '+') {
                ch = ++index < endIndex ? str.charAt(index) : 0;
            }
            if (!isDigit(ch)) {
                throw newNumberFormatException(str, startIndex, endIndex);
            }
            do {
                // Guard against overflow of exp_number
                if (exp_number < MINIMAL_EIGHT_DIGIT_INTEGER) {
                    exp_number = 10 * exp_number + ch - '0';
                }
                ch = ++index < endIndex ? str.charAt(index) : 0;
            } while (isDigit(ch));
            if (neg_exp) {
                exp_number = -exp_number;
            }
            exponent += exp_number;
        }

        // Skip trailing whitespace
        // ------------------------
        index = skipWhitespace(str, index, endIndex);
        if (index < endIndex || !hasLeadingZero && digitCount == 0 && str.charAt(virtualIndexOfPoint) != '.') {
            throw newNumberFormatException(str, startIndex, endIndex);
        }

        // Re-parse digits in case of a potential overflow
        // -----------------------------------------------
        final boolean isDigitsTruncated;
        int skipCountInTruncatedDigits = 0;// counts +1 if we skipped over the decimal point
        if (digitCount > 19) {
            digits = 0;
            for (index = indexOfFirstDigit; index < indexAfterDigits; index++) {
                ch = str.charAt(index);
                if (ch == '.') {
                    skipCountInTruncatedDigits++;
                } else {
                    if (Long.compareUnsigned(digits, MINIMAL_NINETEEN_DIGIT_INTEGER) < 0) {
                        digits = 10 * digits + ch - '0';
                    } else {
                        break;
                    }
                }
            }
            isDigitsTruncated = (index < indexAfterDigits);
        } else {
            isDigitsTruncated = false;
        }

        double result = FastDoubleMath.decFloatLiteralToDouble(
            index,
            isNegative,
            digits,
            exponent,
            virtualIndexOfPoint,
            exp_number,
            isDigitsTruncated,
            skipCountInTruncatedDigits
        );

        if (Double.isNaN(result)) {
            throw nan();
        }

        return result;
    }

    /**
     * Parses the following rules
     * (more rules are defined in {@link #parseDouble(CharSequence)}):
     * <dl>
     * <dt><i>RestOfHexFloatingPointLiteral</i>:
     * <dd><i>RestOfHexSignificand BinaryExponent</i>
     * </dl>
     *
     * <dl>
     * <dt><i>RestOfHexSignificand:</i>
     * <dd><i>HexDigits</i>
     * <dd><i>HexDigits</i> {@code .}
     * <dd><i>[HexDigits]</i> {@code .} <i>HexDigits</i>
     * </dl>
     *
     * @param str        the input string
     * @param index      index to the first character of RestOfHexFloatingPointLiteral
     * @param startIndex the start index of the string
     * @param endIndex   the end index of the string
     * @param isNegative if the resulting number is negative
     * @return a double representation
     */
    private static double parseRestOfHexFloatingPointLiteral(
        CharSequence str,
        int index,
        int startIndex,
        int endIndex,
        boolean isNegative
    ) {

        // Parse digits
        // ------------
        long digits = 0;// digits is treated as an unsigned long
        int exponent = 0;
        final int indexOfFirstDigit = index;
        int virtualIndexOfPoint = -1;
        final int digitCount;
        char ch = 0;
        for (; index < endIndex; index++) {
            ch = str.charAt(index);
            // Table look up is faster than a sequence of if-else-branches.
            int hexValue = ch > 127 ? OTHER_CLASS : CHAR_TO_HEX_MAP[ch];
            if (hexValue >= 0) {
                digits = (digits << 4) | hexValue;// This might overflow, we deal with it later.
            } else if (hexValue == DECIMAL_POINT_CLASS) {
                if (virtualIndexOfPoint != -1) {
                    throw newNumberFormatException(str, startIndex, endIndex);
                }
                virtualIndexOfPoint = index;
            } else {
                break;
            }
        }
        final int indexAfterDigits = index;
        if (virtualIndexOfPoint == -1) {
            digitCount = indexAfterDigits - indexOfFirstDigit;
            virtualIndexOfPoint = indexAfterDigits;
        } else {
            digitCount = indexAfterDigits - indexOfFirstDigit - 1;
            exponent = Math.min(virtualIndexOfPoint - index + 1, MINIMAL_EIGHT_DIGIT_INTEGER) * 4;
        }

        // Parse exponent number
        // ---------------------
        long exp_number = 0;
        final boolean hasExponent = (ch == 'p') || (ch == 'P');
        if (hasExponent) {
            ch = ++index < endIndex ? str.charAt(index) : 0;
            boolean neg_exp = ch == '-';
            if (neg_exp || ch == '+') {
                ch = ++index < endIndex ? str.charAt(index) : 0;
            }
            if (!isDigit(ch)) {
                throw newNumberFormatException(str, startIndex, endIndex);
            }
            do {
                // Guard against overflow of exp_number
                if (exp_number < MINIMAL_EIGHT_DIGIT_INTEGER) {
                    exp_number = 10 * exp_number + ch - '0';
                }
                ch = ++index < endIndex ? str.charAt(index) : 0;
            } while (isDigit(ch));
            if (neg_exp) {
                exp_number = -exp_number;
            }
            exponent += exp_number;
        }

        // Skip trailing whitespace
        // ------------------------
        index = skipWhitespace(str, index, endIndex);
        if (index < endIndex || digitCount == 0 && str.charAt(virtualIndexOfPoint) != '.' || !hasExponent) {
            throw newNumberFormatException(str, startIndex, endIndex);
        }

        // Re-parse digits in case of a potential overflow
        // -----------------------------------------------
        final boolean isDigitsTruncated;
        int skipCountInTruncatedDigits = 0;// counts +1 if we skipped over the decimal point
        if (digitCount > 16) {
            digits = 0;
            for (index = indexOfFirstDigit; index < indexAfterDigits; index++) {
                ch = str.charAt(index);
                // Table look up is faster than a sequence of if-else-branches.
                int hexValue = ch > 127 ? OTHER_CLASS : CHAR_TO_HEX_MAP[ch];
                if (hexValue >= 0) {
                    if (Long.compareUnsigned(digits, MINIMAL_NINETEEN_DIGIT_INTEGER) < 0) {
                        digits = (digits << 4) | hexValue;
                    } else {
                        break;
                    }
                } else {
                    skipCountInTruncatedDigits++;
                }
            }
            isDigitsTruncated = (index < indexAfterDigits);
        } else {
            isDigitsTruncated = false;
        }

        double d = FastDoubleMath.hexFloatLiteralToDouble(
            index,
            isNegative,
            digits,
            exponent,
            virtualIndexOfPoint,
            exp_number,
            isDigitsTruncated,
            skipCountInTruncatedDigits
        );

        if (Double.isNaN(d)) {
            throw nan();
        }

        return d;
    }

    private static int skipWhitespace(CharSequence str, int startIndex, int endIndex) {
        int index = startIndex;
        for (; index < endIndex; index++) {
            if (str.charAt(index) > 0x20) {
                break;
            }
        }
        return index;
    }
}
