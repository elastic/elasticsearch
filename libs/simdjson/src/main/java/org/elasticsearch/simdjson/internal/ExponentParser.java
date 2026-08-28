/*
 * @notice
 *
 * Based on a modification of https://github.com/simdjson/simdjson-java,
 * licensed under the Apache License, Version 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.simdjson.JsonParsingException;

class ExponentParser {

    private final ExponentParsingResult result = new ExponentParsingResult();

    static boolean isExponentIndicator(byte b) {
        return 'e' == b || 'E' == b;
    }

    ExponentParsingResult parse(byte[] buffer, int currentIdx, long exponent) {
        boolean negative = '-' == buffer[currentIdx];
        if (negative || '+' == buffer[currentIdx]) {
            currentIdx++;
        }
        int exponentStartIdx = currentIdx;

        long parsedExponent = 0;
        byte digit = convertCharacterToDigit(buffer[currentIdx]);
        while (digit >= 0 && digit <= 9) {
            parsedExponent = 10 * parsedExponent + digit;
            currentIdx++;
            digit = convertCharacterToDigit(buffer[currentIdx]);
        }

        if (exponentStartIdx == currentIdx) {
            throw new JsonParsingException("Invalid number. Exponent indicator has to be followed by a digit.");
        }
        // Long.MAX_VALUE = 9223372036854775807 (19 digits). Therefore, any number with <= 18 digits can be safely
        // stored in a long without causing an overflow.
        int maxDigitCountLongCanAccommodate = 18;
        if (currentIdx > exponentStartIdx + maxDigitCountLongCanAccommodate) {
            // Potentially, we have an overflow here. We try to skip leading zeros.
            while (buffer[exponentStartIdx] == '0') {
                exponentStartIdx++;
            }
            if (currentIdx > exponentStartIdx + maxDigitCountLongCanAccommodate) {
                // We still have more digits than a long can safely accommodate.
                //
                // The largest finite number that can be represented in binary64 is (1-2^-53) * 2^1024, which is about
                // 1.798e308, and the smallest non-zero number is 2^-1074, roughly 4.941e-324. So, we might, potentially,
                // care only about numbers with explicit exponents falling within the range of [-324, 308], and return
                // either zero or infinity for everything outside of this range.However, we have to take into account
                // the fractional part of the parsed number. This part can potentially cancel out the value of the
                // explicit exponent. For example, 1000e-325 (1 * 10^3 * 10^-325 = 1 * 10^-322) is not equal to zero
                // despite the explicit exponent being less than -324.
                //
                // Let's consider a scenario where the explicit exponent is greater than 999999999999999999. As long as
                // the fractional part has <= 999999999999999690 digits, it doesn't matter whether we take
                // 999999999999999999 or its actual value as the explicit exponent. This is due to the fact that the
                // parsed number is infinite anyway (w * 10^-q * 10^999999999999999999 > (1-2^-53) * 2^1024, 0 < w < 10,
                // 0 <= q <= 999999999999999690). Similarly, in a scenario where the explicit exponent is less than
                // -999999999999999999, as long as the fractional part has <= 999999999999999674 digits, we can safely
                // take 999999999999999999 as the explicit exponent, given that the parsed number is zero anyway
                // (w * 10^q * 10^-999999999999999999 < 2^-1074, 0 < w < 10, 0 <= q <= 999999999999999674)
                //
                // Note that if the fractional part had 999999999999999674 digits, the JSON size would need to be
                // 999999999999999674 bytes, which is approximately ~888 PiB. Consequently, it's reasonable to assume
                // that the fractional part contains no more than 999999999999999674 digits.
                parsedExponent = 999999999999999999L;
            }
        }
        // Note that we don't check if 'exponent' has overflowed after the following addition. This is because we
        // know that the parsed exponent falls within the range of [-999999999999999999, 999999999999999999]. We also
        // assume that 'exponent' before the addition is within the range of [-9223372036854775808, 9223372036854775807].
        // This assumption should always be valid as the value of 'exponent' is constrained by the size of the JSON input.
        exponent += negative ? -parsedExponent : parsedExponent;
        return result.of(exponent, currentIdx);
    }

    private static byte convertCharacterToDigit(byte b) {
        return (byte) (b - '0');
    }

    static class ExponentParsingResult {

        private long exponent;
        private int currentIdx;

        ExponentParsingResult of(long exponent, int currentIdx) {
            this.exponent = exponent;
            this.currentIdx = currentIdx;
            return this;
        }

        long exponent() {
            return exponent;
        }

        int currentIdx() {
            return currentIdx;
        }
    }
}
