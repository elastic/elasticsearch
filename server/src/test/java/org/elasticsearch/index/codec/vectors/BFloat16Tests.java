/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class BFloat16Tests extends ESTestCase {

    public void testRoundToEven() {
        int exp = 0b001111110;  // to create floating numbers around 1.0

        // exact bfloat16 value
        float bfloat16 = construct(exp, 0b1111001_00000000_00000000);
        assertRounding(bfloat16, bfloat16);

        // some FP examples
        assertRounding(1.003f, 1.0f);
        assertRounding(1.004f, 1.0078125f);

        // round down
        assertRounding(construct(exp, 0b0000001_01111111_11111111), construct(exp, 0b0000001_00000000_00000000));

        // round up
        assertRounding(construct(exp, 0b0000001_10000000_00000001), construct(exp, 0b0000010_00000000_00000000));

        // split down to even
        assertRounding(construct(exp, 0b000010_10000000_00000000), construct(exp, 0b000010_00000000_00000000));

        // split up to even
        assertRounding(construct(exp, 0b000001_10000000_00000000), construct(exp, 0b000010_00000000_00000000));

        // round up, overflowing into exponent
        assertRounding(construct(0b000111111, 0b1111111_10000000_00000000), construct(0b001000000, 0b0000000_00000000_00000000));

        // round up, overflowing from denormal to normal number
        assertRounding(construct(0b000000000, 0b1111111_10000000_00000000), construct(0b000000001, 0b0000000_00000000_00000000));

        // round to positive infinity
        assertThat(BFloat16.truncateToBFloat16(construct(0b011111110, 0b1111111_10000000_00000000)), equalTo(Float.POSITIVE_INFINITY));

        // round to negative infinity
        assertThat(BFloat16.truncateToBFloat16(construct(0b111111110, 0b1111111_10000000_00000000)), equalTo(Float.NEGATIVE_INFINITY));

        // round to zero
        assertRounding(construct(0b000000000, 0b0000000_10000000_00000000), 0f);

        // rounding the standard NaN value should be unchanged
        assertThat(Float.floatToRawIntBits(BFloat16.truncateToBFloat16(Float.NaN)), equalTo(Float.floatToRawIntBits(Float.NaN)));

        // you would expect this to be turned into infinity due to overflow, but instead
        // it stays a NaN with a different bit pattern due to using floatToIntBits rather than floatToRawIntBits
        // inside floatToBFloat16
        assertTrue(Float.isNaN(BFloat16.truncateToBFloat16(construct(0b011111111, 0b0000000_10000000_00000000))));
    }

    private static float construct(int exp, int mantissa) {
        assert (exp & 0xfffffe00) == 0;
        assert (mantissa & 0xf8000000) == 0;
        return Float.intBitsToFloat((exp << 23) | mantissa);
    }

    private static void assertRounding(float value, float expectedRounded) {
        assert (Float.floatToIntBits(expectedRounded) & 0xffff) == 0;

        // rounded float value to check should be close to input value
        // this checks the bit representations in the tests are actually sensible
        assertThat(Math.abs(value - expectedRounded), lessThan(0.004f));

        float rounded = BFloat16.truncateToBFloat16(value);

        assertEquals(
            value + " rounded to " + rounded + ", not " + expectedRounded,
            Float.floatToIntBits(expectedRounded),
            Float.floatToIntBits(rounded)
        );

        // there should not be a closer bfloat16 value (comparing using FP math) than the expected rounded value
        float delta = Math.abs(value - rounded);
        float higherValue = Float.intBitsToFloat(Float.floatToIntBits(rounded) + 0x10000);
        assertThat(Math.abs(value - higherValue), greaterThanOrEqualTo(delta));

        float lowerValue = Float.intBitsToFloat(Float.floatToIntBits(rounded) - 0x10000);
        assertThat(Math.abs(value - lowerValue), greaterThanOrEqualTo(delta));
    }
}
