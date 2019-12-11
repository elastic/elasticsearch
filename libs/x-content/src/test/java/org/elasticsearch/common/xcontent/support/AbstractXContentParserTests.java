/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;

public class AbstractXContentParserTests extends ESTestCase {

    // Simple yet super slow implementation
    private static boolean slowIsDouble(char[] chars, int charsOff, int charsLen) {
        try {
            BigDecimal bigDec = new BigDecimal(chars, charsOff, charsLen);
            double asDouble = bigDec.doubleValue();
            if (Double.isFinite(asDouble) == false) {
                return false;
            }
            // Don't use equals since it returns false for decimals that have the
            // same value but different scales.
            return bigDec.compareTo(new BigDecimal(Double.toString(asDouble))) == 0;
        } catch (NumberFormatException e) {
            return true;
        }
    }

    private static boolean isDouble(String s) {
        char[] chars = s.toCharArray();
        final boolean isDouble = slowIsDouble(chars, 0, chars.length);
        assertEquals(isDouble, AbstractXContentParser.isDouble(chars, 0, chars.length));
        assertEquals(isDouble, AbstractXContentParser.slowIsDouble(chars, 0, chars.length));
        return isDouble;
    }

    public void testIsDouble() {
        assertTrue(isDouble("0"));
        assertTrue(isDouble("1"));
        assertTrue(isDouble("0.0"));
        assertTrue(isDouble("1.0"));
        assertTrue(isDouble("-0.0"));
        assertTrue(isDouble("-1.0"));
        assertTrue(isDouble("1E308"));
        assertFalse(isDouble("2E308"));
        assertTrue(isDouble("-1E308"));
        assertFalse(isDouble("-2E308"));
        assertTrue(isDouble("0.00000000000000002"));
        assertFalse(isDouble("4.00000000000000002"));
        assertTrue(isDouble("234567891234567"));
        assertFalse(isDouble("23456789123456789"));
        assertTrue(isDouble(Double.toString(Double.MIN_VALUE)));
        assertTrue(isDouble(Double.toString(-Double.MIN_VALUE)));
        assertTrue(isDouble(Double.toString(Double.MIN_NORMAL)));
        assertTrue(isDouble(Double.toString(-Double.MIN_NORMAL)));
        assertTrue(isDouble(Double.toString(Double.MAX_VALUE)));
        assertTrue(isDouble(Double.toString(-Double.MAX_VALUE)));
        assertFalse(isDouble(BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(Math.ulp(Double.MAX_VALUE))).toString()));
        assertFalse(isDouble(BigDecimal.valueOf(-Double.MAX_VALUE).subtract(BigDecimal.valueOf(Math.ulp(Double.MAX_VALUE))).toString()));
        assertTrue(isDouble(Double.toString(Double.POSITIVE_INFINITY)));
        assertTrue(isDouble(Double.toString(Double.NaN)));
        assertTrue(isDouble(Double.toString(Double.NEGATIVE_INFINITY)));
        for (int i = 0; i < 1000000; ++i) {
            double d = Double.longBitsToDouble(randomLong());
            if (Double.isFinite(d)) {
                assertTrue(isDouble(Double.toString(d)));
                isDouble(Double.toString(d) + randomInt(9));
            }
        }
    }

    public void testGetBase10Exponent() {
        assertEquals(0, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(0)));
        assertEquals(0, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(1)));
        assertEquals(0, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(5)));
        assertEquals(0, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(50, 1))); // 50*10^-1
        assertEquals(2, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(500.)));
        assertEquals(-2, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(.05)));
        assertEquals(308, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(Double.MAX_VALUE)));
        assertEquals(-308, AbstractXContentParser.getBase10Exponent(BigDecimal.valueOf(Double.MIN_NORMAL)));
    }
}
