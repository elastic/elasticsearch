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

    private static boolean isDouble(String s) {
        char[] chars = s.toCharArray();
        boolean isDouble = AbstractXContentParser.isDouble(chars, 0, chars.length);
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
        assertFalse(isDouble("9E999"));
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
        for (int i = 0; i < 10000; ++i) {
            double d = randomDouble();
            assertTrue(isDouble(Double.toString(d)));
        }
    }

}
