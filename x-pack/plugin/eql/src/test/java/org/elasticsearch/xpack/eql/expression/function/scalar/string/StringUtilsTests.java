/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringUtils.substringSlice;

public class StringUtilsTests extends ESTestCase {

    public void testSubstringSlicePositive() {
        String str = randomAlphaOfLength(10);
        assertEquals(str.substring(1, 7), substringSlice(str, 1, 7));
    }

    public void testSubstringSliceNegative() {
        String str = randomAlphaOfLength(10);
        assertEquals(str.substring(5, 9), substringSlice(str, -5, -1));
    }

    public void testSubstringSliceNegativeOverLength() {
        String str = randomAlphaOfLength(10);
        assertEquals("", substringSlice(str, -15, -11));
    }

    public void testSubstringSlicePositiveOverLength() {
        String str = randomAlphaOfLength(10);
        assertEquals("", substringSlice(str, 11, 14));
    }

    public void testSubstringHigherEndThanStartNegative() {
        String str = randomAlphaOfLength(10);
        assertEquals("", substringSlice(str, -20, -11));
    }

    public void testSubstringRandomSlicePositive() {
        String str = randomAlphaOfLength(10);
        int start = randomInt(5);
        int end = start + randomInt(3);
        assertEquals(str.substring(start, end), substringSlice(str, start, end));
    }

    public void testSubstringRandomSliceNegative() {
        String str = randomAlphaOfLength(10);
        int end = 1 + randomInt(3);
        int start = end + randomInt(5);
        assertEquals(str.substring(10 - start, 10 - end), substringSlice(str, -start, -end));
    }

    public void testStartNegativeHigherThanLength() {
        String str = randomAlphaOfLength(10);
        int start = 10 + randomInt(10);
        assertEquals(str.substring(0, 10 - 1), substringSlice(str, -start, -1));
    }

    public void testEndHigherThanLength() {
        String str = randomAlphaOfLength(10);
        int end = 10 + randomInt(10);
        assertEquals(str, substringSlice(str, 0, end));
    }

    public void testSubstringRandomSliceSameStartEnd() {
        String str = randomAlphaOfLength(10);
        int start = randomInt();
        assertEquals("", substringSlice(str, start, start));
    }

    public void testNullValue() {
        assertNull(substringSlice(null, 0, 0));
    }
}
