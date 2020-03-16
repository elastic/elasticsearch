/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringUtils.substringSlice;

public class StringUtilsTests extends ESTestCase {

    public void testSubstringSlicePositive() throws Exception {
        String str = randomAlphaOfLength(10);
        assertEquals(str.substring(1, 7), substringSlice(str, 1, 7));
    }

    public void testSubstringSliceNegative() throws Exception {
        String str = randomAlphaOfLength(10);
        assertEquals(str.substring(5, 9), substringSlice(str, -5, -1));
    }

    public void testSubstringSliceNegativeOverLength() throws Exception {
        String str = randomAlphaOfLength(10);
        assertEquals(str.substring(5, 9), substringSlice(str, -15, -11));
    }

    public void testSubstringRandomSlicePositive() throws Exception {
        String str = randomAlphaOfLength(10);
        int start = randomInt(5);
        int end = start + randomInt(3);
        assertEquals(str.substring(start, end), substringSlice(str, start, end));
    }

    public void testSubstringRandomSliceNegative() throws Exception {
        String str = randomAlphaOfLength(10);
        int end = 1 + randomInt(3);
        int start = end + randomInt(5);
        assertEquals(str.substring(10 - start, 10 - end), substringSlice(str, -start, -end));
    }

    public void testSubstringRandomSliceSameStartEnd() throws Exception {
        String str = randomAlphaOfLength(10);
        int start = randomInt();
        assertEquals("", substringSlice(str, start, start));
    }
}
