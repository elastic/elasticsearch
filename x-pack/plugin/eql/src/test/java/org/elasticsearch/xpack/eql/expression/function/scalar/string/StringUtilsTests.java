/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringUtils.stringContains;
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringUtils.substringSlice;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;
import static org.hamcrest.Matchers.equalTo;

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

    public void testBetweenNullOrEmptyString() throws Exception {
        String left = randomAlphaOfLength(10);
        String right = randomAlphaOfLength(10);
        boolean greedy = randomBoolean();
        boolean caseSensitive = randomBoolean();

        String string = randomBoolean() ? null : EMPTY;
        assertThat(StringUtils.between(string, left, right, greedy, caseSensitive), equalTo(string));
    }

    public void testBetweenEmptyNullLeftRight() throws Exception {
        String string = randomAlphaOfLength(10);
        String left = randomBoolean() ? null : "";
        String right = randomBoolean() ? null : "";
        boolean greedy = randomBoolean();
        boolean caseSensitive = randomBoolean();
        assertThat(StringUtils.between(string, left, right, greedy, caseSensitive), equalTo(string));
    }

    // Test from EQL doc https://eql.readthedocs.io/en/latest/query-guide/functions.html
    public void testBetweenBasicEQLExamples() {
        assertThat(StringUtils.between("welcome to event query language", " ", " ", false, false),
                equalTo("to"));
        assertThat(StringUtils.between("welcome to event query language", " ", " ", true, false),
                equalTo("to event query"));
        assertThat(StringUtils.between("System Idle Process", "s", "e", true, false),
                equalTo("ystem Idle Proc"));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "dev", ".json", false, false),
                equalTo("\\TestLogs\\something"));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "dev", ".json", true, false),
                equalTo("\\TestLogs\\something"));

        assertThat(StringUtils.between("System Idle Process", "s", "e", false, false),
                equalTo("yst"));


        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "dev", ".json", false, true),
                equalTo("\\TestLogs\\something"));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "Test", ".json", false, true),
                equalTo("Logs\\something"));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "test", ".json", false, true),
                equalTo(""));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "dev", ".json", true, true),
                equalTo("\\TestLogs\\something"));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "Test", ".json", true, true),
                equalTo("Logs\\something"));

        assertThat(StringUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "test", ".json", true, true),
                equalTo(""));

        assertThat(StringUtils.between("System Idle Process", "S", "e", false, true),
                equalTo("yst"));

        assertThat(StringUtils.between("System Idle Process", "Y", "e", false, true),
                equalTo(""));
    }

    public void testStringContainsWithNullOrEmpty() {
        assertFalse(stringContains(null, null, true));
        assertFalse(stringContains(null, "", true));
        assertFalse(stringContains("", null, true));

        assertFalse(stringContains(null, null, false));
        assertFalse(stringContains(null, "", false));
        assertFalse(stringContains("", null, false));
    }

    public void testStringContainsWithRandomCaseSensitive() throws Exception {
        String substring = randomAlphaOfLength(10);
        String string = randomValueOtherThan(substring, () -> randomAlphaOfLength(10))
            + substring
            + randomValueOtherThan(substring, () -> randomAlphaOfLength(10));
        assertTrue(stringContains(string, substring, true));
    }

    public void testStringContainsWithRandomCaseInsensitive() throws Exception {
        String substring = randomAlphaOfLength(10);
        String subsChanged = substring.toUpperCase(Locale.ROOT);
        String string = randomValueOtherThan(subsChanged, () -> randomAlphaOfLength(10))
            + subsChanged
            + randomValueOtherThan(subsChanged, () -> randomAlphaOfLength(10));
        assertTrue(stringContains(string, substring, false));

        substring = randomAlphaOfLength(10);
        subsChanged = substring.toLowerCase(Locale.ROOT);
        string = randomValueOtherThan(subsChanged, () -> randomAlphaOfLength(10))
            + subsChanged
            + randomValueOtherThan(subsChanged, () -> randomAlphaOfLength(10));
        assertTrue(stringContains(string, substring, false));
    }
}
