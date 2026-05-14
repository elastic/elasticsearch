/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class BraceExpanderTests extends ESTestCase {

    // -- isBraceOnly --

    public void testIsBraceOnlySimple() {
        assertTrue(BraceExpander.isBraceOnly("{a,b}.parquet"));
    }

    public void testIsBraceOnlyWithStar() {
        assertFalse(BraceExpander.isBraceOnly("{a,b}/*.parquet"));
    }

    public void testIsBraceOnlyMultiGroup() {
        assertTrue(BraceExpander.isBraceOnly("{a,b}/{x,y}.csv"));
    }

    public void testIsBraceOnlyNoPattern() {
        assertFalse(BraceExpander.isBraceOnly("data.parquet"));
    }

    public void testIsBraceOnlyWithQuestionMark() {
        assertFalse(BraceExpander.isBraceOnly("{a,b}?.parquet"));
    }

    public void testIsBraceOnlyWithBracket() {
        assertFalse(BraceExpander.isBraceOnly("{a,b}[0-9].parquet"));
    }

    public void testIsBraceOnlyWithDoubleStar() {
        assertFalse(BraceExpander.isBraceOnly("{a,b}/" + "**/*.parquet"));
    }

    public void testIsBraceOnlyWithWildcardInsideBraces() {
        assertFalse(BraceExpander.isBraceOnly("{a*,b}.parquet"));
    }

    public void testIsBraceOnlyWithNestedBraces() {
        assertFalse(BraceExpander.isBraceOnly("{{{}"));
    }

    public void testIsBraceOnlyWithQuestionInsideBraces() {
        assertFalse(BraceExpander.isBraceOnly("{a?,b}.parquet"));
    }

    // -- expand --

    public void testExpandSingleGroup() {
        List<String> result = BraceExpander.expand("{a,b}.parquet", 100);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a.parquet", result.get(0));
        assertEquals("b.parquet", result.get(1));
    }

    public void testExpandMultipleGroups() {
        List<String> result = BraceExpander.expand("{a,b}/{x,y}.csv", 100);
        assertNotNull(result);
        assertEquals(4, result.size());
        assertTrue(result.contains("a/x.csv"));
        assertTrue(result.contains("a/y.csv"));
        assertTrue(result.contains("b/x.csv"));
        assertTrue(result.contains("b/y.csv"));
    }

    public void testExpandNoGroups() {
        List<String> result = BraceExpander.expand("data.parquet", 100);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("data.parquet", result.get(0));
    }

    public void testExpandExceedsCapReturnsNull() {
        StringBuilder pattern = new StringBuilder("{");
        for (int i = 0; i < 200; i++) {
            if (i > 0) pattern.append(',');
            pattern.append("file").append(i);
        }
        pattern.append("}.parquet");
        assertNull(BraceExpander.expand(pattern.toString(), 100));
    }

    public void testExpandHiveStyleSegments() {
        List<String> result = BraceExpander.expand("year={2024,2025}/month={1,2}/data.parquet", 100);
        assertNotNull(result);
        assertEquals(4, result.size());
        assertTrue(result.contains("year=2024/month=1/data.parquet"));
        assertTrue(result.contains("year=2024/month=2/data.parquet"));
        assertTrue(result.contains("year=2025/month=1/data.parquet"));
        assertTrue(result.contains("year=2025/month=2/data.parquet"));
    }

    public void testExpandSingleAlternative() {
        List<String> result = BraceExpander.expand("{only}.parquet", 100);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("only.parquet", result.get(0));
    }

    public void testExpandEmptyAlternative() {
        List<String> result = BraceExpander.expand("{a,}.parquet", 100);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a.parquet", result.get(0));
        assertEquals(".parquet", result.get(1));
    }

    public void testExpandThreeGroups() {
        List<String> result = BraceExpander.expand("{a,b}/{c,d}/{e,f}", 100);
        assertNotNull(result);
        assertEquals(8, result.size());
    }

    public void testExpandCapAtBoundary() {
        List<String> result = BraceExpander.expand("{a,b}/{c,d}", 4);
        assertNotNull(result);
        assertEquals(4, result.size());
    }

    public void testExpandCapExceededByOneReturnsNull() {
        assertNull(BraceExpander.expand("{a,b}/{c,d}/{e,f}", 7));
    }

    // -- numeric range expansion --

    public void testExpandNumericRangeSimple() {
        List<String> result = BraceExpander.expand("file-{1..5}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-1.parquet", "file-2.parquet", "file-3.parquet", "file-4.parquet", "file-5.parquet"), result);
    }

    public void testExpandNumericRangeZeroPadded() {
        List<String> result = BraceExpander.expand("file-{000..003}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-000.parquet", "file-001.parquet", "file-002.parquet", "file-003.parquet"), result);
    }

    public void testExpandNumericRangeZeroPaddedMixedWidths() {
        List<String> result = BraceExpander.expand("file-{08..12}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-08.parquet", "file-09.parquet", "file-10.parquet", "file-11.parquet", "file-12.parquet"), result);
    }

    public void testExpandNumericRangeDescending() {
        List<String> result = BraceExpander.expand("file-{5..1}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-5.parquet", "file-4.parquet", "file-3.parquet", "file-2.parquet", "file-1.parquet"), result);
    }

    public void testExpandNumericRangeSingleValue() {
        List<String> result = BraceExpander.expand("file-{7..7}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-7.parquet"), result);
    }

    public void testExpandNumericRangeExceedsCap() {
        assertNull(BraceExpander.expand("file-{1..200}.parquet", 100));
    }

    public void testExpandNumericRangeCombinedWithCommaGroup() {
        List<String> result = BraceExpander.expand("{a,b}/file-{1..3}.csv", 100);
        assertNotNull(result);
        assertEquals(6, result.size());
        assertTrue(result.contains("a/file-1.csv"));
        assertTrue(result.contains("a/file-2.csv"));
        assertTrue(result.contains("a/file-3.csv"));
        assertTrue(result.contains("b/file-1.csv"));
        assertTrue(result.contains("b/file-2.csv"));
        assertTrue(result.contains("b/file-3.csv"));
    }

    public void testExpandNumericRangeFromZero() {
        List<String> result = BraceExpander.expand("part-{0..2}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("part-0.parquet", "part-1.parquet", "part-2.parquet"), result);
    }

    public void testExpandNumericRangeNoPaddingWithoutLeadingZero() {
        List<String> result = BraceExpander.expand("file-{9..11}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-9.parquet", "file-10.parquet", "file-11.parquet"), result);
    }

    public void testExpandNumericRangePaddingWithLeadingZeroOnStart() {
        List<String> result = BraceExpander.expand("file-{09..11}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-09.parquet", "file-10.parquet", "file-11.parquet"), result);
    }

    public void testIsBraceOnlyWithNumericRange() {
        assertTrue(BraceExpander.isBraceOnly("file-{0..9}.parquet"));
    }

    public void testIsBraceOnlyWithNumericRangeAndStar() {
        assertFalse(BraceExpander.isBraceOnly("dir/*/file-{0..9}.parquet"));
    }

    public void testExpandNumericRangeNotNumeric() {
        List<String> result = BraceExpander.expand("{a..b}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("a..b.parquet"), result);
    }

    public void testExpandNumericRangeLargeZeroPadded() {
        List<String> result = BraceExpander.expand("shard-{000..099}.parquet", 200);
        assertNotNull(result);
        assertEquals(100, result.size());
        assertEquals("shard-000.parquet", result.get(0));
        assertEquals("shard-050.parquet", result.get(50));
        assertEquals("shard-099.parquet", result.get(99));
    }

    public void testExpandNumericRangeDescendingWithPadding() {
        List<String> result = BraceExpander.expand("file-{003..001}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-003.parquet", "file-002.parquet", "file-001.parquet"), result);
    }

    public void testExpandNumericRangeExactlyAtCap() {
        List<String> result = BraceExpander.expand("file-{1..100}.parquet", 100);
        assertNotNull(result);
        assertEquals(100, result.size());
        assertEquals("file-1.parquet", result.get(0));
        assertEquals("file-100.parquet", result.get(99));
    }

    public void testExpandCommaWinsOverRange() {
        List<String> result = BraceExpander.expand("{1,2..3}.parquet", 100);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1.parquet", result.get(0));
        assertEquals("2..3.parquet", result.get(1));
    }

    public void testExpandNumericRangeZeroToZero() {
        List<String> result = BraceExpander.expand("file-{0..0}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-0.parquet"), result);
    }

    public void testExpandNumericRangeDoubleZeroPadded() {
        List<String> result = BraceExpander.expand("file-{00..00}.parquet", 100);
        assertNotNull(result);
        assertEquals(List.of("file-00.parquet"), result);
    }
}
