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
}
