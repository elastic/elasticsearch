/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

public class GlobMatcherTests extends ESTestCase {

    public void testStarMatchesSingleSegment() {
        GlobMatcher m = new GlobMatcher("*.parquet");
        assertTrue(m.matches("file.parquet"));
        assertTrue(m.matches("data.parquet"));
        assertFalse(m.matches("dir/file.parquet"));
        assertFalse(m.matches("file.csv"));
    }

    public void testStarInMiddle() {
        GlobMatcher m = new GlobMatcher("data-*-output.parquet");
        assertTrue(m.matches("data-2024-output.parquet"));
        assertTrue(m.matches("data--output.parquet"));
        assertFalse(m.matches("data-a/b-output.parquet"));
    }

    public void testDoubleStarMatchesRecursive() {
        GlobMatcher m = new GlobMatcher("**/*.parquet");
        assertTrue(m.matches("file.parquet"));
        assertTrue(m.matches("a/file.parquet"));
        assertTrue(m.matches("a/b/c/file.parquet"));
        assertFalse(m.matches("file.csv"));
    }

    public void testQuestionMarkMatchesSingleChar() {
        GlobMatcher m = new GlobMatcher("file?.parquet");
        assertTrue(m.matches("file1.parquet"));
        assertTrue(m.matches("fileA.parquet"));
        assertFalse(m.matches("file.parquet"));
        assertFalse(m.matches("file12.parquet"));
    }

    public void testBraceAlternatives() {
        GlobMatcher m = new GlobMatcher("*.{parquet,csv}");
        assertTrue(m.matches("data.parquet"));
        assertTrue(m.matches("data.csv"));
        assertFalse(m.matches("data.json"));
    }

    public void testCharacterClass() {
        GlobMatcher m = new GlobMatcher("file[123].parquet");
        assertTrue(m.matches("file1.parquet"));
        assertTrue(m.matches("file2.parquet"));
        assertFalse(m.matches("file4.parquet"));
        assertFalse(m.matches("fileA.parquet"));
    }

    public void testNegatedCharacterClass() {
        GlobMatcher m = new GlobMatcher("file[!0-9].txt");
        assertTrue(m.matches("fileA.txt"));
        assertFalse(m.matches("file1.txt"));
    }

    @SuppressWarnings("RegexpMultiline")
    public void testNeedsRecursion() {
        assertTrue(new GlobMatcher("**/*.parquet").needsRecursion());
        assertTrue(new GlobMatcher("data/**" + "/file.csv").needsRecursion());
        assertFalse(new GlobMatcher("*.parquet").needsRecursion());
        assertFalse(new GlobMatcher("data/*.csv").needsRecursion());
    }

    public void testLiteralDotsEscaped() {
        GlobMatcher m = new GlobMatcher("file.parquet");
        assertTrue(m.matches("file.parquet"));
        assertFalse(m.matches("fileXparquet"));
    }

    public void testGlob() {
        assertEquals("*.parquet", new GlobMatcher("*.parquet").glob());
    }
}
