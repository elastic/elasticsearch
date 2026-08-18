/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

/**
 * Direct unit tests for {@link GlobExpander#isHiddenObject}. The method is exercised indirectly by the
 * expansion tests in {@code GlobExpanderTests}, but keeping a focused table test here makes edge-case
 * coverage cheap to maintain and catches off-by-one errors in the segment loop before they surface in
 * integration tests.
 */
public class GlobExpanderIsHiddenObjectTests extends ESTestCase {

    // -- directory placeholders (paths ending in '/') --

    public void testTrailingSlashIsHidden() {
        assertTrue(GlobExpander.isHiddenObject("subdir/"));
        assertTrue(GlobExpander.isHiddenObject("_delta_log/"));
        assertTrue(GlobExpander.isHiddenObject("/"));
    }

    // -- underscore-prefixed names (no '=' → not a Hive partition segment) --

    public void testUnderscorePrefixedNameIsHidden() {
        assertTrue(GlobExpander.isHiddenObject("_SUCCESS"));
        assertTrue(GlobExpander.isHiddenObject("_metadata"));
        assertTrue(GlobExpander.isHiddenObject("_"));
    }

    public void testUnderscorePrefixedDirectoryIsHidden() {
        assertTrue(GlobExpander.isHiddenObject("_delta_log/00000000000000000001.json"));
        assertTrue(GlobExpander.isHiddenObject("_temporary/part-00001.parquet"));
        assertTrue(GlobExpander.isHiddenObject("year=2024/_hidden/file.parquet"));
    }

    // -- underscore-prefixed Hive partition directories (contain '=' → not hidden) --

    public void testUnderscorePrefixedHivePartitionIsNotHidden() {
        // Hive partition directories use key=value; a '_'-prefixed key is valid (e.g. _index=alpha).
        assertFalse(GlobExpander.isHiddenObject("_index=alpha/part1.csv"));
        assertFalse(GlobExpander.isHiddenObject("_index=beta/part1.parquet"));
        assertFalse(GlobExpander.isHiddenObject("year=2024/_index=alpha/data.parquet"));
    }

    // -- dot-prefixed names --

    public void testDotPrefixedNameIsHidden() {
        assertTrue(GlobExpander.isHiddenObject(".part-r-00001.parquet.crc"));
        assertTrue(GlobExpander.isHiddenObject("."));
    }

    public void testDotPrefixedDirectoryIsHidden() {
        assertTrue(GlobExpander.isHiddenObject(".hidden/file.parquet"));
        assertTrue(GlobExpander.isHiddenObject("year=2024/.git/config"));
    }

    // -- ordinary data paths (not hidden) --

    public void testDataPathIsNotHidden() {
        assertFalse(GlobExpander.isHiddenObject("file.parquet"));
        assertFalse(GlobExpander.isHiddenObject("year=2024/data.parquet"));
        assertFalse(GlobExpander.isHiddenObject("a/b/c/d.parquet"));
        assertFalse(GlobExpander.isHiddenObject("part-r-00001.parquet"));
    }

    // -- edge cases --

    public void testEmptyStringIsNotHidden() {
        assertFalse(GlobExpander.isHiddenObject(""));
    }

    public void testSegmentThatIsExactlyUnderscore() {
        assertTrue(GlobExpander.isHiddenObject("_"));
        assertTrue(GlobExpander.isHiddenObject("a/_/b"));
    }

    public void testSegmentThatIsExactlyDot() {
        assertTrue(GlobExpander.isHiddenObject("."));
        assertTrue(GlobExpander.isHiddenObject("a/./b"));
    }

    /** Consecutive slashes produce empty segments between them; those must not be considered hidden. */
    public void testConsecutiveSlashesAreNotHidden() {
        assertFalse(GlobExpander.isHiddenObject("a//b"));
    }

    /** A path ending in '/' is a directory placeholder regardless of segment content. */
    public void testNonHiddenNameWithTrailingSlashIsStillHidden() {
        assertTrue(GlobExpander.isHiddenObject("normaldir/"));
    }
}
