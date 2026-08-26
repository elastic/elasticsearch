/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * The gate on making non-data exclusion user-controllable: what the default settings drop must be exactly what the
 * fixed predicate they replaced dropped, so no dataset that resolves today resolves differently tomorrow.
 *
 * <p>The subject is the composition the listing loop applies — the unconditional directory-placeholder skip, then
 * {@link ExclusionConfig#DEFAULT}. The two are compared together because they are not separable: a segment name never
 * carries its trailing slash, so no name glob can express the placeholder rule, which is why that rule moved out of
 * the policy and into listing normalization.
 *
 * <p>The oracle below is a frozen verbatim copy of the pre-existing {@code GlobExpander.isHiddenObject}. It is the
 * specification, not an implementation detail: <b>never edit it</b>. Changing the convention means changing
 * {@link ExclusionConfig#DEFAULT} and watching this test fail, which is the point.
 */
public class ExclusionDefaultEquivalenceTests extends ESTestCase {

    private static final ExclusionConfig.Matchers DEFAULT_MATCHERS = ExclusionConfig.DEFAULT.compile();

    /** Frozen pre-#1765 predicate. DO NOT EDIT — it is the oracle this change is measured against. */
    private static boolean oracleExcludes(String relativePath) {
        if (relativePath.endsWith("/")) {
            return true;
        }
        int start = 0;
        for (int i = 0; i <= relativePath.length(); i++) {
            if (i == relativePath.length() || relativePath.charAt(i) == '/') {
                if (i > start) {
                    char first = relativePath.charAt(start);
                    if (first == '.') {
                        return true;
                    }
                    if (first == '_') {
                        int eqIdx = relativePath.indexOf('=', start);
                        boolean isPartitionSegment = eqIdx >= start && eqIdx < i;
                        if (isPartitionSegment == false) {
                            return true;
                        }
                    }
                }
                start = i + 1;
            }
        }
        return false;
    }

    /** The shipped composition, exactly as {@code doExpandGlob} composes it. */
    private static boolean newModelExcludes(String relativePath) {
        return relativePath.endsWith("/") || DEFAULT_MATCHERS.keeps(relativePath) == false;
    }

    private void assertSame(String relativePath, boolean expectedExcluded) {
        assertEquals("oracle disagrees with itself on [" + relativePath + "]", expectedExcluded, oracleExcludes(relativePath));
        assertEquals(
            "default config diverges from the frozen convention on [" + relativePath + "]",
            oracleExcludes(relativePath),
            newModelExcludes(relativePath)
        );
    }

    // -- directory placeholders (paths ending in '/') --

    public void testTrailingSlashIsExcluded() {
        assertSame("subdir/", true);
        assertSame("_delta_log/", true);
        assertSame("/", true);
        assertSame("normaldir/", true);
    }

    // -- underscore-prefixed names (no '=' -> not a Hive partition segment) --

    public void testUnderscorePrefixedNameIsExcluded() {
        assertSame("_SUCCESS", true);
        assertSame("_metadata", true);
        assertSame("_", true);
    }

    public void testUnderscorePrefixedDirectoryIsExcluded() {
        assertSame("_delta_log/00000000000000000001.json", true);
        assertSame("_temporary/part-00001.parquet", true);
        assertSame("year=2024/_hidden/file.parquet", true);
    }

    // -- underscore-prefixed Hive partition directories (contain '=' -> kept) --

    public void testUnderscorePrefixedHivePartitionIsKept() {
        assertSame("_index=alpha/part1.csv", false);
        assertSame("_index=beta/part1.parquet", false);
        assertSame("year=2024/_index=alpha/data.parquet", false);
    }

    // -- dot-prefixed names --

    public void testDotPrefixedNameIsExcluded() {
        assertSame(".part-r-00001.parquet.crc", true);
        assertSame(".", true);
        assertSame(".hidden/file.parquet", true);
        assertSame("year=2024/.git/config", true);
    }

    // -- ordinary data paths --

    public void testDataPathIsKept() {
        assertSame("file.parquet", false);
        assertSame("year=2024/data.parquet", false);
        assertSame("a/b/c/d.parquet", false);
        assertSame("part-r-00001.parquet", false);
    }

    // -- edge cases --

    public void testEmptyAndDegenerateSegments() {
        assertSame("", false);
        assertSame("a/_/b", true);
        assertSame("a/./b", true);
        assertSame("a//b", false);
    }

    /**
     * The adversarial band around the '=' carve-out, which a glob list cannot express directly and which the
     * default's second list reproduces: the '=' must be inside the '_'-prefixed segment itself.
     */
    public void testCarveOutBoundaries() {
        assertSame("_key=", false);
        assertSame("_=x", false);
        assertSame("_abc/x=y", true);
        assertSame("x=y/_abc", true);
        assertSame(".key=value", true);
        assertSame("__HIVE_DEFAULT_PARTITION__/f.parquet", true);
    }

    /**
     * Randomized paths over the characters the convention actually turns on. A divergence anywhere in this space is
     * a dataset somewhere whose file set silently changed.
     */
    public void testRandomizedPathsAgreeWithTheFrozenConvention() {
        for (int iteration = 0; iteration < 2000; iteration++) {
            String path = randomRelativePath();
            assertEquals(
                "default config diverges from the frozen convention on [" + path + "]",
                oracleExcludes(path),
                newModelExcludes(path)
            );
        }
    }

    private String randomRelativePath() {
        int segments = randomIntBetween(1, 5);
        List<String> parts = new ArrayList<>(segments);
        for (int i = 0; i < segments; i++) {
            parts.add(randomSegment());
        }
        String path = String.join("/", parts);
        return randomInt(9) == 0 ? path + "/" : path;
    }

    private String randomSegment() {
        int length = randomIntBetween(0, 6);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(randomFrom('_', '.', '=', '_', '.', '=', 'a', 'b', 'z', '0', '9', '-'));
        }
        return sb.toString();
    }
}
