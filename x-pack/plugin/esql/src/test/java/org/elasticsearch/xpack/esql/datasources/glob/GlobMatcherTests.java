/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

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

    // -- edge and failure cases. Driven by measured coverage: every one of these closed a line or branch that no
    // test reached, on a class that decides which objects every dataset reads.

    public void testNullGlobIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher(null));
        assertEquals("glob pattern cannot be null", e.getMessage());
    }

    public void testNullPathNeverMatches() {
        assertFalse(new GlobMatcher("*").matches(null));
    }

    public void testToStringNamesThePattern() {
        assertEquals("GlobMatcher[a/*.csv]", new GlobMatcher("a/*.csv").toString());
    }

    /**
     * A class is a single-character construct, so like {@code *} it must never span a separator. A negated class
     * used to: {@code x[!a]y} matched {@code x/y}, silently crossing a segment. A class holding a separator is
     * refused outright, since segments are split before the class is parsed.
     */
    public void testCharacterClassNeverMatchesTheSeparator() {
        assertFalse("a negated class must exclude the separator", new GlobMatcher("a[!x]b").matches("a/b"));
        assertFalse("a range spanning the separator's code point must still exclude it", new GlobMatcher("a[.-0]b").matches("a/b"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher("a[/]b"));
        assertTrue(e.getMessage(), e.getMessage().contains("cannot contain or span a path separator"));
    }

    public void testQuestionMarkNeverMatchesTheSeparator() {
        assertFalse(new GlobMatcher("a?b").matches("a/b"));
    }

    /** Wildcards inside a brace alternative, which reach the alternative-matching path rather than the plain one. */
    public void testWildcardsInsideBraceAlternatives() {
        GlobMatcher anyChar = new GlobMatcher("{a?,b}.csv");
        assertTrue(anyChar.matches("ax.csv"));
        assertTrue(anyChar.matches("b.csv"));
        assertFalse(anyChar.matches("a.csv"));
        assertFalse("an alternative's ? must not span a separator", new GlobMatcher("{a?,b}c").matches("a/c"));

        GlobMatcher charClass = new GlobMatcher("{a[0-9],b}.csv");
        assertTrue(charClass.matches("a5.csv"));
        assertTrue(charClass.matches("b.csv"));
        assertFalse(charClass.matches("ax.csv"));

        GlobMatcher star = new GlobMatcher("{a*,b}.csv");
        assertTrue(star.matches("a.csv"));
        assertTrue(star.matches("axyz.csv"));
        assertTrue(star.matches("b.csv"));
        assertFalse(star.matches(".csv"));
    }

    /** A trailing {@code -} is a literal, not the start of a range; a reversed range is refused. */
    public void testRangeEdges() {
        assertTrue(new GlobMatcher("a[x-]b").matches("a-b"));
        assertTrue(new GlobMatcher("a[x-]b").matches("axb"));
        assertTrue(new GlobMatcher("a[-x]b").matches("a-b"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher("a[z-a]b"));
        assertTrue(e.getMessage(), e.getMessage().contains("reversed range"));
    }

    /** A {@code ]} immediately after the opener is a literal member rather than the terminator. */
    public void testClosingBracketAsTheFirstClassMember() {
        assertTrue(new GlobMatcher("a[]]b").matches("a]b"));
        assertFalse(new GlobMatcher("a[]]b").matches("ab"));
    }

    public void testOverWideBraceExpansionIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher("f{1..99999}.csv"));
        assertTrue(e.getMessage(), e.getMessage().contains("more than"));
    }

    /** Degenerate inputs: the empty pattern, bare separators, and repeated separators. */
    public void testDegenerateInputs() {
        assertTrue(new GlobMatcher("").matches(""));
        assertFalse(new GlobMatcher("").matches("a"));
        assertTrue(new GlobMatcher("a//b").matches("a//b"));
        assertFalse("a doubled separator is two segments, one of them empty", new GlobMatcher("a/b").matches("a//b"));
        assertTrue(new GlobMatcher("*").matches(""));
        assertFalse(new GlobMatcher("?").matches(""));
    }

    /** A character above a range's upper bound must be refused, not only one below its lower bound. */
    public void testRangeUpperBoundIsEnforced() {
        GlobMatcher m = new GlobMatcher("a[b-d]e");
        assertTrue(m.matches("ace"));
        assertFalse("below the range", m.matches("aae"));
        assertFalse("above the range", m.matches("aze"));
    }

    /**
     * The memo is keyed on (pattern segment, path segment) and caches negative results as well as positive ones —
     * caching only the hits would leave the exponential re-exploration this rewrite exists to remove. A globstar
     * pattern that fails deep in a wide tree revisits the same pairs repeatedly, so it exercises the cached-false
     * path rather than only the cached-true one.
     */
    public void testNegativeResultsAreMemoisedToo() {
        GlobMatcher m = new GlobMatcher("**" + "/x/**" + "/y.csv");
        // Every globstar split lands on the same (pattern segment, path segment) pairs, all of which fail. Without
        // caching the misses this is the exponential re-exploration the rewrite exists to remove.
        assertFalse(m.matches("a/a/a/a/a/a/a/a/z.csv"));
        assertFalse(m.matches("x/x/x/x/x/x/x/x/z.csv"));
        assertTrue(m.matches("a/x/b/y.csv"));
    }

    /** The memo is keyed on (pattern segment, path segment); repeated probes must give the same answer. */
    public void testRepeatedMatchesAreStable() {
        GlobMatcher m = new GlobMatcher("a/**" + "/z.csv");
        for (int i = 0; i < 3; i++) {
            assertTrue(m.matches("a/b/c/z.csv"));
            assertFalse(m.matches("a/b/c/y.csv"));
        }
    }
}
