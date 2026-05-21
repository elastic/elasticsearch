/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class WildcardLikeShapeTests extends ESTestCase {

    public void testPrefixOnly() {
        WildcardLikeShape s = WildcardLikeShape.of("foo*");
        assertNotNull(s);
        assertEquals(new BytesRef("foo"), s.prefix());
        assertNull(s.literal());
        assertNull(s.suffix());
        assertFalse(s.matchesAll());
    }

    public void testSuffixOnly() {
        WildcardLikeShape s = WildcardLikeShape.of("*foo");
        assertNotNull(s);
        assertNull(s.prefix());
        assertNull(s.literal());
        assertEquals(new BytesRef("foo"), s.suffix());
    }

    public void testPrefixAndSuffix() {
        WildcardLikeShape s = WildcardLikeShape.of("http*com");
        assertNotNull(s);
        assertEquals(new BytesRef("http"), s.prefix());
        assertNull(s.literal());
        assertEquals(new BytesRef("com"), s.suffix());
    }

    public void testContainsLiteralOnly() {
        WildcardLikeShape s = WildcardLikeShape.of("*google*");
        assertNotNull(s);
        assertNull(s.prefix());
        assertEquals(new BytesRef("google"), s.literal());
        assertNull(s.suffix());
    }

    public void testPrefixAndLiteral() {
        WildcardLikeShape s = WildcardLikeShape.of("http*google*");
        assertNotNull(s);
        assertEquals(new BytesRef("http"), s.prefix());
        assertEquals(new BytesRef("google"), s.literal());
        assertNull(s.suffix());
    }

    public void testLiteralAndSuffix() {
        WildcardLikeShape s = WildcardLikeShape.of("*google*com");
        assertNotNull(s);
        assertNull(s.prefix());
        assertEquals(new BytesRef("google"), s.literal());
        assertEquals(new BytesRef("com"), s.suffix());
    }

    public void testFullAffixContains() {
        WildcardLikeShape s = WildcardLikeShape.of("http*google*com");
        assertNotNull(s);
        assertEquals(new BytesRef("http"), s.prefix());
        assertEquals(new BytesRef("google"), s.literal());
        assertEquals(new BytesRef("com"), s.suffix());
    }

    public void testStarAloneIsMatchesAll() {
        WildcardLikeShape s = WildcardLikeShape.of("*");
        assertNotNull(s);
        assertTrue(s.matchesAll());
        assertNull(s.prefix());
        assertNull(s.literal());
        assertNull(s.suffix());
    }

    public void testAdjacentStarsCollapseInPrefix() {
        // "**" semantically matches anything — same as "*".
        WildcardLikeShape s = WildcardLikeShape.of("**");
        assertNotNull(s);
        assertTrue(s.matchesAll());
    }

    public void testAdjacentStarsCollapseInLiteral() {
        // "*foo**bar*" semantically equals "*foo*bar*", which has 3 unescaped non-adjacent stars
        // (4 segments) and is outside the affix-contains family.
        assertNull(WildcardLikeShape.of("*foo**bar*"));
    }

    public void testAdjacentStarsCollapseAtTail() {
        // "foo***" collapses to "foo*" — pure prefix.
        WildcardLikeShape s = WildcardLikeShape.of("foo***");
        assertNotNull(s);
        assertEquals(new BytesRef("foo"), s.prefix());
        assertNull(s.literal());
        assertNull(s.suffix());
    }

    public void testTooManyStarsRejected() {
        // 'prefix*literal1*literal2*literal3*suffix' shape would need ordered substring matching;
        // not in the affix-contains family.
        assertNull(WildcardLikeShape.of("a*b*c*d"));
    }

    public void testQuestionMarkRejected() {
        // '?' matches a single Unicode codepoint, awkward on UTF-8 byte boundaries.
        assertNull(WildcardLikeShape.of("foo?bar"));
        assertNull(WildcardLikeShape.of("*foo?bar*"));
        assertNull(WildcardLikeShape.of("foo*bar?"));
    }

    public void testEscapeRejected() {
        // Any backslash escape forces the automaton path so this parser stays simple and
        // verifiably correct.
        assertNull(WildcardLikeShape.of("foo\\*bar"));
        assertNull(WildcardLikeShape.of("foo\\?bar"));
        assertNull(WildcardLikeShape.of("foo\\\\bar"));
        assertNull(WildcardLikeShape.of("*foo\\*bar*"));
    }

    public void testNoWildcardRejected() {
        // Equivalent to Equals — already decomposed by the logical optimizer; out of scope here.
        assertNull(WildcardLikeShape.of("exact"));
    }

    public void testEmptyPatternRejected() {
        assertNull(WildcardLikeShape.of(""));
    }

    public void testUtf8MultiByteLiterals() {
        // The shape parser commits to UTF-8 bytes — what KEYWORD inputs are encoded as on disk.
        WildcardLikeShape s = WildcardLikeShape.of("*café*");
        assertNotNull(s);
        assertEquals(new BytesRef("café"), s.literal());
        // 'café' is 5 UTF-8 bytes (c=1, a=1, f=1, é=2).
        assertEquals(5, s.literal().length);
    }

    public void testSingleCharSegments() {
        WildcardLikeShape s = WildcardLikeShape.of("a*b*c");
        assertNotNull(s);
        assertEquals(new BytesRef("a"), s.prefix());
        assertEquals(new BytesRef("b"), s.literal());
        assertEquals(new BytesRef("c"), s.suffix());
    }

    /**
     * Property pin: for every accepted shape, {@link ByteMatchers#affixContains} must agree with
     * the {@link ByteRunAutomaton} compiled from the same Lucene wildcard pattern on every value
     * in a randomized corpus. Because the affix-contains dispatcher is the sole consumer of this
     * parser inside the Parquet evaluator, divergence here would silently change query results.
     *
     * <p>The corpus mixes values that satisfy zero, one, two, and three of the components, plus
     * values shorter than the combined affix length (to exercise the combined-length guard). The
     * patterns sweep every shape branch in {@link WildcardLikeShape#of}.
     */
    public void testAffixContainsAgreesWithByteRunAutomaton() {
        String[] patterns = {
            "https*",
            "*.com",
            "*google*",
            "https*com",
            "https*google*",
            "*google*com",
            "https*google*com",
            "a*",
            "*a",
            "*a*",
            "a*b",
            "a*b*",
            "*a*b",
            "a*b*c" };
        String[] values = {
            "",
            "a",
            "ab",
            "abc",
            "https://www.google.com",
            "https://www.google.org",
            "ftp://www.google.com",
            "https://com",
            "https://www.bing.com",
            "google",
            "googleSurprise.com",
            ".com",
            "https://www.café.com" };
        for (String pattern : patterns) {
            WildcardLikeShape shape = WildcardLikeShape.of(pattern);
            assertNotNull("pattern [" + pattern + "] should be in the affix-contains family", shape);
            ByteRunAutomaton runner = new ByteRunAutomaton(
                WildcardQuery.toAutomaton(new Term("f", pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
            );
            for (String v : values) {
                byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
                BytesRef ref = new BytesRef(bytes);
                boolean fromAutomaton = runner.run(bytes, 0, bytes.length);
                boolean fromShape = ByteMatchers.affixContains(ref, shape.prefix(), shape.literal(), shape.suffix());
                assertEquals("pattern [" + pattern + "] value [" + v + "]: shape and automaton diverge", fromAutomaton, fromShape);
            }
        }
    }
}
