/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

public class StringPatternTests extends ESTestCase {

    private WildcardPattern like(String pattern) {
        return new WildcardPattern(pattern);
    }

    private RLikePattern rlike(String pattern) {
        return new RLikePattern(pattern);
    }

    private boolean likeMatchesAll(String pattern) {
        return like(pattern).matchesAll();
    }

    private boolean likeExactMatch(String pattern) {
        String escaped = pattern.replace("\\", StringUtils.EMPTY);
        return escaped.equals(like(pattern).exactMatch());
    }

    private boolean rlikeMatchesAll(String pattern) {
        return rlike(pattern).matchesAll();
    }

    private boolean rlikeExactMatch(String pattern) {
        return pattern.equals(rlike(pattern).exactMatch());
    }

    public void testWildcardMatchAll() {
        assertTrue(likeMatchesAll("*"));
        assertTrue(likeMatchesAll("**"));

        assertFalse(likeMatchesAll("a*"));
        assertFalse(likeMatchesAll("*?"));
        assertFalse(likeMatchesAll("*?*?*"));
        assertFalse(likeMatchesAll("?*"));
        assertFalse(likeMatchesAll("\\*"));
    }

    public void testRegexMatchAll() {
        assertTrue(rlikeMatchesAll(".*"));
        assertTrue(rlikeMatchesAll(".*.*"));
        assertTrue(rlikeMatchesAll(".*.?"));
        assertTrue(rlikeMatchesAll(".?.*"));
        assertTrue(rlikeMatchesAll(".*.?.*"));

        assertFalse(rlikeMatchesAll("..*"));
        assertFalse(rlikeMatchesAll("ab."));
        assertFalse(rlikeMatchesAll("..?"));
    }

    public void testWildcardExactMatch() {
        assertTrue(likeExactMatch("\\*"));
        assertTrue(likeExactMatch("\\?"));
        assertTrue(likeExactMatch("123"));
        assertTrue(likeExactMatch("123\\?"));
        assertTrue(likeExactMatch("123\\?321"));

        assertFalse(likeExactMatch("*"));
        assertFalse(likeExactMatch("**"));
        assertFalse(likeExactMatch("a*"));
        assertFalse(likeExactMatch("a?"));
    }

    public void testRegexExactMatch() {
        assertFalse(rlikeExactMatch(".*"));
        assertFalse(rlikeExactMatch(".*.*"));
        assertFalse(rlikeExactMatch(".*.?"));
        assertFalse(rlikeExactMatch(".?.*"));
        assertFalse(rlikeExactMatch(".*.?.*"));
        assertFalse(rlikeExactMatch("..*"));
        assertFalse(rlikeExactMatch("ab."));
        assertFalse(rlikeExactMatch("..?"));

        assertTrue(rlikeExactMatch("abc"));
        assertTrue(rlikeExactMatch("12345"));
    }

    public void testTooComplexPattern() {
        var e = expectThrows(IllegalArgumentException.class, () -> rlike("(a|b)*a(a|b){13}").createAutomaton(false));
        assertEquals("Pattern was too complex to determinize", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> like("*a?????????????").createAutomaton(false));
        assertEquals("Pattern was too complex to determinize", e.getMessage());
    }

    public void testDeeplyNestedRLikePattern() {
        // A deeply nested regex pattern (thousands of nested parentheses) previously caused a
        // StackOverflowError in Lucene's RegExp.findLeaves(), surfacing as a 500 instead of a 400.
        String deepPattern = "(".repeat(5000) + "a" + ")".repeat(5000);
        var e = expectThrows(IllegalArgumentException.class, () -> rlike(deepPattern).createAutomaton(false));
        assertEquals("Pattern nesting is too deep to evaluate", e.getMessage());
    }
}
