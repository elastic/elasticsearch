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

    private String exactMatchRLike(String pattern) {
        return rlike(pattern).exactMatch();
    }

    private boolean rlikeExactMatch(String pattern) {
        return pattern.equals(exactMatchRLike(pattern));
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

    public void testRegexExactMatchWithEmptyMatch() {
        // As soon as there's one no conditional `#` in the pattern, it'll match nothing
        assertNull(exactMatchRLike("#"));
        assertNull(exactMatchRLike("##"));
        assertNull(exactMatchRLike("#foo"));
        assertNull(exactMatchRLike("#foo#"));
        assertNull(exactMatchRLike("f#oo"));
        assertNull(exactMatchRLike("foo#"));
        assertNull(exactMatchRLike("#[A-Z]*"));
        assertNull(exactMatchRLike("foo(#)"));

        assertNotNull(exactMatchRLike("foo#?"));
        assertNotNull(exactMatchRLike("#|foo"));
        assertNotNull(exactMatchRLike("foo|#"));
    }

    public void testWildcardExtractPrefix() {
        assertEquals("foo", like("foo*").extractPrefix());
        assertNull(like("*bar").extractPrefix());
        assertEquals("foo", like("foo*bar").extractPrefix());
        assertEquals("foo*bar", like("foo\\*bar*").extractPrefix());
        assertNull(like("*").extractPrefix());
        assertEquals("exact", like("exact").extractPrefix());
        assertEquals("foo", like("foo?bar*").extractPrefix());
        assertNull(like("").extractPrefix());
        assertEquals("*", like("\\*").extractPrefix());
    }

    public void testWildcardExtractSuffix() {
        assertNull(like("foo*").extractSuffix());
        assertEquals("bar", like("*bar").extractSuffix());
        assertEquals("bar", like("foo*bar").extractSuffix());
        assertNull(like("foo\\*bar*").extractSuffix());
        assertNull(like("*").extractSuffix());
        assertEquals("exact", like("exact").extractSuffix());
        assertNull(like("foo?bar*").extractSuffix());
        assertNull(like("").extractSuffix());
        assertEquals("*", like("\\*").extractSuffix());
    }

    public void testShapePrefix() {
        assertEquals(new WildcardPattern.Shape.Prefix("foo"), like("foo*").shape());
        assertEquals(new WildcardPattern.Shape.Prefix("foo*"), like("foo\\**").shape());
        assertEquals(new WildcardPattern.Shape.Prefix("foo?"), like("foo\\?*").shape());
        assertEquals(new WildcardPattern.Shape.Prefix("café"), like("café*").shape());
        assertEquals(new WildcardPattern.Shape.Prefix("\\"), like("\\\\*").shape());
        // The bare "*" case is covered in testShapeAmbiguous (it parses as Suffix(""),
        // not Prefix("") — both are semantically equivalent on the matching side).
    }

    public void testShapeSuffix() {
        assertEquals(new WildcardPattern.Shape.Suffix("bar"), like("*bar").shape());
        assertEquals(new WildcardPattern.Shape.Suffix("*bar"), like("*\\*bar").shape());
        assertEquals(new WildcardPattern.Shape.Suffix("?bar"), like("*\\?bar").shape());
        assertEquals(new WildcardPattern.Shape.Suffix("café"), like("*café").shape());
        assertEquals(new WildcardPattern.Shape.Suffix("\\"), like("*\\\\").shape());
    }

    public void testShapeContains() {
        assertEquals(new WildcardPattern.Shape.Contains("foo"), like("*foo*").shape());
        assertEquals(new WildcardPattern.Shape.Contains(""), like("**").shape());
        assertEquals(new WildcardPattern.Shape.Contains("*"), like("*\\**").shape());
        assertEquals(new WildcardPattern.Shape.Contains("?"), like("*\\?*").shape());
        assertEquals(new WildcardPattern.Shape.Contains("\\"), like("*\\\\*").shape());
        assertEquals(new WildcardPattern.Shape.Contains("café"), like("*café*").shape());
    }

    public void testShapeAffix() {
        assertEquals(new WildcardPattern.Shape.Affix("foo", "", "bar"), like("foo*bar").shape());
        assertEquals(new WildcardPattern.Shape.Affix("foo", "x", ""), like("foo*x*").shape());
        assertEquals(new WildcardPattern.Shape.Affix("", "x", "bar"), like("*x*bar").shape());
        assertEquals(new WildcardPattern.Shape.Affix("foo", "x", "bar"), like("foo*x*bar").shape());
        // Adjacent unescaped stars collapse: foo**bar is prefix*suffix.
        assertEquals(new WildcardPattern.Shape.Affix("foo", "", "bar"), like("foo**bar").shape());
        assertEquals(new WildcardPattern.Shape.Prefix("foo"), like("foo**").shape());
        // Escaped \* is a literal star, not a raw star-group.
        assertEquals(new WildcardPattern.Shape.Affix("foo", "", "bar*baz"), like("foo*bar\\*baz").shape());
        assertEquals(new WildcardPattern.Shape.Prefix("foo*"), like("foo\\**").shape());
        assertEquals(new WildcardPattern.Shape.Contains("foo*bar"), like("*foo\\*bar*").shape());
    }

    public void testShapeGeneral() {
        // Empty pattern, exact strings, and patterns that don't fit Prefix/Suffix/Contains/Affix
        // all fall through to General.
        assertSame(WildcardPattern.Shape.General.INSTANCE, like("").shape());
        assertSame(WildcardPattern.Shape.General.INSTANCE, like("foo").shape());
        assertSame(WildcardPattern.Shape.General.INSTANCE, like("foo\\*").shape());      // escaped-only, exact-match
        assertSame(WildcardPattern.Shape.General.INSTANCE, like("foo?bar").shape());     // unescaped '?'
        assertSame(WildcardPattern.Shape.General.INSTANCE, like("*foo*bar*").shape());   // three star-groups
        assertSame(WildcardPattern.Shape.General.INSTANCE, like("*foo?bar*").shape());   // unescaped '?' in body
        // Note: dangling-backslash patterns ("foo\\", "*foo\\", "\\") are rejected by the
        // WildcardPattern constructor itself (via StringUtils.wildcardToJavaPattern), so they
        // never reach shape(). The dangling-escape branch in shape() exists as a defensive
        // fall-through for callers that bypass the constructor's validation.
    }

    public void testShapeAmbiguous() {
        // Single bare "*" is ambiguous between Prefix(""), Suffix(""), and Contains("") —
        // all three are semantically "matches everything". The parser is allowed to pick
        // any of them; pin the actual choice so a future refactor is intentional.
        assertEquals(new WildcardPattern.Shape.Suffix(""), like("*").shape());
    }

    /**
     * Comprehensive cross-product over the shape parser's input space: emptiness, leading/trailing
     * stars in every combination, every body content class (empty / plain ASCII / unicode /
     * escaped specials / unescaped {@code ?} / unescaped {@code *} / dangling backslash).
     * The expected {@link WildcardPattern.Shape} is computed from a separate reference
     * implementation walking the same definition, then compared against the production parser.
     */
    public void testShapeCombinatoric() {
        // Bodies are inputs that the WildcardPattern constructor will accept (no dangling
        // backslashes, no escapes-of-non-special chars — those throw at construction time and
        // never reach shape()).
        String[] bodies = new String[] {
            "",                          // empty body
            "a",                         // single ASCII char
            "abc",                       // multi-char ASCII
            "café",                      // unicode (multibyte UTF-8)
            "北京",                       // unicode (3-byte UTF-8)
            "a\\*b",                     // escaped '*' in body — unescapes to "a*b"
            "a\\?b",                     // escaped '?' in body — unescapes to "a?b"
            "a\\\\b",                    // escaped '\\' in body — unescapes to "a\\b"
            "a?b",                       // unescaped '?' — forces General
            "a*b",                       // unescaped '*' in middle — Affix, or General when wrapped with more stars
        };
        boolean[] flags = new boolean[] { false, true };
        for (String body : bodies) {
            for (boolean leading : flags) {
                for (boolean trailing : flags) {
                    String pattern = (leading ? "*" : "") + body + (trailing ? "*" : "");
                    WildcardPattern.Shape actual = like(pattern).shape();
                    WildcardPattern.Shape expected = referenceShape(pattern);
                    assertEquals("pattern=[" + pattern + "]", expected, actual);
                }
            }
        }
    }

    /**
     * Independent reference implementation of {@link WildcardPattern#shape()} used purely
     * as a cross-check oracle for {@link #testShapeCombinatoric}. Splits on maximal runs
     * of unescaped {@code *} (adjacent stars collapse) and classifies by segment count.
     * Kept separate from the production parser so a single bug cannot break both at once.
     */
    private static WildcardPattern.Shape referenceShape(String pattern) {
        int n = pattern.length();
        if (n == 0) {
            return WildcardPattern.Shape.General.INSTANCE;
        }
        String[] segs = new String[4];
        int segCount = 0;
        StringBuilder cur = new StringBuilder();
        int starChars = 0;
        boolean inStarRun = false;
        int i = 0;
        while (i < n) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                if (i + 1 >= n) {
                    return WildcardPattern.Shape.General.INSTANCE;
                }
                cur.append(pattern.charAt(i + 1));
                inStarRun = false;
                i += 2;
            } else if (c == '?') {
                return WildcardPattern.Shape.General.INSTANCE;
            } else if (c == '*') {
                starChars++;
                if (inStarRun == false) {
                    if (segCount == 4) {
                        return WildcardPattern.Shape.General.INSTANCE;
                    }
                    segs[segCount++] = cur.toString();
                    cur.setLength(0);
                    inStarRun = true;
                }
                i++;
            } else {
                cur.append(c);
                inStarRun = false;
                i++;
            }
        }
        if (segCount == 4) {
            return WildcardPattern.Shape.General.INSTANCE;
        }
        segs[segCount++] = cur.toString();
        int groups = segCount - 1;
        if (groups == 0) {
            return WildcardPattern.Shape.General.INSTANCE;
        }
        if (groups == 1) {
            String p = segs[0];
            String s = segs[1];
            if (p.isEmpty()) {
                if (s.isEmpty() && starChars >= 2) {
                    return new WildcardPattern.Shape.Contains("");
                }
                return new WildcardPattern.Shape.Suffix(s);
            }
            if (s.isEmpty()) {
                return new WildcardPattern.Shape.Prefix(p);
            }
            return new WildcardPattern.Shape.Affix(p, "", s);
        }
        if (groups == 2) {
            String p = segs[0];
            String m = segs[1];
            String s = segs[2];
            if (p.isEmpty() && s.isEmpty()) {
                return new WildcardPattern.Shape.Contains(m);
            }
            return new WildcardPattern.Shape.Affix(p, m, s);
        }
        return WildcardPattern.Shape.General.INSTANCE;
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
