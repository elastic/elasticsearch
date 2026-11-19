/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.concurrent.atomic.AtomicReference;

public class StringPatternTests extends ESTestCase {

    private LikePattern like(String pattern, char escape) {
        return new LikePattern(pattern, escape);
    }

    private RLikePattern rlike(String pattern) {
        return new RLikePattern(pattern);
    }

    private boolean matchesAll(String pattern, char escape) {
        return like(pattern, escape).matchesAll();
    }

    private boolean exactMatch(String pattern, char escape) {
        String escaped = pattern.replace(Character.toString(escape), StringUtils.EMPTY);
        return escaped.equals(like(pattern, escape).exactMatch());
    }

    private boolean matchesAll(String pattern) {
        return rlike(pattern).matchesAll();
    }

    private boolean exactMatch(String pattern) {
        return pattern.equals(rlike(pattern).exactMatch());
    }

    public void testWildcardMatchAll() throws Exception {
        assertTrue(matchesAll("%", '0'));
        assertTrue(matchesAll("%%", '0'));

        assertFalse(matchesAll("a%", '0'));
        assertFalse(matchesAll("%_", '0'));
        assertFalse(matchesAll("%_%_%", '0'));
        assertFalse(matchesAll("_%", '0'));
        assertFalse(matchesAll("0%", '0'));
    }

    public void testRegexMatchAll() throws Exception {
        assertTrue(matchesAll(".*"));
        assertTrue(matchesAll(".*.*"));
        assertTrue(matchesAll(".*.?"));
        assertTrue(matchesAll(".?.*"));
        assertTrue(matchesAll(".*.?.*"));

        assertFalse(matchesAll("..*"));
        assertFalse(matchesAll("ab."));
        assertFalse(matchesAll("..?"));
    }

    public void testWildcardExactMatch() throws Exception {
        assertTrue(exactMatch("0%", '0'));
        assertTrue(exactMatch("0_", '0'));
        assertTrue(exactMatch("123", '0'));
        assertTrue(exactMatch("1230_", '0'));
        assertTrue(exactMatch("1230_321", '0'));

        assertFalse(exactMatch("%", '0'));
        assertFalse(exactMatch("%%", '0'));
        assertFalse(exactMatch("a%", '0'));
        assertFalse(exactMatch("a_", '0'));
    }

    public void testRegexExactMatch() throws Exception {
        assertFalse(exactMatch(".*"));
        assertFalse(exactMatch(".*.*"));
        assertFalse(exactMatch(".*.?"));
        assertFalse(exactMatch(".?.*"));
        assertFalse(exactMatch(".*.?.*"));
        assertFalse(exactMatch("..*"));
        assertFalse(exactMatch("ab."));
        assertFalse(exactMatch("..?"));

        assertTrue(exactMatch("abc"));
        assertTrue(exactMatch("12345"));
    }

    /** The text scan must answer exactly what the automaton Lucene builds for the same wildcard would. */
    public void testWildcardScanAgreesWithLuceneAutomaton() {
        String[] pieces = { "a", "b", "*", "*", "?", "\\", "\u00e9", "\uD83D\uDE00" };
        for (int i = 0; i < 2000; i++) {
            StringBuilder b = new StringBuilder();
            for (int n = between(0, 8); n > 0; n--) {
                b.append(randomFrom(pieces));
            }
            String wildcard = b.toString();
            Automaton automaton = WildcardQuery.toAutomaton(new Term("f", wildcard), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            // Lucene builds the empty wildcard as an automaton with no states, which getSingleton cannot read
            IntsRef singleton = automaton.getNumStates() == 0 ? null : Operations.getSingleton(automaton);
            String expected = singleton == null ? null : UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length);
            assertEquals(wildcard, Operations.isTotal(automaton), LuceneWildcard.matchesAll(wildcard));
            assertEquals(wildcard, expected, LuceneWildcard.exactMatch(wildcard));
        }
    }

    /** A run of {@code %} is quadratic to build as an automaton; read as text its length does not matter. */
    public void testLongWildcardRun() {
        assertTrue(like("%".repeat(100_000), '0').matchesAll());
        assertNull(like("%".repeat(100_000) + "a", '0').exactMatch());
        assertTrue(new WildcardPattern("*".repeat(100_000)).matchesAll());
    }

    public void testRegexLongerThanBoundIsNotCompiled() {
        String atBound = "a".repeat(RLikePattern.MAX_COMPILED_LENGTH);
        assertEquals(atBound, rlike(atBound).exactMatch());
        assertNotSimplified(rlike(atBound + "a"));
        assertNotSimplified(rlike(".*" + ".?".repeat(RLikePattern.MAX_COMPILED_LENGTH)));
    }

    /**
     * Short patterns that are huge or slow to build: nested repeats of a finite atom, repeats of something that accepts the
     * empty string, and one too complex to determinize. Building any of the first three would exhaust the test JVM, so
     * returning at all shows they were refused before the build.
     */
    public void testRegexPastBudgetIsNotCompiled() {
        for (String regex : new String[] { "[ab]{1000}{1000}{1000}", "(.*){100}{100}", "<0-999999999>{1000}{50}", "(a|b)*a(a|b){30}" }) {
            assertNotSimplified(rlike(regex));
        }
    }

    /**
     * Lucene parses nested groups recursively, and on a coordinator thread the Error would take the node down. The length
     * bound keeps real patterns shallow, so it is lifted here, and the stack is small and fixed so the overflow is certain.
     */
    public void testDeeplyNestedRegexDoesNotOverflow() throws Exception {
        String regex = "(".repeat(5000) + "a" + ")".repeat(5000);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicReference<Automaton> built = new AtomicReference<>();
        Thread thread = new Thread(null, () -> {
            try {
                built.set(new RLikePattern(regex, Integer.MAX_VALUE).createAutomaton());
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "small-stack-regex", 256 * 1024);
        thread.start();
        thread.join(TimeValue.timeValueSeconds(30).millis());
        assertFalse("pattern compilation did not finish", thread.isAlive());
        assertNull(thrown.get());
        assertNull(built.get());
    }

    private static void assertNotSimplified(RLikePattern pattern) {
        assertFalse(pattern.matchesAll());
        assertNull(pattern.exactMatch());
    }
}
