/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.util.automaton;

import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingRegExp.Cost;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingRegExp.Shape;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CircuitBreakingRegExpTests extends ESTestCase {

    private static final int FLAGS = RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT;

    /** One pattern per kind of parse-tree node, and the combinations whose sizes the costs model. */
    public void testSameLanguageAsLuceneForEveryKind() {
        for (String pattern : List.of(
            "a|bc|d",
            "abc",
            "ab(c|d)e",
            "(a|b)&(b|c)",
            "a?",
            "(ab)*",
            "a{3,}",
            "(ab){2,4}",
            "a{0,3}",
            "a{1,3}",
            "a{3}",
            "~(ab)",
            "[^a-c]",
            "x",
            "[a-f]",
            "[a-cx-z]",
            ".",
            "#",
            "\"quoted\"",
            "@",
            "<1-100>",
            "(a?){20}",
            "((a|b){2,3}){0,4}",
            "a+++",
            "#{3}",
            "#{0,3}",
            "#*",
            "(a?|b){2,5}c",
            "a*b?c+",
            "(x?){5}{3}",
            "(.*a.*)&(.*b.*)",
            "(ab|a){1,}",
            ""
        )) {
            assertSameLanguageAsLucene(pattern, FLAGS, 0);
        }
    }

    public void testSameLanguageAsLuceneWhenCaseInsensitive() {
        for (String pattern : List.of("AbC", "a[b-d]E", "(Ab|cD){2}", "[^A]x")) {
            assertSameLanguageAsLucene(pattern, FLAGS, RegExp.CASE_INSENSITIVE);
        }
    }

    public void testSameLanguageAsLuceneOnRandomRegexps() {
        int checked = 0;
        for (int i = 0; i < 500; i++) {
            String pattern = AutomatonTestUtil.randomRegexp(random());
            int matchFlags = randomBoolean() ? 0 : RegExp.CASE_INSENSITIVE;
            RegExp lucene;
            try {
                lucene = new RegExp(pattern, FLAGS, matchFlags);
            } catch (IllegalArgumentException e) {
                continue;
            }
            Automaton expected;
            try {
                expected = Operations.determinize(lucene.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            } catch (TooComplexToDeterminizeException | IllegalArgumentException e) {
                continue;
            }
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
            Automaton actual;
            try {
                actual = new CircuitBreakingRegExp(pattern, FLAGS, matchFlags).toAutomaton(breaker, "test");
            } catch (TooComplexToDeterminizeException e) {
                continue;
            }
            assertEquals("everything charged while building [" + pattern + "] is released", 0L, breaker.getUsed());
            assertTrue(
                "same language as Lucene for [" + pattern + "]",
                AutomatonTestUtil.sameLanguage(expected, Operations.determinize(actual, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT))
            );
            checked++;
        }
        assertThat(checked, greaterThanOrEqualTo(100));
    }

    /** Every cost bounds the states and transitions of what the Lucene operation actually builds. */
    public void testCostsBoundTheBuiltAutomaton() {
        for (int i = 0; i < 300; i++) {
            Automaton a = AutomatonTestUtil.randomAutomaton(random());
            Automaton b = AutomatonTestUtil.randomAutomaton(random());
            Shape shape = Shape.of(a);
            int min = between(0, 4);
            int max = min + between(0, 4);
            assertBounds(CircuitBreakingRegExp.concatenateCost(new Automaton[] { a, b }), Operations.concatenate(List.of(a, b)), a);
            assertBounds(CircuitBreakingRegExp.unionCost(new Automaton[] { a, b }), Operations.union(List.of(a, b)), a);
            assertBounds(CircuitBreakingRegExp.optionalCost(shape), Operations.optional(a), a);
            assertBounds(CircuitBreakingRegExp.starCost(shape), Operations.repeat(a), a);
            assertBounds(CircuitBreakingRegExp.repeatCost(shape, min), Operations.repeat(a, min), a);
            assertBounds(CircuitBreakingRegExp.repeatCost(shape, min, max), Operations.repeat(a, min, max), a);
        }
    }

    /**
     * Pieces that accept the empty string make a concatenation's transitions grow with the square of its length, which the
     * state count does not show. Sized so that the build fits in the test heap even if it were not charged.
     */
    public void testNullableRepeatsAreCharged() {
        assertTripsSmallAndReleases("x?{1000}{2}", ByteSizeValue.ofMb(4));
    }

    /** An intersection with a complement written inside one pattern is a product, charged as it grows. */
    public void testIntersectionWithComplementIsCharged() {
        assertTripsSmallAndReleases("[ab]{200}&~((a|b)*b(a|b){10})", ByteSizeValue.ofMb(2));
    }

    /** Repeating an empty language builds nothing, but Lucene still allocates per copy. */
    public void testRepeatOfEmptyLanguageIsCharged() {
        for (String pattern : List.of("#{200000000}", "#{200000000,}", "#{0,2000000000}")) {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
            expectThrows(CircuitBreakingException.class, () -> new CircuitBreakingRegExp(pattern, FLAGS, 0).toAutomaton(breaker, "test"));
            assertEquals(0L, breaker.getUsed());
        }
    }

    /** Each stacked quantifier roughly doubles what the one inside it built; the doubling is charged step by step. */
    public void testStackedQuantifiersAreCharged() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(16));
        expectThrows(
            CircuitBreakingException.class,
            () -> new CircuitBreakingRegExp("a" + "+".repeat(40), FLAGS, 0).toAutomaton(breaker, "test")
        );
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * Optional copies are linked by scanning everything built so far, so their work grows with the square of the count while
     * the output stays small. The work limit refuses them before Lucene starts.
     */
    public void testConstructionWorkIsBounded() {
        for (String pattern : List.of("x{0,1000000}", "((a?){50}){0,500}")) {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
            expectThrows(
                TooComplexToDeterminizeException.class,
                () -> new CircuitBreakingRegExp(pattern, FLAGS, 0).toAutomaton(breaker, "test")
            );
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testOrdinaryBoundedRepeatsFitTheWorkLimit() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        for (String pattern : List.of("[a-z]{1,64}", ".{0,2000}", "(foo|bar){0,50}", "[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
            new CircuitBreakingRegExp(pattern, FLAGS, 0).toAutomaton(breaker, "test");
            assertEquals(0L, breaker.getUsed());
        }
    }

    /** The walk is iterative, so a long run of character classes, which Lucene parses iteratively, builds on a small stack. */
    public void testLongConcatenationBuildsOnASmallStack() throws InterruptedException {
        String pattern = "[^a]".repeat(50_000);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicReference<Automaton> built = new AtomicReference<>();
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Thread thread = new Thread(null, () -> {
            try {
                built.set(new CircuitBreakingRegExp(pattern, FLAGS, 0).toAutomaton(breaker, "test"));
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "small-stack-regex", 256 * 1024);
        thread.setDaemon(true);
        thread.start();
        thread.join(TimeValue.timeValueSeconds(30).millis());
        assertFalse("regex compilation did not finish", thread.isAlive());
        assertNull(thrown.get());
        assertEquals(0L, breaker.getUsed());
        assertTrue(Operations.run(Operations.determinize(built.get(), Integer.MAX_VALUE), "b".repeat(50_000)));
    }

    public void testUnknownNamedAutomatonIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new CircuitBreakingRegExp("<foo>", FLAGS, 0).toAutomaton(newLimitedBreaker(ByteSizeValue.ofMb(1)), "test")
        );
        assertEquals("'foo' not found", e.getMessage());
    }

    private static void assertSameLanguageAsLucene(String pattern, int syntaxFlags, int matchFlags) {
        Automaton expected = Operations.determinize(
            new RegExp(pattern, syntaxFlags, matchFlags).toAutomaton(),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Automaton actual = new CircuitBreakingRegExp(pattern, syntaxFlags, matchFlags).toAutomaton(breaker, "test");
        assertEquals("everything charged while building [" + pattern + "] is released", 0L, breaker.getUsed());
        assertTrue(
            "same language as Lucene for [" + pattern + "]",
            AutomatonTestUtil.sameLanguage(expected, Operations.determinize(actual, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT))
        );
    }

    private static void assertTripsSmallAndReleases(String pattern, ByteSizeValue small) {
        CircuitBreaker tight = newLimitedBreaker(small);
        expectThrows(CircuitBreakingException.class, () -> new CircuitBreakingRegExp(pattern, FLAGS, 0).toAutomaton(tight, "test"));
        assertEquals("everything reserved is released on failure", 0L, tight.getUsed());
        CircuitBreaker roomy = newLimitedBreaker(ByteSizeValue.ofGb(1));
        Automaton built = new CircuitBreakingRegExp(pattern, FLAGS, 0).toAutomaton(roomy, "test");
        assertTrue(built.getNumStates() > 0);
        assertEquals("everything reserved is released after the build", 0L, roomy.getUsed());
    }

    /** An operation that returns its operand unchanged allocates nothing, so there is nothing to bound. */
    private static void assertBounds(Cost cost, Automaton built, Automaton operand) {
        if (built == operand) {
            return;
        }
        assertThat("states", cost.states(), greaterThanOrEqualTo((long) built.getNumStates()));
        assertThat("transitions", cost.transitions(), greaterThanOrEqualTo((long) built.getNumTransitions()));
    }
}
