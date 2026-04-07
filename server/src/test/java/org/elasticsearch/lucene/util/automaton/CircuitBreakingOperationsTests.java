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
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class CircuitBreakingOperationsTests extends ESTestCase {

    public void testDeterminizeWithNullBreakerThrowsAssertionError() {
        Automaton a = buildSimpleWildcardNFA("*test*");
        expectThrows(
            AssertionError.class,
            () -> CircuitBreakingOperations.determinize(a, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, "test")
        );
    }

    public void testDeterminizeWithLimitedBreakerProducesCorrectResult() {
        int num = atLeast(50);
        for (int i = 0; i < num; i++) {
            Automaton a = AutomatonTestUtil.randomAutomaton(random());
            Automaton expected = Operations.determinize(a, Integer.MAX_VALUE);
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
            Automaton actual = CircuitBreakingOperations.determinize(a, Integer.MAX_VALUE, breaker, "test");
            assertAcceptSameStrings(expected, actual);
        }
    }

    public void testDeterminizeWithDeterministicInputReturnsSameInstance() {
        Automaton a = Automata.makeString("hello");
        assertTrue(a.isDeterministic());
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        Automaton result = CircuitBreakingOperations.determinize(a, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, breaker, "test");
        assertSame(a, result);
    }

    public void testDeterminizeWithSingleStateAutomatonReturnsSameInstance() {
        Automaton a = new Automaton();
        a.createState();
        a.finishState();
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        Automaton result = CircuitBreakingOperations.determinize(a, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, breaker, "test");
        assertSame(a, result);
    }

    public void testDeterminizeExceedingWorkLimitThrowsTooComplex() {
        Automaton nfa = buildPathologicalNFA(20);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofGb(1));
        expectThrows(TooComplexToDeterminizeException.class, () -> CircuitBreakingOperations.determinize(nfa, 10, breaker, "test"));
    }

    public void testCircuitBreakerTripsOnLargeAutomaton() {
        Automaton nfa = buildPathologicalNFA(10);
        CircuitBreaker tripBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(5_000));
        expectThrows(
            CircuitBreakingException.class,
            () -> CircuitBreakingOperations.determinize(nfa, Integer.MAX_VALUE, tripBreaker, "test-label")
        );
    }

    public void testBreakerMemoryReleasedOnSuccess() {
        Automaton nfa = buildSimpleWildcardNFA("*test*");
        CircuitBreaker trackingBreaker = newLimitedBreaker(ByteSizeValue.ofMb(1));

        Automaton result = CircuitBreakingOperations.determinize(nfa, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, trackingBreaker, "test");
        assertTrue(result.isDeterministic());
        assertEquals("All temporarily reserved memory should be released after successful determinize", 0, trackingBreaker.getUsed());
    }

    public void testBreakerMemoryReleasedOnException() {
        Automaton nfa = buildPathologicalNFA(20);
        CircuitBreaker tripBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(12_800));

        expectThrows(
            CircuitBreakingException.class,
            () -> CircuitBreakingOperations.determinize(nfa, Integer.MAX_VALUE, tripBreaker, "test-label")
        );
        assertEquals("All reserved memory should be released after circuit breaker exception", 0, tripBreaker.getUsed());
    }

    /**
     * Builds a pathological NFA that causes exponential state blowup during determinization:
     * .*a.*b.*c.*d... with {@code depth} interleaved wildcards and literals.
     */
    private static Automaton buildPathologicalNFA(int depth) {
        List<Automaton> automata = new ArrayList<>();
        for (int i = 0; i < depth; i++) {
            automata.add(Automata.makeAnyString());
            automata.add(Automata.makeChar('a' + (i % 26)));
        }
        automata.add(Automata.makeAnyString());
        return Operations.concatenate(automata);
    }

    private static Automaton buildSimpleWildcardNFA(String pattern) {
        List<Automaton> automata = new ArrayList<>();
        for (int i = 0; i < pattern.length();) {
            int c = pattern.codePointAt(i);
            if (c == '*') {
                automata.add(Automata.makeAnyString());
            } else if (c == '?') {
                automata.add(Automata.makeAnyChar());
            } else {
                automata.add(Automata.makeChar(c));
            }
            i += Character.charCount(c);
        }
        return Operations.concatenate(automata);
    }

    private static void assertAcceptSameStrings(Automaton expected, Automaton actual) {
        assertTrue(
            "Circuit-breaking determinize should produce the same language as Lucene's determinize",
            Operations.subsetOf(expected, actual) && Operations.subsetOf(actual, expected)
        );
    }
}
