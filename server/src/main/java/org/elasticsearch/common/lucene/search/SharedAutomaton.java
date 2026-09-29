/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.NFARunAutomaton;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.lucene.search.cost.AutomatonQueryCostEstimator;

/**
 * A determinized automaton paired with the compiled form Lucene runs against the term dictionary. The compiled
 * form is derived from the automaton alone, so clauses that differ only by field can share one instance
 * instead of compiling one each.
 * <p>
 * Only non-mutating forms belong here. {@link CompiledAutomaton#floor} keeps per-instance scratch state and is
 * used by fuzzy term enumeration, which therefore builds its own automata.
 */
public record SharedAutomaton(Automaton automaton, CompiledAutomaton compiled) implements Accountable {

    private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(SharedAutomaton.class);

    /**
     * Compiles {@code dfa}, holding an estimate of the construction peak on {@code breaker} for the duration of
     * the build so the otherwise unguarded UTF-8 expansion is visible. The estimate is released before returning;
     * the retained size is charged by whoever stores the result.
     *
     * @param dfa a determinized automaton. Lucene compiles a non-deterministic one into an {@code NFARunAutomaton},
     *            which determinizes lazily and caches on the instance, so it cannot be shared between clauses.
     */
    public static SharedAutomaton compile(Automaton dfa, CircuitBreaker breaker, String label) {
        if (dfa.isDeterministic() == false) {
            throw new IllegalArgumentException("shared automata require a determinized automaton, got an NFA for [" + label + "]");
        }
        long reservation = new AutomatonQueryCostEstimator(dfa.ramBytesUsed()).estimate();
        breaker.addEstimateBytesAndMaybeBreak(reservation, label);
        try {
            CompiledAutomaton compiled = new CompiledAutomaton(dfa, false, true, false);
            // Guaranteed by the check above, but Lucene picks the fallback on its own conditions: catch an upgrade
            // that changes them before the mutable runnable reaches a shared instance.
            assert compiled.getByteRunnable() instanceof NFARunAutomaton == false
                : "shared automata must not carry a mutable NFA: " + label;
            return new SharedAutomaton(dfa, compiled);
        } finally {
            breaker.addWithoutBreaking(-reservation, label);
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES + automaton.ramBytesUsed() + compiled.ramBytesUsed();
    }
}
