/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;

/**
 * Identifies an automaton that {@link SearchExecutionContext#computeAutomatonIfAbsent} builds once and every later
 * clause of the same request reuses.
 * <p>
 * One record per kind, so two kinds that happen to carry the same pattern can never collide: a prefix and a wildcard
 * over {@code foo} are different types, not two strings that have to be spelled apart. Patterns must be given as
 * resolved, after per-field normalization, since two fields whose normalizers disagree compile different automata.
 */
public sealed interface AutomatonKey {

    /** Circuit breaker category the automaton is charged under. */
    String category();

    record Wildcard(String pattern, boolean caseInsensitive) implements AutomatonKey {
        @Override
        public String category() {
            return ChildMemoryCircuitBreaker.CATEGORY_WILDCARD;
        }
    }

    record Regexp(String pattern, int syntaxFlags, int matchFlags, int determinizeWorkLimit) implements AutomatonKey {
        @Override
        public String category() {
            return ChildMemoryCircuitBreaker.CATEGORY_REGEXP;
        }
    }
}
