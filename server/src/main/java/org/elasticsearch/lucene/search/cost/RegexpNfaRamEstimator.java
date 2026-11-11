/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.automaton.RegExp;

import java.util.Objects;

/**
 * Estimates how much heap {@link RegExp#toAutomaton()} will use to build a regexp's NFA, without building it.
 */
public final class RegexpNfaRamEstimator {

    private RegexpNfaRamEstimator() {}

    static final long ESTIMATED_BYTES_PER_STATE = 32L;

    /** Estimates the NFA build cost, in bytes, for {@code pattern}. */
    public static long estimateRamBytes(String pattern, int syntaxFlags, int matchFlags) {
        return estimateRamBytes(new RegExp(pattern, syntaxFlags, matchFlags));
    }

    /** Estimates the NFA build cost, in bytes, for the already-parsed {@code re}, saturating to {@link Long#MAX_VALUE}. */
    public static long estimateRamBytes(RegExp re) {
        Objects.requireNonNull(re, "re must not be null");
        return multiplySaturating(estimateStates(re), ESTIMATED_BYTES_PER_STATE);
    }

    /** Upper bound on the number of NFA states, mirroring how {@link RegExp#toAutomaton()} composes them. */
    private static long estimateStates(RegExp re) {
        switch (re.kind) {
            case REGEXP_UNION:
                return addSaturating(addSaturating(estimateStates(re.exp1), estimateStates(re.exp2)), 1L);
            case REGEXP_CONCATENATION:
                return addSaturating(estimateStates(re.exp1), estimateStates(re.exp2));
            case REGEXP_INTERSECTION:
                return multiplySaturating(estimateStates(re.exp1), estimateStates(re.exp2));
            case REGEXP_OPTIONAL:
            case REGEXP_REPEAT:
                return addSaturating(estimateStates(re.exp1), 1L);
            case REGEXP_REPEAT_MIN:
                // a{n,}: n copies of the sub-expression plus a repeat.
                return multiplySaturating(estimateStates(re.exp1), addSaturating(re.min, 1L));
            case REGEXP_REPEAT_MINMAX:
                // a{n,m}: up to m copies of the sub-expression. This multiplication is the blow-up we guard against.
                return multiplySaturating(estimateStates(re.exp1), Math.max(1L, re.max));
            case REGEXP_COMPLEMENT:
            case REGEXP_DEPRECATED_COMPLEMENT:
                return estimateStates(re.exp1);
            case REGEXP_STRING:
                return re.s == null ? 2L : addSaturating(re.s.length(), 1L);
            case REGEXP_CHAR_CLASS:
                return re.from == null ? 2L : addSaturating(re.from.length, 1L);
            case REGEXP_CHAR:
            case REGEXP_CHAR_RANGE:
            case REGEXP_ANYCHAR:
            case REGEXP_EMPTY:
            case REGEXP_ANYSTRING:
            case REGEXP_INTERVAL:
            case REGEXP_AUTOMATON:
                return 2L;
        }
        throw new AssertionError("unexpected RegExp kind: " + re.kind);
    }

    private static long addSaturating(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long multiplySaturating(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
