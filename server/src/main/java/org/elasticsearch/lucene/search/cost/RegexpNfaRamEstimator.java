/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import java.util.Objects;

/**
 * Estimates how much heap {@link RegExp#toAutomaton()} will use to build a regexp's NFA, without building it.
 * <p>
 * Lucene composes the NFA bottom-up, and the cost of a composition is in its transitions, not only its states:
 * concatenating a sub-automaton that accepts the empty string links every accept state of everything before it to the
 * initial transitions of everything after it, so {@code (.*){100}{100}} has 10,000 states and fifty million transitions.
 * Each node is therefore summarised by the counts the next composition needs, and the estimate is an upper bound on what
 * Lucene builds, sized so that the inputs and the result of the last composition fit at once.
 */
public final class RegexpNfaRamEstimator {

    private RegexpNfaRamEstimator() {}

    /** Two ints per state in the automaton, and the inputs of a composition are alive while its result is built. */
    static final long BYTES_PER_STATE = 32L;
    /** Three ints per transition, doubled for the same reason. */
    static final long BYTES_PER_TRANSITION = 24L;
    /** The automaton object and its empty arrays, before any state is added. */
    static final long BASE_BYTES = 256L;
    /** Determinization inside a complement stops after this much effort, so its DFA cannot have more states. */
    private static final long MAX_DETERMINIZED_STATES = 10L * Operations.DEFAULT_DETERMINIZE_WORK_LIMIT + 1;

    /**
     * What a later composition needs to know about a sub-automaton: how many states and transitions it has, how many
     * states accept, how many transitions leave its initial state, and whether it accepts the empty string.
     */
    record Shape(long states, long transitions, long accepts, long initial, boolean nullable) {
        static final Shape EMPTY_STRING = new Shape(1, 0, 1, 0, true);
        static final Shape ANY_STRING = new Shape(1, 1, 1, 1, true);
        static final Shape SINGLE = new Shape(2, 1, 1, 1, false);

        long bytes() {
            return add(BASE_BYTES, add(mul(states, BYTES_PER_STATE), mul(transitions, BYTES_PER_TRANSITION)));
        }

        /** {@code Operations.concatenate}: every accept state of {@code this} gains the initial transitions of {@code next}. */
        Shape then(Shape next) {
            return new Shape(
                add(states, next.states),
                add(add(transitions, next.transitions), mul(accepts, next.initial)),
                add(next.accepts, next.nullable ? accepts : 0),
                add(initial, nullable ? next.initial : 0),
                nullable && next.nullable
            );
        }

        /** {@code this} concatenated with itself {@code n} times, in closed form so a huge {@code n} costs nothing. */
        Shape times(long n) {
            if (n <= 0) {
                return EMPTY_STRING;
            }
            if (n == 1) {
                return this;
            }
            // accepts of copy k stay accept only while every later copy is nullable, so the links are quadratic then, linear otherwise
            long links = nullable ? mul(mul(accepts, initial), mul(n, n - 1) / 2) : mul(mul(accepts, initial), n - 1);
            return new Shape(
                mul(states, n),
                add(mul(transitions, n), links),
                nullable ? mul(accepts, n) : accepts,
                nullable ? mul(initial, n) : initial,
                nullable
            );
        }

        /** {@code Operations.repeat}: a new accepting initial state, and every accept state gains the initial transitions. */
        Shape star() {
            return new Shape(add(states, 1), add(add(transitions, initial), mul(accepts, initial)), add(accepts, 1), initial, true);
        }

        /** {@code Operations.optional}: at most one new accepting initial state carrying the initial transitions. */
        Shape optional() {
            return nullable ? this : new Shape(add(states, 1), add(transitions, initial), add(accepts, 1), initial, true);
        }

        /** {@code Operations.union}: a new initial state with epsilon transitions to each member. */
        Shape or(Shape other) {
            return new Shape(
                add(add(states, other.states), 1),
                add(add(transitions, other.transitions), add(initial, other.initial)),
                add(add(accepts, other.accepts), 1),
                add(initial, other.initial),
                nullable || other.nullable
            );
        }

        /** {@code Operations.intersection}: at most a state per pair and a transition per overlapping pair. */
        Shape and(Shape other) {
            return new Shape(
                mul(states, other.states),
                mul(transitions, other.transitions),
                mul(accepts, other.accepts),
                mul(initial, other.initial),
                nullable && other.nullable
            );
        }

        /** {@code Operations.complement}: determinized under Lucene's own work limit, then totalized. */
        Shape complement() {
            long dfaStates = states >= 62 ? MAX_DETERMINIZED_STATES : Math.min(1L << states, MAX_DETERMINIZED_STATES);
            long perState = add(mul(transitions, 2), 2);
            return new Shape(dfaStates, mul(dfaStates, perState), dfaStates, perState, true);
        }
    }

    /** Estimates the NFA build cost, in bytes, for {@code pattern}. */
    public static long estimateRamBytes(String pattern, int syntaxFlags, int matchFlags) {
        Shape shape = shape(new RegExp(pattern, syntaxFlags, matchFlags));
        if ((matchFlags & (RegExp.CASE_INSENSITIVE | RegExp.ASCII_CASE_INSENSITIVE)) != 0) {
            // case folding adds up to three alternates per character
            shape = new Shape(shape.states, mul(shape.transitions, 4), shape.accepts, mul(shape.initial, 4), shape.nullable);
        }
        return shape.bytes();
    }

    /** Estimates the NFA build cost, in bytes, for the already-parsed {@code re}, saturating to {@link Long#MAX_VALUE}. */
    public static long estimateRamBytes(RegExp re) {
        Objects.requireNonNull(re, "re must not be null");
        return shape(re).bytes();
    }

    /**
     * Estimates the NFA build cost, in bytes, of a Lucene wildcard pattern as {@code WildcardQuery.toAutomaton} builds it:
     * one automaton per character, {@code *} accepting anything, concatenated.
     */
    public static long estimateWildcardRamBytes(String wildcard) {
        Shape shape = Shape.EMPTY_STRING;
        for (int i = 0; i < wildcard.length();) {
            int c = wildcard.codePointAt(i);
            i += Character.charCount(c);
            if (c == '\\' && i < wildcard.length()) {
                i += Character.charCount(wildcard.codePointAt(i));
            }
            shape = shape.then(c == '*' ? Shape.ANY_STRING : Shape.SINGLE);
        }
        return shape.bytes();
    }

    /** Mirrors how {@link RegExp#toAutomaton()} composes each node. */
    static Shape shape(RegExp re) {
        switch (re.kind) {
            case REGEXP_UNION:
                return shape(re.exp1).or(shape(re.exp2));
            case REGEXP_CONCATENATION:
                return shape(re.exp1).then(shape(re.exp2));
            case REGEXP_INTERSECTION:
                return shape(re.exp1).and(shape(re.exp2));
            case REGEXP_OPTIONAL:
                return shape(re.exp1).optional();
            case REGEXP_REPEAT:
                return shape(re.exp1).star();
            case REGEXP_REPEAT_MIN: {
                // a{n,}: n copies then a repeat
                Shape inner = shape(re.exp1);
                return inner.times(re.min).then(inner.star());
            }
            case REGEXP_REPEAT_MINMAX: {
                // a{n,m}: n copies, then m - n optional copies each reached from the accept states of the one before
                Shape inner = shape(re.exp1);
                Shape required = inner.times(re.min);
                long optionalCopies = Math.max(0L, (long) re.max - re.min);
                if (optionalCopies == 0) {
                    return required;
                }
                long links = add(mul(required.accepts, inner.initial), mul(mul(inner.accepts, inner.initial), optionalCopies - 1));
                return new Shape(
                    add(required.states, mul(inner.states, optionalCopies)),
                    add(add(required.transitions, mul(inner.transitions, optionalCopies)), links),
                    add(required.accepts, mul(inner.accepts, optionalCopies)),
                    add(required.initial, required.nullable ? inner.initial : 0),
                    required.nullable
                );
            }
            case REGEXP_COMPLEMENT:
            case REGEXP_DEPRECATED_COMPLEMENT:
                return shape(re.exp1).complement();
            case REGEXP_STRING: {
                long length = re.s == null ? 0 : re.s.codePointCount(0, re.s.length());
                return length == 0 ? Shape.EMPTY_STRING : new Shape(length + 1, length, 1, 1, false);
            }
            case REGEXP_CHAR_CLASS: {
                long ranges = re.from == null ? 1 : Math.max(1, re.from.length);
                return new Shape(2, ranges, 1, ranges, false);
            }
            case REGEXP_INTERVAL: {
                // a digit trie with a leading-zero loop; generous per digit, measured at 100 states and 116 transitions for 9 digits
                long digits = Math.max(re.digits, Integer.toString(re.max).length());
                return new Shape(add(mul(digits, 12), 4), add(mul(digits, 14), 10), add(mul(digits, 2), 2), add(mul(digits, 4), 4), false);
            }
            case REGEXP_EMPTY:
                return new Shape(0, 0, 0, 0, false);
            case REGEXP_ANYSTRING:
                return Shape.ANY_STRING;
            case REGEXP_CHAR:
            case REGEXP_CHAR_RANGE:
            case REGEXP_ANYCHAR:
            case REGEXP_AUTOMATON:
                return Shape.SINGLE;
        }
        throw new AssertionError("unexpected RegExp kind: " + re.kind);
    }

    private static long add(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long mul(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
