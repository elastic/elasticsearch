/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Parser;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.RE2;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Regexp;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Unicode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A regular expression in RE2 syntax, compiled to a Lucene {@link Automaton} that accepts the strings it matches whole.
 * <p>
 * RE2 is the syntax of Go's {@code regexp} package, and so of PromQL label matchers. Lucene's {@link RegExp} has a
 * grammar of its own, so the expression is parsed by the fork of RE2/J's parser in the {@code re2} package, with the
 * {@code Perl} and {@code DotNL} flags Prometheus parses label matchers with, and its tree is built into an automaton
 * with Lucene's {@link Operations}.
 */
public final class Re2RegExp {

    private final Regexp regexp;

    public Re2RegExp(String pattern) {
        this(pattern, false);
    }

    /**
     * @param ignoreCase whether to match as if the expression started with {@code (?i)}
     * @throws IllegalArgumentException if the expression isn't valid RE2
     */
    public Re2RegExp(String pattern, boolean ignoreCase) {
        this.regexp = Parser.parse(pattern, RE2.PERL | RE2.DOT_NL | (ignoreCase ? RE2.FOLD_CASE : 0));
    }

    /**
     * The automaton accepting the strings the expression matches whole; it is not determinized.
     */
    public Automaton toAutomaton() {
        Builder builder = new Builder();
        Automaton automaton = builder.build(regexp);
        if (builder.hasAssertions) {
            automaton = Assertions.resolve(automaton);
        }
        // as Lucene's RegExp does, so that a run automaton can be built from the determinized one
        return Operations.isEmpty(automaton) ? Automata.makeEmpty() : automaton;
    }

    private static final class Builder {
        private boolean hasAssertions;

        private Automaton build(Regexp re) {
            return switch (re.op) {
                case NO_MATCH -> Automata.makeEmpty();
                case EMPTY_MATCH -> Automata.makeEmptyString();
                case LITERAL -> {
                    List<Automaton> runes = new ArrayList<>(re.runes.length);
                    for (int rune : re.runes) {
                        runes.add((re.flags & RE2.FOLD_CASE) != 0 ? Automata.makeCharSet(orbit(rune)) : Automata.makeChar(rune));
                    }
                    yield Operations.concatenate(runes);
                }
                case CHAR_CLASS -> chars(re.runes);
                case ANY_CHAR_NOT_NL -> chars(new int[] { 0, '\n' - 1, '\n' + 1, Character.MAX_CODE_POINT });
                case ANY_CHAR -> Automata.makeAnyChar();
                case BEGIN_LINE, END_LINE, BEGIN_TEXT, END_TEXT, WORD_BOUNDARY, NO_WORD_BOUNDARY -> {
                    hasAssertions = true;
                    yield Automata.makeChar(Assertions.marker(re.op));
                }
                case CAPTURE -> build(re.subs[0]);
                case STAR -> Operations.repeat(build(re.subs[0]));
                case PLUS -> Operations.repeat(build(re.subs[0]), 1);
                case QUEST -> Operations.optional(build(re.subs[0]));
                case REPEAT -> re.max < 0
                    ? Operations.repeat(build(re.subs[0]), re.min)
                    : Operations.repeat(build(re.subs[0]), re.min, re.max);
                case CONCAT -> Operations.concatenate(Arrays.stream(re.subs).map(this::build).toList());
                case ALTERNATE -> Operations.union(Arrays.stream(re.subs).map(this::build).toList());
                case LEFT_PAREN, VERTICAL_BAR -> throw new IllegalStateException("[" + re.op + "] is only used while parsing");
            };
        }

        /** {@code rune} and the code points it folds to. */
        private static int[] orbit(int rune) {
            List<Integer> orbit = new ArrayList<>();
            int r = rune;
            do {
                orbit.add(r);
                r = Unicode.simpleFold(r);
            } while (r != rune);
            return orbit.stream().mapToInt(Integer::intValue).toArray();
        }

        /** Any one code point in {@code ranges}, {@code [lo, hi]} pairs. */
        private static Automaton chars(int[] ranges) {
            Automaton automaton = new Automaton();
            int start = automaton.createState();
            int end = automaton.createState();
            automaton.setAccept(end, true);
            for (int i = 0; i < ranges.length; i += 2) {
                automaton.addTransition(start, end, ranges[i], ranges[i + 1]);
            }
            automaton.finishState();
            return automaton;
        }
    }

    /**
     * Lucene has nothing for RE2's zero-width assertions, conditions on the code points either side of a position. So
     * each is built as a marker, a label past the last code point, the markers that the code points around them don't
     * satisfy are cut off by intersecting with an automaton of the conditions, and the rest are dropped.
     */
    private static final class Assertions {
        private static final List<Regexp.Op> KINDS = List.of(
            Regexp.Op.BEGIN_LINE,
            Regexp.Op.END_LINE,
            Regexp.Op.BEGIN_TEXT,
            Regexp.Op.END_TEXT,
            Regexp.Op.WORD_BOUNDARY,
            Regexp.Op.NO_WORD_BOUNDARY
        );

        // the classes of the code point on either side of a position; EDGE is before the first or after the last
        private static final int EDGE = 0;
        private static final int NEWLINE = 1;
        private static final int WORD = 2;
        private static final int OTHER = 3;
        private static final int ALL = 0b1111;

        /** The code points of each class but {@link #EDGE}, as {@code [lo, hi]} pairs. */
        private static final int[][] CLASS_RANGES = {
            {},
            { '\n', '\n' },
            { '0', '9', 'A', 'Z', '_', '_', 'a', 'z' },
            { 0, '\n' - 1, '\n' + 1, '0' - 1, '9' + 1, 'A' - 1, 'Z' + 1, '_' - 1, '_' + 1, 'a' - 1, 'z' + 1, Character.MAX_CODE_POINT } };

        static int marker(Regexp.Op kind) {
            return Character.MAX_CODE_POINT + 1 + KINDS.indexOf(kind);
        }

        static Automaton resolve(Automaton automaton) {
            return dropMarkers(Operations.intersection(automaton, conditions()));
        }

        /**
         * The strings of code points and markers where every marker holds: its states pair the class of the code point
         * before with the classes the markers since allow the next one to have.
         */
        private static Automaton conditions() {
            Automaton.Builder builder = new Automaton.Builder();
            Map<Integer, Integer> states = new HashMap<>();
            ArrayDeque<Integer> pending = new ArrayDeque<>();
            state(builder, states, pending, EDGE, ALL);
            while (pending.isEmpty() == false) {
                int key = pending.poll();
                int previous = key >> 4;
                int next = key & ALL;
                int from = states.get(key);
                builder.setAccept(from, (next & 1 << EDGE) != 0);
                for (int cls = NEWLINE; cls <= OTHER; cls++) {
                    if ((next & 1 << cls) != 0) {
                        int to = state(builder, states, pending, cls, ALL);
                        int[] ranges = CLASS_RANGES[cls];
                        for (int i = 0; i < ranges.length; i += 2) {
                            builder.addTransition(from, to, ranges[i], ranges[i + 1]);
                        }
                    }
                }
                for (Regexp.Op kind : KINDS) {
                    int allowed = next & allowedNext(kind, previous);
                    if (allowed != 0) {
                        builder.addTransition(from, state(builder, states, pending, previous, allowed), marker(kind));
                    }
                }
            }
            return builder.finish();
        }

        private static int state(
            Automaton.Builder builder,
            Map<Integer, Integer> states,
            ArrayDeque<Integer> pending,
            int previous,
            int next
        ) {
            int key = previous << 4 | next;
            Integer state = states.get(key);
            if (state == null) {
                state = builder.createState();
                states.put(key, state);
                pending.add(key);
            }
            return state;
        }

        /** The classes an assertion allows the next code point to have, given the previous one's; none if it fails. */
        private static int allowedNext(Regexp.Op kind, int previous) {
            int word = 1 << WORD;
            return switch (kind) {
                case BEGIN_TEXT -> previous == EDGE ? ALL : 0;
                case END_TEXT -> 1 << EDGE;
                case BEGIN_LINE -> previous == EDGE || previous == NEWLINE ? ALL : 0;
                case END_LINE -> 1 << EDGE | 1 << NEWLINE;
                case WORD_BOUNDARY -> previous == WORD ? ALL & ~word : word;
                case NO_WORD_BOUNDARY -> previous == WORD ? word : ALL & ~word;
                default -> throw new IllegalArgumentException("[" + kind + "] is no assertion");
            };
        }

        /** The same automaton with its marker transitions taken as empty ones. */
        private static Automaton dropMarkers(Automaton automaton) {
            Automaton.Builder builder = new Automaton.Builder();
            for (int state = 0; state < automaton.getNumStates(); state++) {
                builder.createState();
            }
            Transition t = new Transition();
            for (int state = 0; state < automaton.getNumStates(); state++) {
                // the states the markers from this one lead to, itself included
                BitSet reached = new BitSet();
                ArrayDeque<Integer> pending = new ArrayDeque<>(List.of(state));
                while (pending.isEmpty() == false) {
                    int from = pending.poll();
                    if (reached.get(from)) {
                        continue;
                    }
                    reached.set(from);
                    if (automaton.isAccept(from)) {
                        builder.setAccept(state, true);
                    }
                    for (int i = automaton.initTransition(from, t); i > 0; i--) {
                        automaton.getNextTransition(t);
                        if (t.min > Character.MAX_CODE_POINT) {
                            pending.add(t.dest);
                        } else {
                            builder.addTransition(state, t.dest, t.min, t.max);
                        }
                    }
                }
            }
            return builder.finish();
        }
    }
}
