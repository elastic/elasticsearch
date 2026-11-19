/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Compiler;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Inst;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Parser;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Prog;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.RE2;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Simplify;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Unicode;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Utils;

import java.util.Arrays;

/**
 * A regular expression in RE2 syntax, compiled to a Lucene {@link Automaton} that accepts the strings it matches whole.
 * <p>
 * RE2 is the syntax of Go's {@code regexp} package, and so of PromQL label matchers. Lucene's {@link RegExp} has a
 * grammar of its own, so the expression is parsed and compiled by the fork of RE2/J in the {@code re2} package into the
 * program its matcher runs, and the automaton is read off that program in one pass.
 * <p>
 * The program is a nondeterministic machine whose empty moves are its {@code Alt}, {@code Nop}, {@code Capture} and
 * {@code EmptyWidth} instructions. A state of the automaton is the program counter after a rune and, when the expression
 * has zero-width assertions ({@code ^ $ \A \z \b \B}), the class of that rune. An assertion is a condition on the runes
 * either side of a position, which Lucene has no notion of: the class of the rune before is the state's, and the class
 * of the rune after restricts the transitions out of it.
 */
public final class Re2RegExp {

    private final Prog prog;

    /**
     * @param flags the {@link RE2} parser flags, such as {@link RE2#PERL}, {@link RE2#DOT_NL} and {@link RE2#FOLD_CASE}
     * @throws IllegalArgumentException if the expression isn't valid RE2
     */
    public Re2RegExp(String pattern, int flags) {
        this.prog = Compiler.compileRegexp(Simplify.simplify(Parser.parse(pattern, flags)));
    }

    /**
     * The automaton accepting the strings the expression matches whole; it is not determinized.
     */
    public Automaton toAutomaton() {
        Builder builder = new Builder(prog);
        Automaton automaton = builder.build();
        if (builder.mayHaveDeadStates == false) {
            return automaton;
        }
        // the states past a rune that no match follows are dead, as Lucene's operations leave none
        automaton = Operations.removeDeadStates(automaton);
        // none is live if the expression matches nothing: then the empty automaton, as Lucene's RegExp has it, so
        // that a run automaton can be built from the determinized one
        return automaton.getNumStates() == 0 ? Automata.makeEmpty() : automaton;
    }

    // the classes of the rune on either side of a position; EDGE is before the first or after the last
    private static final int EDGE = 0;
    private static final int NEWLINE = 1;
    private static final int WORD = 2;
    private static final int OTHER = 3;
    private static final int CLASSES = 4;
    private static final int ALL = (1 << CLASSES) - 1;

    /** The runes of each class but {@link #EDGE}, as sorted {@code [lo, hi]} pairs. */
    private static final int[][] CLASS_RANGES = {
        {},
        { '\n', '\n' },
        { '0', '9', 'A', 'Z', '_', '_', 'a', 'z' },
        { 0, '\n' - 1, '\n' + 1, '0' - 1, '9' + 1, 'A' - 1, 'Z' + 1, '_' - 1, '_' + 1, 'a' - 1, 'z' + 1, Character.MAX_CODE_POINT } };

    private static final int[] ANY = { 0, Character.MAX_CODE_POINT };
    private static final int[] ANY_NOT_NL = { 0, '\n' - 1, '\n' + 1, Character.MAX_CODE_POINT };

    private static final class Builder {
        private final Prog prog;
        /** Whether the program has assertions, and so a state needs the class of the rune before it. */
        private final boolean contextual;
        private final Automaton.Builder automaton = new Automaton.Builder();
        /** The state of each program counter and class of the rune before, at {@code pc * CLASSES + class}, or -1. */
        private final int[] states;
        private final IntArrayList pending = new IntArrayList();
        /**
         * The closure being built, which every array below is of, is the {@code closure}-th: an entry of an earlier one
         * is stale, so that they need no clearing.
         */
        private int closure;
        /** The closure each program counter was last reached in, with the classes of the next rune in {@code next}. */
        private final int[] reachedIn;
        /** The classes the next rune may have, as {@code 1 << class} bits, at each rune instruction the closure reaches. */
        private final int[] next;
        /** The rune instructions the closure reaches, in the order it does. */
        private final IntArrayList reached = new IntArrayList();
        /** The closure each program counter and classes of the next rune, at {@code pc << 4 | bits}, was visited in. */
        private final int[] visitedIn;
        private final IntArrayList stack = new IntArrayList();
        /**
         * Whether a state may be dead, no match following it. Every instruction of a program is on a way to its match
         * but {@code Fail}, which an expression that matches nothing compiles to, so only reaching it or an assertion
         * that cuts a way off can leave one.
         */
        private boolean mayHaveDeadStates;

        Builder(Prog prog) {
            this.prog = prog;
            boolean contextual = false;
            for (int pc = 0; pc < prog.numInst(); pc++) {
                contextual |= prog.getInst(pc).op == Inst.EMPTY_WIDTH;
            }
            this.contextual = contextual;
            this.mayHaveDeadStates = contextual;
            this.states = new int[prog.numInst() * CLASSES];
            Arrays.fill(states, -1);
            this.reachedIn = new int[prog.numInst()];
            this.next = new int[prog.numInst()];
            this.visitedIn = new int[prog.numInst() << 4];
        }

        Automaton build() {
            state(prog.start, EDGE);
            for (int p = 0; p < pending.size(); p++) {
                int key = pending.get(p);
                closure(states[key], key / CLASSES, key % CLASSES);
            }
            return automaton.finish();
        }

        private int state(int pc, int previous) {
            int key = pc * CLASSES + (contextual ? previous : EDGE);
            int state = states[key];
            if (state < 0) {
                state = automaton.createState();
                states[key] = state;
                pending.add(key);
            }
            return state;
        }

        /**
         * Adds the transitions out of {@code state}, whose next instruction is {@code pc} and whose rune before is of
         * class {@code previous}: on the runes of the rune instructions the empty moves from {@code pc} reach, of the
         * classes the assertions on the way allow the next rune to have.
         */
        private void closure(int state, int pc, int previous) {
            closure++;
            reached.clear();
            stack.add(pc << 4 | ALL);
            while (stack.isEmpty() == false) {
                int item = stack.removeLast();
                if (visitedIn[item] == closure) {
                    continue;
                }
                visitedIn[item] = closure;
                Inst inst = prog.getInst(item >>> 4);
                int next = item & ALL;
                switch (inst.op) {
                    case Inst.ALT, Inst.ALT_MATCH -> {
                        stack.add(inst.arg << 4 | next);
                        stack.add(inst.out << 4 | next);
                    }
                    case Inst.NOP, Inst.CAPTURE -> stack.add(inst.out << 4 | next);
                    case Inst.EMPTY_WIDTH -> {
                        int allowed = next & allowedNext(inst.arg, previous);
                        if (allowed != 0) {
                            stack.add(inst.out << 4 | allowed);
                        }
                    }
                    case Inst.MATCH -> {
                        if ((next & 1 << EDGE) != 0) {
                            automaton.setAccept(state, true);
                        }
                    }
                    case Inst.RUNE, Inst.RUNE1, Inst.RUNE_ANY, Inst.RUNE_ANY_NOT_NL -> {
                        int runePc = item >>> 4;
                        if (reachedIn[runePc] == closure) {
                            this.next[runePc] |= next;
                        } else {
                            reachedIn[runePc] = closure;
                            this.next[runePc] = next;
                            reached.add(runePc);
                        }
                    }
                    case Inst.FAIL -> mayHaveDeadStates = true;
                    default -> throw new IllegalStateException("unknown instruction [" + inst.op + "]");
                }
            }
            for (int i = 0; i < reached.size(); i++) {
                int runePc = reached.get(i);
                transitions(state, prog.getInst(runePc), next[runePc]);
            }
        }

        private void transitions(int from, Inst inst, int next) {
            if (contextual == false && inst.op == Inst.RUNE1) {
                automaton.addTransition(from, state(inst.out, EDGE), inst.runes[0]);
                return;
            }
            int[] runes = runes(inst);
            if (contextual == false) {
                int to = state(inst.out, EDGE);
                for (int i = 0; i < runes.length; i += 2) {
                    automaton.addTransition(from, to, runes[i], runes[i + 1]);
                }
                return;
            }
            for (int cls = NEWLINE; cls <= OTHER; cls++) {
                if ((next & 1 << cls) != 0) {
                    addIntersection(from, inst.out, cls, runes);
                }
            }
        }

        /**
         * Transitions from {@code from} to the state after {@code pc} and a rune of class {@code cls}, on the runes
         * of {@code runes}, sorted pairs, of that class. The state is only made if there is one, so that every state
         * is reachable.
         */
        private void addIntersection(int from, int pc, int cls, int[] runes) {
            int[] a = runes;
            int[] b = CLASS_RANGES[cls];
            int to = -1;
            int i = 0;
            int j = 0;
            while (i < a.length && j < b.length) {
                int lo = Math.max(a[i], b[j]);
                int hi = Math.min(a[i + 1], b[j + 1]);
                if (lo <= hi) {
                    if (to < 0) {
                        to = state(pc, cls);
                    }
                    automaton.addTransition(from, to, lo, hi);
                }
                if (a[i + 1] < b[j + 1]) {
                    i += 2;
                } else {
                    j += 2;
                }
            }
        }

        /** The runes a rune instruction matches, as sorted {@code [lo, hi]} pairs. */
        private static int[] runes(Inst inst) {
            return switch (inst.op) {
                case Inst.RUNE_ANY -> ANY;
                case Inst.RUNE_ANY_NOT_NL -> ANY_NOT_NL;
                case Inst.RUNE1 -> new int[] { inst.runes[0], inst.runes[0] };
                case Inst.RUNE -> inst.runes.length == 1 ? literal(inst.runes[0], (inst.arg & RE2.FOLD_CASE) != 0) : inst.runes;
                default -> throw new IllegalStateException("[" + inst.op + "] is no rune instruction");
            };
        }

        /** A literal rune and, if it folds case, the runes it folds to, as the matcher's {@code Inst.matchRune} has them. */
        private static int[] literal(int rune, boolean fold) {
            if (fold == false) {
                return new int[] { rune, rune };
            }
            IntArrayList orbit = new IntArrayList();
            int r = rune;
            do {
                orbit.add(r);
                r = Unicode.simpleFold(r);
            } while (r != rune);
            int[] sorted = orbit.toArray();
            Arrays.sort(sorted);
            int[] ranges = new int[sorted.length * 2];
            for (int i = 0; i < sorted.length; i++) {
                ranges[2 * i] = sorted[i];
                ranges[2 * i + 1] = sorted[i];
            }
            return ranges;
        }

        /** The classes the assertions of {@code empty}, {@code EMPTY_*} flags, allow the next rune to have. */
        private static int allowedNext(int empty, int previous) {
            int allowed = ALL;
            int word = 1 << WORD;
            if ((empty & Utils.EMPTY_BEGIN_TEXT) != 0 && previous != EDGE) {
                allowed = 0;
            }
            if ((empty & Utils.EMPTY_BEGIN_LINE) != 0 && previous != EDGE && previous != NEWLINE) {
                allowed = 0;
            }
            if ((empty & Utils.EMPTY_END_TEXT) != 0) {
                allowed &= 1 << EDGE;
            }
            if ((empty & Utils.EMPTY_END_LINE) != 0) {
                allowed &= 1 << EDGE | 1 << NEWLINE;
            }
            if ((empty & Utils.EMPTY_WORD_BOUNDARY) != 0) {
                allowed &= previous == WORD ? ALL & ~word : word;
            }
            if ((empty & Utils.EMPTY_NO_WORD_BOUNDARY) != 0) {
                allowed &= previous == WORD ? word : ALL & ~word;
            }
            return allowed;
        }
    }
}
