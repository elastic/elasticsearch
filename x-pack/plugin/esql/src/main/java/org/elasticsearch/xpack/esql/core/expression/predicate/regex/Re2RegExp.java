/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import com.google.re2j.Re2Programs;
import com.google.re2j.Re2Programs.Instruction;
import com.google.re2j.Re2Programs.Parsed;
import com.google.re2j.Re2Programs.Program;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingRegExp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * A regular expression in RE2 syntax, compiled to a Lucene {@link Automaton} that accepts the strings it matches whole.
 * <p>
 * RE2 is the syntax of Go's {@code regexp} package, and so of PromQL label matchers. Lucene's {@link RegExp} has a
 * grammar of its own, so the expression is parsed and compiled by RE2/J, through {@link Re2Programs}, into the
 * program its matcher runs, and the automaton is read off that program in one pass.
 * <p>
 * The program is a nondeterministic machine whose empty moves are its {@code Alt}, {@code Nop}, {@code Capture} and
 * {@code EmptyWidth} instructions. A state of the automaton is the program counter after a rune and, when the expression
 * has zero-width assertions ({@code ^ $ \A \z \b \B}), the class of that rune. An assertion is a condition on the runes
 * either side of a position, which Lucene has no notion of: the class of the rune before is the state's, and the class
 * of the rune after restricts the transitions out of it.
 */
public final class Re2RegExp {

    /** The strings of an expression that is an alternation of literals, which needs no parsing; null if it isn't one. */
    private final List<String> literals;
    /** The parsed expression, simplified and compiled only once the memory of that is reserved; null for literals. */
    private final Parsed parsed;

    /**
     * @param flags the RE2/J parser flags, such as {@link Re2Programs#PERL}, {@link Re2Programs#DOT_NL} and {@link Re2Programs#FOLD_CASE}
     * @throws IllegalArgumentException if the expression isn't valid RE2
     */
    public Re2RegExp(String pattern, int flags) {
        this.literals = (flags & Re2Programs.FOLD_CASE) == 0 ? literals(pattern) : null;
        this.parsed = literals == null ? Re2Programs.parse(pattern, flags) : null;
    }

    /**
     * Peak bytes per instruction of the program: its instruction, the node simplifying expands to it, and the builder's
     * arrays indexed by instruction, of which the visited marks take sixteen ints.
     */
    public static final long BYTES_PER_INSTRUCTION = 256L;

    /**
     * Work units per state or transition built, against one per instruction a closure walks, as a Lucene regular
     * expression's build weighs them.
     */
    private static final long WORK_PER_BUILT_STATE_OR_TRANSITION = 64L;

    /**
     * Most states and transitions one build may make: as many as {@link CircuitBreakingRegExp#DEFAULT_WORK_LIMIT} lets a
     * Lucene regular expression build. Concatenated repeats of a class with many ranges, such as {@code \pL{1000}\pL{1000}},
     * pass the parser's limit on nested repeats and add a transition per range per copy.
     */
    static final long MAX_BUILT_STATES_AND_TRANSITIONS = CircuitBreakingRegExp.DEFAULT_WORK_LIMIT / WORK_PER_BUILT_STATE_OR_TRANSITION;

    /**
     * Peak bytes per state and per transition while building: the builder's entries with growth headroom, the finished
     * automaton's copy, and the reversed copy that trimming dead states makes.
     */
    static final long BYTES_PER_STATE = 64L;
    static final long BYTES_PER_TRANSITION = 72L;
    /** Reserve in steps of this size rather than on every transition. */
    private static final long CHARGE_STEP = 64 * 1024L;

    /**
     * The automaton accepting the strings the expression matches whole; it is not determinized.
     *
     * @throws TooComplexToDeterminizeException if building it takes more work than {@link CircuitBreakingRegExp#DEFAULT_WORK_LIMIT}:
     *         a unit per instruction the empty moves from each state walk, and {@link #WORK_PER_BUILT_STATE_OR_TRANSITION}
     *         per state and transition it has
     */
    public Automaton toAutomaton() {
        return toAutomaton(null, null);
    }

    /**
     * {@link #toAutomaton()}, with the memory of the build reserved on {@code breaker} while it runs and released before
     * it returns; the caller accounts the automaton it gets.
     */
    public Automaton toAutomaton(CircuitBreaker breaker, String label) {
        return toAutomaton(breaker, label, CircuitBreakingRegExp.DEFAULT_WORK_LIMIT);
    }

    /** {@link #toAutomaton(CircuitBreaker, String)} within {@code workLimit} rather than the default, so tests can reach it. */
    Automaton toAutomaton(CircuitBreaker breaker, String label, long workLimit) {
        if (literals != null) {
            if (literals.size() == 1) {
                return Automata.makeString(literals.getFirst());
            }
            List<BytesRef> sorted = literals.stream().map(BytesRef::new).sorted().toList();
            return Automata.makeStringUnion(sorted);
        }
        // the program is as large as its counted repeats expand to, so its size is bounded and reserved before it is built
        long instructions = parsed.programSize();
        if (instructions > workLimit / WORK_PER_BUILT_STATE_OR_TRANSITION) {
            throw new TooComplexToDeterminizeException(Automata.makeEmpty(), Math.toIntExact(workLimit));
        }
        long programBytes = 0;
        if (breaker != null) {
            breaker.addEstimateBytesAndMaybeBreak(instructions * BYTES_PER_INSTRUCTION, label);
            programBytes = instructions * BYTES_PER_INSTRUCTION;
        }
        Builder builder = null;
        try {
            builder = new Builder(parsed.compile(), breaker, label, workLimit);
            Automaton automaton = builder.build();
            if (builder.mayHaveDeadStates == false) {
                return automaton;
            }
            // the states past a rune that no match follows are dead, as Lucene's operations leave none
            automaton = Operations.removeDeadStates(automaton);
            // none is live if the expression matches nothing: then the empty automaton, as Lucene's RegExp has it, so
            // that a run automaton can be built from the determinized one
            return automaton.getNumStates() == 0 ? Automata.makeEmpty() : automaton;
        } finally {
            if (builder != null) {
                builder.release();
            }
            if (programBytes > 0) {
                breaker.addWithoutBreaking(-programBytes);
            }
        }
    }

    /** The runes that aren't literals in RE2, outside a class. */
    private static final String META = "\\.+*?()|[]{}^$";

    /**
     * The strings of an alternation of literals, optionally in a group, optionally anchored at either end, the shape
     * of most PromQL label matchers, which Grafana's multi-value variables expand to: {@code a}, {@code a|b},
     * {@code ^(?:a|b)$}. As the expression is matched whole, its anchors at either end hold anyway. Null if the
     * expression isn't of that shape, or its literals have a rune past the BMP or are too long to make a union of.
     */
    private static List<String> literals(String pattern) {
        int start = 0;
        int end = pattern.length();
        if (start < end && pattern.charAt(start) == '^') {
            start++;
        }
        if (start < end && pattern.charAt(end - 1) == '$') {
            end--;
        }
        if (end - start >= 2 && pattern.charAt(start) == '(' && pattern.charAt(end - 1) == ')') {
            start += pattern.startsWith("?:", start + 1) ? 3 : 1;
            end--;
        }
        List<String> literals = new ArrayList<>();
        int from = start;
        for (int i = start; i < end; i++) {
            char c = pattern.charAt(i);
            if (c == '|') {
                literals.add(pattern.substring(from, i));
                from = i + 1;
            } else if (META.indexOf(c) >= 0 || Character.isSurrogate(c)) {
                return null;
            }
        }
        literals.add(pattern.substring(from, end));
        for (String literal : literals) {
            if (literal.length() * 3 > Automata.MAX_STRING_UNION_TERM_LENGTH) {
                return null;
            }
        }
        return literals;
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
        private final Program prog;
        /**
         * The assertions that hold wherever they are reached: one at the beginning that no rune comes before, as in
         * {@code ^a}, and one at the end that no rune comes after, as in {@code a$}, as the expression is matched whole.
         */
        private final boolean[] holds;
        /** Whether the program has other assertions, and so a state needs the class of the rune before it. */
        private final boolean contextual;
        private final Automaton.Builder automaton = new Automaton.Builder();
        /** The state of each program counter and class of the rune before, at {@code pc * CLASSES + class}, or -1. */
        private final int[] stateOf;
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
        /** Accounts the build, or null if nothing does; only the bound on its size holds then. */
        private final CircuitBreaker breaker;
        private final String label;
        private long states;
        private long transitions;
        /**
         * The instructions the closures walked: a closure may walk a long run of empty moves, such as {@code (){1000}},
         * from every state and add few transitions, so the walk is bounded as well as what it builds.
         */
        private long steps;
        private long reserved;
        private final long workLimit;

        Builder(Program prog, CircuitBreaker breaker, String label, long workLimit) {
            this.prog = prog;
            this.breaker = breaker;
            this.label = label;
            this.workLimit = workLimit;
            int n = prog.numInst();
            this.holds = new boolean[n];
            BitSet afterRune = null;
            BitSet beforeRune = null;
            boolean contextual = false;
            for (int pc = 0; pc < n; pc++) {
                Instruction inst = prog.getInst(pc);
                if (inst.op != Instruction.EMPTY_WIDTH) {
                    continue;
                }
                int begin = Re2Programs.EMPTY_BEGIN_TEXT | Re2Programs.EMPTY_BEGIN_LINE;
                int end = Re2Programs.EMPTY_END_TEXT | Re2Programs.EMPTY_END_LINE;
                boolean holds = (inst.arg & ~(begin | end)) == 0;
                if (holds && (inst.arg & begin) != 0) {
                    if (afterRune == null) {
                        afterRune = afterRune(prog);
                    }
                    holds = afterRune.get(pc) == false;
                }
                if (holds && (inst.arg & end) != 0) {
                    if (beforeRune == null) {
                        beforeRune = beforeRune(prog);
                    }
                    holds = beforeRune.get(inst.out) == false;
                }
                this.holds[pc] = holds;
                contextual |= holds == false;
            }
            this.contextual = contextual;
            this.mayHaveDeadStates = contextual;
            this.stateOf = new int[prog.numInst() * CLASSES];
            Arrays.fill(stateOf, -1);
            this.reachedIn = new int[prog.numInst()];
            this.next = new int[prog.numInst()];
            this.visitedIn = new int[prog.numInst() << 4];
        }

        Automaton build() {
            state(prog.start, EDGE);
            for (int p = 0; p < pending.size(); p++) {
                int key = pending.get(p);
                closure(stateOf[key], key / CLASSES, key % CLASSES);
            }
            return automaton.finish();
        }

        private int state(int pc, int previous) {
            int key = pc * CLASSES + (contextual ? previous : EDGE);
            int state = stateOf[key];
            if (state < 0) {
                states++;
                charge();
                state = automaton.createState();
                stateOf[key] = state;
                pending.add(key);
            }
            return state;
        }

        private void addTransition(int from, int to, int min, int max) {
            transitions++;
            charge();
            automaton.addTransition(from, to, min, max);
        }

        /** Stops the build past its work limit. */
        private void checkWork() {
            if (steps + (states + transitions) * WORK_PER_BUILT_STATE_OR_TRANSITION > workLimit) {
                throw new TooComplexToDeterminizeException(Automata.makeEmpty(), Math.toIntExact(workLimit));
            }
        }

        /** Stops the build past its work limit, and reserves what it has grown by on the breaker. */
        private void charge() {
            checkWork();
            if (breaker == null) {
                return;
            }
            long bytes = states * BYTES_PER_STATE + transitions * BYTES_PER_TRANSITION;
            if (bytes - reserved >= CHARGE_STEP) {
                breaker.addEstimateBytesAndMaybeBreak(bytes - reserved, label);
                reserved = bytes;
            }
        }

        void release() {
            if (reserved > 0) {
                breaker.addWithoutBreaking(-reserved);
                reserved = 0;
            }
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
                steps++;
                checkWork();
                Instruction inst = prog.getInst(item >>> 4);
                int next = item & ALL;
                switch (inst.op) {
                    case Instruction.ALT, Instruction.ALT_MATCH -> {
                        stack.add(inst.arg << 4 | next);
                        stack.add(inst.out << 4 | next);
                    }
                    case Instruction.NOP, Instruction.CAPTURE -> stack.add(inst.out << 4 | next);
                    case Instruction.EMPTY_WIDTH -> {
                        if (holds[item >>> 4]) {
                            stack.add(inst.out << 4 | next);
                            continue;
                        }
                        int allowed = next & allowedNext(inst.arg, previous);
                        if (allowed != 0) {
                            stack.add(inst.out << 4 | allowed);
                        }
                    }
                    case Instruction.MATCH -> {
                        if ((next & 1 << EDGE) != 0) {
                            automaton.setAccept(state, true);
                        }
                    }
                    case Instruction.RUNE, Instruction.RUNE1, Instruction.RUNE_ANY, Instruction.RUNE_ANY_NOT_NL -> {
                        int runePc = item >>> 4;
                        if (reachedIn[runePc] == closure) {
                            this.next[runePc] |= next;
                        } else {
                            reachedIn[runePc] = closure;
                            this.next[runePc] = next;
                            reached.add(runePc);
                        }
                    }
                    case Instruction.FAIL -> mayHaveDeadStates = true;
                    default -> throw new IllegalStateException("unknown instruction [" + inst.op + "]");
                }
            }
            for (int i = 0; i < reached.size(); i++) {
                int runePc = reached.get(i);
                transitions(state, prog.getInst(runePc), next[runePc]);
            }
        }

        private void transitions(int from, Instruction inst, int next) {
            if (contextual == false && inst.op == Instruction.RUNE1) {
                addTransition(from, state(inst.out, EDGE), inst.runes[0], inst.runes[0]);
                return;
            }
            int[] runes = runes(inst);
            if (contextual == false) {
                int to = state(inst.out, EDGE);
                for (int i = 0; i < runes.length; i += 2) {
                    addTransition(from, to, runes[i], runes[i + 1]);
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
                    addTransition(from, to, lo, hi);
                }
                if (a[i + 1] < b[j + 1]) {
                    i += 2;
                } else {
                    j += 2;
                }
            }
        }

        /** The instructions the empty moves after a rune reach, the positions that a rune comes before. */
        private static BitSet afterRune(Program prog) {
            BitSet reached = new BitSet(prog.numInst());
            IntArrayList stack = new IntArrayList();
            for (int pc = 0; pc < prog.numInst(); pc++) {
                if (isRune(prog.getInst(pc))) {
                    stack.add(prog.getInst(pc).out);
                }
            }
            emptyMoves(prog, stack, reached);
            return reached;
        }

        /**
         * The instructions whose empty moves reach a rune instruction, them included, the positions that a rune may come
         * after: found in one walk back along the empty moves from every rune instruction, rather than one walk per assertion.
         */
        private static BitSet beforeRune(Program prog) {
            int n = prog.numInst();
            // the empty moves into each instruction, as linked lists: head[to] and next[edge], from[edge]
            int[] head = new int[n];
            Arrays.fill(head, -1);
            IntArrayList from = new IntArrayList();
            IntArrayList next = new IntArrayList();
            for (int pc = 0; pc < n; pc++) {
                Instruction inst = prog.getInst(pc);
                switch (inst.op) {
                    case Instruction.ALT, Instruction.ALT_MATCH -> {
                        head[inst.out] = edge(from, next, pc, head[inst.out]);
                        head[inst.arg] = edge(from, next, pc, head[inst.arg]);
                    }
                    case Instruction.NOP, Instruction.CAPTURE, Instruction.EMPTY_WIDTH -> head[inst.out] = edge(
                        from,
                        next,
                        pc,
                        head[inst.out]
                    );
                    case Instruction.RUNE, Instruction.RUNE1, Instruction.RUNE_ANY, Instruction.RUNE_ANY_NOT_NL, Instruction.MATCH,
                        Instruction.FAIL -> {
                        // a move past a rune, or the end of the way
                    }
                    default -> throw new IllegalStateException("unknown instruction [" + inst.op + "]");
                }
            }
            BitSet reached = new BitSet(n);
            IntArrayList stack = new IntArrayList();
            for (int pc = 0; pc < n; pc++) {
                if (isRune(prog.getInst(pc))) {
                    stack.add(pc);
                }
            }
            while (stack.isEmpty() == false) {
                int pc = stack.removeLast();
                if (reached.get(pc)) {
                    continue;
                }
                reached.set(pc);
                for (int e = head[pc]; e >= 0; e = next.get(e)) {
                    stack.add(from.get(e));
                }
            }
            return reached;
        }

        private static int edge(IntArrayList from, IntArrayList next, int pc, int previous) {
            from.add(pc);
            next.add(previous);
            return from.size() - 1;
        }

        /** Adds the instructions the empty moves from those on {@code stack} reach, them included, to {@code reached}. */
        private static void emptyMoves(Program prog, IntArrayList stack, BitSet reached) {
            while (stack.isEmpty() == false) {
                int pc = stack.removeLast();
                if (reached.get(pc)) {
                    continue;
                }
                reached.set(pc);
                Instruction inst = prog.getInst(pc);
                switch (inst.op) {
                    case Instruction.ALT, Instruction.ALT_MATCH -> {
                        stack.add(inst.out);
                        stack.add(inst.arg);
                    }
                    case Instruction.NOP, Instruction.CAPTURE, Instruction.EMPTY_WIDTH -> stack.add(inst.out);
                    case Instruction.RUNE, Instruction.RUNE1, Instruction.RUNE_ANY, Instruction.RUNE_ANY_NOT_NL, Instruction.MATCH,
                        Instruction.FAIL -> {
                        // a move past a rune, or the end of the way
                    }
                    default -> throw new IllegalStateException("unknown instruction [" + inst.op + "]");
                }
            }
        }

        private static boolean isRune(Instruction inst) {
            return switch (inst.op) {
                case Instruction.RUNE, Instruction.RUNE1, Instruction.RUNE_ANY, Instruction.RUNE_ANY_NOT_NL -> true;
                case Instruction.ALT, Instruction.ALT_MATCH, Instruction.NOP, Instruction.CAPTURE, Instruction.EMPTY_WIDTH,
                    Instruction.MATCH, Instruction.FAIL -> false;
                default -> throw new IllegalStateException("unknown instruction [" + inst.op + "]");
            };
        }

        /** The runes a rune instruction matches, as sorted {@code [lo, hi]} pairs. */
        private static int[] runes(Instruction inst) {
            return switch (inst.op) {
                case Instruction.RUNE_ANY -> ANY;
                case Instruction.RUNE_ANY_NOT_NL -> ANY_NOT_NL;
                case Instruction.RUNE1 -> new int[] { inst.runes[0], inst.runes[0] };
                case Instruction.RUNE -> inst.runes.length == 1
                    ? literal(inst.runes[0], (inst.arg & Re2Programs.FOLD_CASE) != 0)
                    : inst.runes;
                default -> throw new IllegalStateException("[" + inst.op + "] is no rune instruction");
            };
        }

        /** A literal rune and, if it folds case, the runes it folds to, as the matcher's {@code Instruction.matchRune} has them. */
        private static int[] literal(int rune, boolean fold) {
            if (fold == false) {
                return new int[] { rune, rune };
            }
            IntArrayList orbit = new IntArrayList();
            int r = rune;
            do {
                orbit.add(r);
                r = Re2Programs.simpleFold(r);
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
            if ((empty & Re2Programs.EMPTY_BEGIN_TEXT) != 0 && previous != EDGE) {
                allowed = 0;
            }
            if ((empty & Re2Programs.EMPTY_BEGIN_LINE) != 0 && previous != EDGE && previous != NEWLINE) {
                allowed = 0;
            }
            if ((empty & Re2Programs.EMPTY_END_TEXT) != 0) {
                allowed &= 1 << EDGE;
            }
            if ((empty & Re2Programs.EMPTY_END_LINE) != 0) {
                allowed &= 1 << EDGE | 1 << NEWLINE;
            }
            if ((empty & Re2Programs.EMPTY_WORD_BOUNDARY) != 0) {
                allowed &= previous == WORD ? ALL & ~word : word;
            }
            if ((empty & Re2Programs.EMPTY_NO_WORD_BOUNDARY) != 0) {
                allowed &= previous == WORD ? word : ALL & ~word;
            }
            return allowed;
        }
    }
}
