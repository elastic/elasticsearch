/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package com.google.re2j;

/**
 * Exposes the parser and compiler of RE2/J, which it keeps package private, so that a PromQL label matcher can be
 * compiled to a Lucene automaton from the same program RE2/J matches with. It lives in RE2/J's package to reach them,
 * and only reads what they produce, so RE2/J stays the unmodified dependency.
 * <p>
 * RE2/J bounds each counted repeat to 1000 but not the copies nested repeats make, which simplifying the expression
 * expands, so {@link #parse} adds Go's bound on them.
 */
public final class Re2Programs {

    private Re2Programs() {}

    public static final int FOLD_CASE = RE2.FOLD_CASE;
    public static final int DOT_NL = RE2.DOT_NL;
    public static final int NON_GREEDY = RE2.NON_GREEDY;
    public static final int PERL = RE2.PERL;

    public static final int EMPTY_BEGIN_LINE = Utils.EMPTY_BEGIN_LINE;
    public static final int EMPTY_END_LINE = Utils.EMPTY_END_LINE;
    public static final int EMPTY_BEGIN_TEXT = Utils.EMPTY_BEGIN_TEXT;
    public static final int EMPTY_END_TEXT = Utils.EMPTY_END_TEXT;
    public static final int EMPTY_WORD_BOUNDARY = Utils.EMPTY_WORD_BOUNDARY;
    public static final int EMPTY_NO_WORD_BOUNDARY = Utils.EMPTY_NO_WORD_BOUNDARY;

    /** The most copies of its innermost expression that nested counted repeats may make, as in Go's regexp/syntax. */
    private static final int MAX_REPEAT_COPIES = 1000;

    /**
     * Parses {@code pattern} with the {@link RE2} parser {@code flags}.
     *
     * @throws IllegalArgumentException if the expression isn't valid RE2, or its nested repeats make too many copies
     */
    public static Parsed parse(String pattern, int flags) {
        Regexp re;
        try {
            re = Parser.parse(pattern, flags);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        if (repeatIsValid(re, MAX_REPEAT_COPIES) == false) {
            throw new IllegalArgumentException("error parsing regexp: invalid repeat count: `" + pattern + "`");
        }
        return new Parsed(re);
    }

    /** The runes {@code r} folds to, in turn, as RE2/J's matcher folds case. */
    public static int simpleFold(int r) {
        return Unicode.simpleFold(r);
    }

    /**
     * Whether the repeats of {@code re} make at most {@code n} copies of their innermost expression, nested ones
     * multiplying, as Go's regexp/syntax checks them.
     */
    private static boolean repeatIsValid(Regexp re, int n) {
        if (re.op == Regexp.Op.REPEAT) {
            int m = re.max;
            if (m == 0) {
                return true;
            }
            if (m < 0) {
                m = re.min;
            }
            if (m > n) {
                return false;
            }
            if (m > 0) {
                n /= m;
            }
        }
        if (re.subs != null) {
            for (Regexp sub : re.subs) {
                if (repeatIsValid(sub, n) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    /** A parsed expression, not yet simplified or compiled. */
    public static final class Parsed {
        private final Regexp re;

        private Parsed(Regexp re) {
            this.re = re;
        }

        /**
         * An upper bound of the instructions of the compiled program, as Go's regexp/syntax calcSize counts them before
         * the expression is simplified, so that the memory of simplifying and compiling it can be reserved first.
         */
        public long programSize() {
            return programSize(re);
        }

        /** Simplifies and compiles the expression into the program RE2/J matches with. */
        public Program compile() {
            return new Program(Compiler.compileRegexp(Simplify.simplify(re)));
        }

        private static long programSize(Regexp re) {
            long size = 0;
            switch (re.op) {
                case STAR, PLUS, QUEST -> {
                    // simplifying repeats the empty string once, and a star, plus or quest of the same one once
                    Regexp sub = re.subs[0];
                    if (sub.op == Regexp.Op.EMPTY_MATCH || (sub.op == re.op && (re.flags & NON_GREEDY) == (sub.flags & NON_GREEDY))) {
                        return programSize(sub);
                    }
                    // a star can be 1+ or 2+; 2 is the pessimistic count
                    size = (re.op == Regexp.Op.STAR ? 2 : 1) + programSize(sub);
                }
                case LITERAL -> size = re.runes.length;
                case CAPTURE -> size = 2 + programSize(re.subs[0]);
                case CONCAT -> {
                    for (Regexp sub : re.subs) {
                        size += programSize(sub);
                    }
                }
                case ALTERNATE -> {
                    for (Regexp sub : re.subs) {
                        size += programSize(sub);
                    }
                    if (re.subs.length > 1) {
                        size += re.subs.length - 1;
                    }
                }
                case REPEAT -> {
                    long sub = programSize(re.subs[0]);
                    if (re.max == -1) {
                        size = re.min == 0 ? 2 + sub : 1 + (long) re.min * sub;
                    } else {
                        size = (long) re.max * sub + (re.max - re.min);
                    }
                }
                case NO_MATCH, EMPTY_MATCH, CHAR_CLASS, ANY_CHAR_NOT_NL, ANY_CHAR, BEGIN_LINE, END_LINE, BEGIN_TEXT, END_TEXT,
                    WORD_BOUNDARY, NO_WORD_BOUNDARY -> size = 1;
                case LEFT_PAREN, VERTICAL_BAR -> throw new IllegalStateException("[" + re.op + "] is only on the parser's stack");
            }
            return Math.max(1, size);
        }
    }

    /** A compiled program: its instructions, read by index, and the one it starts at. */
    public static final class Program {
        public final int start;
        private final Instruction[] instructions;

        private Program(Prog prog) {
            this.start = prog.start;
            this.instructions = new Instruction[prog.numInst()];
            for (int pc = 0; pc < instructions.length; pc++) {
                instructions[pc] = new Instruction(prog.getInst(pc));
            }
        }

        public int numInst() {
            return instructions.length;
        }

        public Instruction getInst(int pc) {
            return instructions[pc];
        }
    }

    /** An instruction of a {@link Program}, with the fields and opcodes of RE2/J's own. */
    public static final class Instruction {
        public static final int ALT = Inst.ALT;
        public static final int ALT_MATCH = Inst.ALT_MATCH;
        public static final int CAPTURE = Inst.CAPTURE;
        public static final int EMPTY_WIDTH = Inst.EMPTY_WIDTH;
        public static final int FAIL = Inst.FAIL;
        public static final int MATCH = Inst.MATCH;
        public static final int NOP = Inst.NOP;
        public static final int RUNE = Inst.RUNE;
        public static final int RUNE1 = Inst.RUNE1;
        public static final int RUNE_ANY = Inst.RUNE_ANY;
        public static final int RUNE_ANY_NOT_NL = Inst.RUNE_ANY_NOT_NL;

        public final int op;
        public final int out;
        public final int arg;
        public final int[] runes;

        private Instruction(Inst inst) {
            this.op = inst.op;
            this.out = inst.out;
            this.arg = inst.arg;
            this.runes = inst.runes;
        }
    }
}
