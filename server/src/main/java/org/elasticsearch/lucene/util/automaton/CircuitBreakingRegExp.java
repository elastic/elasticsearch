/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.util.automaton;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations.addSaturating;
import static org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations.buildBytes;
import static org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations.multiplySaturating;

/**
 * A regular expression whose automaton is built with each step charged to a circuit breaker and bounded by a work limit.
 * <p>
 * {@link RegExp#toAutomaton()} gives its caller no way to account for what it allocates, and what it allocates depends on
 * how Lucene links the pieces together, not on the parse tree alone: bounded repeats multiply, and concatenating pieces that
 * accept the empty string copies transitions quadratically. So this walks the parse tree itself and, for each node, calls
 * the Lucene operation {@code toAutomaton} would call, after reserving an upper bound on that operation's peak memory
 * computed from the operands already built. Intersection and complement, whose size is only known once built, go through
 * the charged implementations in {@link CircuitBreakingOperations}. Lucene's operations can also spend time far out of
 * proportion to what they build, so the walk counts their work too and gives up past {@link #DEFAULT_WORK_LIMIT}.
 * <p>
 * For example, {@code (a|b)*} is built bottom-up in three steps, each with its own reservation:
 * <pre>
 *   a, b      leaves: built directly (their size is bounded by the pattern text), then held on the breaker
 *   a|b       Operations.union, with unionCost's bound reserved while it runs; a and b are released after it
 *   (a|b)*    Operations.repeat, with starCost's bound reserved while it runs; the union is released after it
 * </pre>
 * The bounds are what make short patterns safe: {@code x?{1000}} is eight characters, but Lucene links its copies with
 * half a million transitions, and {@code concatenateCost} charges for them before any of them exist.
 * <p>
 * The walk is iterative, so the depth of the parse tree cannot overflow the stack. Lucene's parser still recurses on nested
 * groups, so the constructor can throw {@link StackOverflowError}.
 */
public final class CircuitBreakingRegExp {

    /**
     * Upper bound on the work of building one automaton. Scanning an entry costs one unit; building a state or a transition
     * costs {@link #WORK_PER_BUILT_STATE_OR_TRANSITION}, because Lucene sorts and trims what it builds, which takes far longer
     * per item than a scan; each optional copy of a bounded repeat costs {@link #WORK_PER_OPTIONAL_COPY} for Lucene's
     * bookkeeping around it. Ordinary patterns use at most a few million units, and patterns such as {@code .{0,10000}}
     * about a hundred million; the limit allows a fraction of a second of any kind of work.
     */
    public static final int DEFAULT_WORK_LIMIT = 250_000_000;

    /** Work units per state or transition an operation builds, against one unit per entry it scans. */
    static final long WORK_PER_BUILT_STATE_OR_TRANSITION = 64L;

    /** Work units per optional copy of a bounded repeat: the set of accept states and the iteration Lucene allocates for it. */
    static final long WORK_PER_OPTIONAL_COPY = 32L;

    /** Bytes per entry of the operand list and per-copy bookkeeping in Lucene's repeat operations. */
    private static final long REPEAT_BYTES_PER_COPY = 16L;
    private static final long RETAINED_BYTES_PER_STATE = 12L;
    private static final long RETAINED_BYTES_PER_TRANSITION = 16L;
    private static final long BUILDER_BYTES_PER_TRANSITION = 16L;

    private static final RegExp[] NO_OPERANDS = new RegExp[0];

    private final RegExp regExp;
    private final int matchFlags;

    /**
     * Parses {@code pattern} as {@link RegExp#RegExp(String, int, int)} does.
     *
     * @throws IllegalArgumentException if the pattern is malformed
     */
    public CircuitBreakingRegExp(String pattern, int syntaxFlags, int matchFlags) {
        this.regExp = new RegExp(pattern, syntaxFlags, matchFlags);
        this.matchFlags = matchFlags;
    }

    /**
     * Builds the automaton {@link RegExp#toAutomaton()} would, with every step reserved on {@code breaker} while it runs. As
     * with {@link CircuitBreakingOperations#determinize}, nothing is held on return: the caller accounts the result's
     * {@code ramBytesUsed()}.
     *
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException if a step would exceed the breaker's limit
     * @throws TooComplexToDeterminizeException if building the automaton would take more than {@link #DEFAULT_WORK_LIMIT}
     *         work, or a complement needs more than the determinize work limit
     */
    public Automaton toAutomaton(CircuitBreaker breaker, String label) {
        return toAutomaton(breaker, label, DEFAULT_WORK_LIMIT);
    }

    Automaton toAutomaton(CircuitBreaker breaker, String label, int workLimit) {
        Walk walk = new Walk(breaker, label, workLimit);
        try {
            return walk.run();
        } finally {
            // on failure, the operands of every node still on the walk's stack are still held
            walk.releaseHeld();
        }
    }

    /** A parse-tree node and the automata built so far for its operands. */
    private static final class Node {
        final RegExp regExp;
        final RegExp[] operands;
        final Automaton[] built;
        final long[] held;
        int next;

        Node(RegExp regExp) {
            this.regExp = regExp;
            this.operands = operands(regExp);
            this.built = new Automaton[operands.length];
            this.held = new long[operands.length];
        }
    }

    /** The operands {@link RegExp#toAutomaton()} builds before combining them at {@code re}, in the order it builds them. */
    private static RegExp[] operands(RegExp re) {
        return switch (re.kind) {
            case REGEXP_UNION, REGEXP_CONCATENATION -> leaves(re);
            case REGEXP_INTERSECTION -> new RegExp[] { re.exp1, re.exp2 };
            case REGEXP_OPTIONAL, REGEXP_REPEAT, REGEXP_REPEAT_MIN, REGEXP_REPEAT_MINMAX, REGEXP_COMPLEMENT, REGEXP_DEPRECATED_COMPLEMENT ->
                new RegExp[] { re.exp1 };
            case REGEXP_CHAR, REGEXP_CHAR_RANGE, REGEXP_CHAR_CLASS, REGEXP_ANYCHAR, REGEXP_EMPTY, REGEXP_STRING, REGEXP_ANYSTRING,
                REGEXP_AUTOMATON, REGEXP_INTERVAL -> NO_OPERANDS;
        };
    }

    /**
     * The maximal same-kind subtree's leaves, left to right, as {@code RegExp.findLeaves} collects them: Lucene unions and
     * concatenates a list, so a union of unions is one list. For {@code a*|b|c?}, parsed as {@code UNION(UNION(a*, b), c?)},
     * the leaves are {@code [a*, b, c?]}; each of them is built as its own node.
     */
    private static RegExp[] leaves(RegExp re) {
        List<RegExp> leaves = new ArrayList<>();
        ArrayDeque<RegExp> pending = new ArrayDeque<>();
        pending.push(re.exp2);
        pending.push(re.exp1);
        while (pending.isEmpty() == false) {
            RegExp e = pending.pop();
            if (e.kind == re.kind) {
                pending.push(e.exp2);
                pending.push(e.exp1);
            } else {
                leaves.add(e);
            }
        }
        return leaves.toArray(RegExp[]::new);
    }

    /** One build: the breaker charges it holds and the work it has spent. */
    private final class Walk {
        private final CircuitBreaker breaker;
        private final String label;
        private final int workLimit;
        private long held;
        private long work;

        Walk(CircuitBreaker breaker, String label, int workLimit) {
            this.breaker = breaker;
            this.label = label;
            this.workLimit = workLimit;
        }

        /**
         * Builds the automaton bottom-up: a node is built once all its operands are, and each operand stays held on the
         * breaker until that build returns. The root's result is returned unheld.
         */
        Automaton run() {
            ArrayDeque<Node> stack = new ArrayDeque<>();
            stack.push(new Node(regExp));
            while (true) {
                Node node = stack.peek();
                if (node.next < node.operands.length) {
                    stack.push(new Node(node.operands[node.next]));
                    continue;
                }
                stack.pop();
                Automaton result = build(node);
                for (long bytes : node.held) {
                    release(bytes);
                }
                Node parent = stack.peek();
                if (parent == null) {
                    return result;
                }
                parent.built[parent.next] = result;
                parent.held[parent.next] = hold(result);
                parent.next++;
            }
        }

        private Automaton build(Node node) {
            RegExp re = node.regExp;
            Automaton[] in = node.built;
            return switch (re.kind) {
                case REGEXP_UNION -> guarded(unionCost(in), () -> Operations.union(Arrays.asList(in)));
                case REGEXP_CONCATENATION -> guarded(concatenateCost(in), () -> Operations.concatenate(Arrays.asList(in)));
                case REGEXP_INTERSECTION -> intersection(in[0], in[1]);
                case REGEXP_OPTIONAL -> guarded(optionalCost(Shape.of(in[0])), () -> Operations.optional(in[0]));
                case REGEXP_REPEAT -> guarded(starCost(Shape.of(in[0])), () -> Operations.repeat(in[0]));
                case REGEXP_REPEAT_MIN -> guarded(repeatCost(Shape.of(in[0]), re.min), () -> Operations.repeat(in[0], re.min));
                case REGEXP_REPEAT_MINMAX -> guarded(
                    repeatCost(Shape.of(in[0]), re.min, re.max),
                    () -> Operations.repeat(in[0], re.min, re.max)
                );
                // Lucene complements a negated character class with no work limit, and any other complement with the default
                case REGEXP_COMPLEMENT -> CircuitBreakingOperations.complement(in[0], Integer.MAX_VALUE, breaker, label);
                case REGEXP_DEPRECATED_COMPLEMENT -> CircuitBreakingOperations.complement(
                    in[0],
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    breaker,
                    label
                );
                case REGEXP_CHAR -> caseInsensitive() ? Automata.makeCaseInsensitiveChar(re.c) : Automata.makeChar(re.c);
                case REGEXP_CHAR_RANGE -> Automata.makeCharRange(re.from[0], re.to[0]);
                case REGEXP_CHAR_CLASS -> Automata.makeCharClass(re.from, re.to);
                case REGEXP_ANYCHAR -> Automata.makeAnyChar();
                case REGEXP_EMPTY -> Automata.makeEmpty();
                case REGEXP_STRING -> caseInsensitive() ? Automata.makeCaseInsensitiveString(re.s) : Automata.makeString(re.s);
                case REGEXP_ANYSTRING -> Automata.makeAnyString();
                case REGEXP_AUTOMATON -> throw new IllegalArgumentException("'" + re.s + "' not found");
                case REGEXP_INTERVAL -> Automata.makeDecimalInterval(re.min, re.max, re.digits);
            };
        }

        /** Runs {@code build} with its peak memory reserved, after checking that its work fits in what is left. */
        private Automaton guarded(Cost cost, Supplier<Automaton> build) {
            long reserved = CircuitBreakingOperations.reserve(breaker, cost.bytes(), label);
            try {
                work = addSaturating(work, cost.work());
                if (work > workLimit) {
                    throw tooComplex();
                }
                return build.get();
            } finally {
                breaker.addWithoutBreaking(-reserved, label);
            }
        }

        /**
         * The product of two automata, charged as it grows like any other and, since its size is only known once built,
         * refused as soon as it holds more states and transitions than the work left allows.
         */
        private Automaton intersection(Automaton a1, Automaton a2) {
            long itemsLeft = Math.max(0, workLimit - work) / WORK_PER_BUILT_STATE_OR_TRANSITION;
            Automaton product;
            try {
                product = CircuitBreakingOperations.intersection(a1, a2, breaker, label, itemsLeft);
            } catch (TooComplexToDeterminizeException e) {
                throw tooComplex();
            }
            long built = (long) product.getNumStates() + product.getNumTransitions();
            work = addSaturating(work, multiplySaturating(built, WORK_PER_BUILT_STATE_OR_TRANSITION));
            return product;
        }

        /** Refers to no operand, so the exception keeps nothing large alive once the breaker has released it. */
        private TooComplexToDeterminizeException tooComplex() {
            return new TooComplexToDeterminizeException(regExp, new TooComplexToDeterminizeException(Automata.makeEmpty(), workLimit));
        }

        private long hold(Automaton a) {
            long bytes = CircuitBreakingOperations.reserve(breaker, a.ramBytesUsed(), label);
            held += bytes;
            return bytes;
        }

        private void release(long bytes) {
            breaker.addWithoutBreaking(-bytes, label);
            held -= bytes;
        }

        void releaseHeld() {
            if (held > 0) {
                breaker.addWithoutBreaking(-held, label);
                held = 0;
            }
        }
    }

    @SuppressWarnings("deprecation") // RegExp.toAutomaton honours ASCII_CASE_INSENSITIVE as well as CASE_INSENSITIVE
    private boolean caseInsensitive() {
        return (matchFlags & (RegExp.ASCII_CASE_INSENSITIVE | RegExp.CASE_INSENSITIVE)) != 0;
    }

    /**
     * What the bounds read from an operand that is already built: its states, its transitions, its accept states, the
     * transitions leaving its initial state, and whether its initial state accepts, that is whether it matches the empty
     * string. For {@code x?}: 2 states, 1 transition, 2 accept states, 1 initial transition, nullable.
     */
    record Shape(long states, long transitions, long accepts, long initial, boolean nullable) {
        static Shape of(Automaton a) {
            if (a.getNumStates() == 0) {
                return new Shape(0, 0, 0, 0, false);
            }
            return new Shape(
                a.getNumStates(),
                a.getNumTransitions(),
                a.getAcceptStates().cardinality(),
                a.getNumTransitions(0),
                a.isAccept(0)
            );
        }
    }

    /**
     * An upper bound on one Lucene operation:
     * <ul>
     *   <li>{@code states} and {@code transitions}: what it builds before trimming dead states, which is the most it holds;</li>
     *   <li>{@code bytes}: its peak live memory, the build together with whatever else it holds at the same time, such as its
     *       operand list or the result of an earlier stage;</li>
     *   <li>{@code work}: its time, in the units of {@link #DEFAULT_WORK_LIMIT}.</li>
     * </ul>
     * For example, concatenating 1000 copies of {@code x?} builds at most 2000 states and 1,000,000 transitions, so it
     * reserves {@code buildBytes(2000, 1_000_000)} plus 16 bytes for each entry of its operand list.
     */
    record Cost(long states, long transitions, long bytes, long work) {
        static final Cost NONE = new Cost(0, 0, 0, 0);

        /** One stage that builds the output, holding {@code extraBytes} besides, with {@code extraWork} on top of its size. */
        static Cost of(long states, long transitions, long extraBytes, long extraWork) {
            long built = addSaturating(states, transitions);
            return new Cost(
                states,
                transitions,
                addSaturating(buildBytes(states, transitions), extraBytes),
                addSaturating(multiplySaturating(built, WORK_PER_BUILT_STATE_OR_TRANSITION), extraWork)
            );
        }
    }

    /**
     * {@link Operations#concatenate(List)}. Lucene copies every piece, then gives each accept state of a piece the initial
     * transitions of the next piece, and keeps going through the pieces after that for as long as they match the empty
     * string. So a piece whose followers are all nullable links to every one of them, which is where a concatenation's
     * transitions grow with the square of its length.
     * <p>
     * The bound walks the pieces from the end, carrying in {@code chained} what each accept state of the current piece will
     * receive. For four pieces {@code x?}, each with 1 transition, 2 accept states and 1 initial transition, and nullable:
     * <pre>
     *   piece   chains into   links per accept state
     *   1       2, 3, 4       3
     *   2       3, 4          2
     *   3       4             1
     *   4       nothing       0
     * </pre>
     * that is 2 accept states times (3 + 2 + 1), 12 links, plus the pieces' own 4 transitions: 16.
     */
    static Cost concatenateCost(Automaton[] pieces) {
        Shape[] shapes = new Shape[pieces.length];
        for (int i = 0; i < pieces.length; i++) {
            shapes[i] = Shape.of(pieces[i]);
        }
        return concatenateCost(shapes, pieces.length);
    }

    /** {@link #concatenateCost(Automaton[])} over the pieces' shapes, with {@code listSize} entries in Lucene's operand list. */
    static Cost concatenateCost(Shape[] shapes, long listSize) {
        long states = 0;
        long transitions = 0;
        long chained = 0;
        for (int i = shapes.length - 1; i >= 0; i--) {
            Shape piece = shapes[i];
            states = addSaturating(states, piece.states());
            transitions = addSaturating(transitions, addSaturating(piece.transitions(), multiplySaturating(piece.accepts(), chained)));
            chained = addSaturating(piece.initial(), piece.nullable() ? chained : 0);
        }
        return Cost.of(states, transitions, multiplySaturating(listSize, REPEAT_BYTES_PER_COPY), listSize);
    }

    /**
     * {@link Operations#union(java.util.Collection)}: a new initial state, a copy of every piece, and the new initial state
     * given each piece's initial transitions. For {@code cat|dog}, two pieces of 4 states, 3 transitions and 1 initial
     * transition: at most 9 states and 8 transitions.
     */
    static Cost unionCost(Automaton[] pieces) {
        long states = 1;
        long transitions = 0;
        for (Automaton piece : pieces) {
            Shape shape = Shape.of(piece);
            states = addSaturating(states, shape.states());
            transitions = addSaturating(transitions, addSaturating(shape.transitions(), shape.initial()));
        }
        return Cost.of(states, transitions, multiplySaturating(pieces.length, REPEAT_BYTES_PER_COPY), pieces.length);
    }

    /**
     * {@link Operations#optional(Automaton)}: nothing if the operand already matches the empty string, since Lucene then
     * returns it unchanged; otherwise at most a new initial state given the operand's initial transitions. For {@code (ab)?}:
     * at most 4 states and 3 transitions.
     */
    static Cost optionalCost(Shape a) {
        if (a.nullable()) {
            return Cost.NONE;
        }
        return Cost.of(addSaturating(a.states(), 1), addSaturating(a.transitions(), a.initial()), 0, 0);
    }

    /**
     * {@link Operations#repeat(Automaton)}: a new accepting initial state, a copy of the operand, and the operand's initial
     * transitions copied onto the new initial state and onto every accept state, which is what lets it loop. Lucene builds it
     * through {@code Automaton.Builder}, so the builder's bytes are added. For {@code (ab)*}, whose operand has 3 states,
     * 2 transitions, 1 accept state and 1 initial transition: at most 4 states and 2 + 1 + 1 = 4 transitions.
     */
    static Cost starCost(Shape a) {
        if (a.states() == 0) {
            return Cost.NONE;
        }
        Shape star = starShape(a);
        return Cost.of(star.states(), star.transitions(), builderBytes(star.transitions()), 0);
    }

    private static Shape starShape(Shape a) {
        long transitions = addSaturating(addSaturating(a.transitions(), a.initial()), multiplySaturating(a.accepts(), a.initial()));
        return new Shape(addSaturating(a.states(), 1), transitions, addSaturating(a.accepts(), 1), a.initial(), true);
    }

    /**
     * {@link Operations#repeat(Automaton, int)}, that is {@code a{min,}}: Lucene first builds the operand's star, then
     * concatenates {@code min} copies of the operand followed by that star. The two stages run one after the other and the
     * star stays alive through the second, so the peak is the larger of the star's build and the concatenation plus the
     * star. For {@code a{3,}}: the star {@code a*}, then {@code a a a a*}.
     */
    static Cost repeatCost(Shape a, int min) {
        if (min == 0) {
            return starCost(a);
        }
        Shape star = starShape(a);
        Cost starStage = starCost(a);
        Cost copies = copiesCost(a, min, star);
        long concatenateStage = addSaturating(retainedBytes(star.states(), star.transitions()), copies.bytes());
        return new Cost(
            copies.states(),
            copies.transitions(),
            Math.max(starStage.bytes(), concatenateStage),
            addSaturating(starStage.work(), copies.work())
        );
    }

    /**
     * {@link Operations#repeat(Automaton, int, int)}, that is {@code a{min,max}}. Lucene builds it in two stages, one after
     * the other:
     * <ol>
     *   <li>{@code b}: just the empty string when {@code min} is 0, a copy of the operand when it is 1, otherwise {@code min}
     *       copies concatenated;</li>
     *   <li>a builder holding {@code b}, then {@code max - min} more copies, each linked by copying its initial transitions
     *       onto every accept state of the one before it, the first onto {@code b}'s accept states.</li>
     * </ol>
     * {@code b} stays alive through the second stage, so the peak is the larger of {@code b}'s build and {@code b} plus the
     * builder and its output.
     * <p>
     * Each link calls {@code Automaton.Builder.addEpsilon}, which scans every transition in the builder, so the work grows with
     * the square of the optional copies even when the output is small. For {@code x{0,20000}} the output is about 40,000
     * states and 40,000 transitions, but copy j is linked by scanning the roughly 2j transitions before it, about 400 million
     * entries in all, which the work limit refuses.
     */
    static Cost repeatCost(Shape a, int min, int max) {
        if (min > max) {
            return Cost.NONE;
        }
        long bStates;
        long bTransitions;
        long bAccepts;
        long bBytes;
        long bWork;
        if (min == 0) {
            bStates = 1;
            bTransitions = 0;
            bAccepts = 1;
            bBytes = 0;
            bWork = WORK_PER_BUILT_STATE_OR_TRANSITION;
        } else if (min == 1) {
            bStates = a.states();
            bTransitions = a.transitions();
            bAccepts = a.accepts();
            bBytes = retainedBytes(bStates, bTransitions);
            bWork = multiplySaturating(addSaturating(bStates, bTransitions), WORK_PER_BUILT_STATE_OR_TRANSITION);
        } else {
            Cost b = copiesCost(a, min, null);
            bStates = b.states();
            bTransitions = b.transitions();
            bAccepts = a.nullable() ? multiplySaturating(min, a.accepts()) : a.accepts();
            bBytes = b.bytes();
            bWork = b.work();
        }
        long optionalCopies = (long) max - min;
        long states = addSaturating(bStates, multiplySaturating(optionalCopies, a.states()));
        long transitions = addSaturating(bTransitions, multiplySaturating(optionalCopies, a.transitions()));
        long scanWork = 0;
        if (optionalCopies > 0) {
            long linked = addSaturating(
                multiplySaturating(bAccepts, a.initial()),
                multiplySaturating(optionalCopies - 1, multiplySaturating(a.accepts(), a.initial()))
            );
            transitions = addSaturating(transitions, linked);
            long linksPerCopy = Math.max(bAccepts, a.accepts());
            long growthPerCopy = addSaturating(a.transitions(), multiplySaturating(linksPerCopy, a.initial()));
            long triangle = multiplySaturating(optionalCopies, addSaturating(optionalCopies, 1)) / 2;
            scanWork = multiplySaturating(
                linksPerCopy,
                addSaturating(multiplySaturating(optionalCopies, bTransitions), multiplySaturating(growthPerCopy, triangle))
            );
        }
        long builderStage = addSaturating(
            addSaturating(retainedBytes(bStates, bTransitions), buildBytes(states, transitions)),
            addSaturating(builderBytes(transitions), multiplySaturating(optionalCopies, REPEAT_BYTES_PER_COPY))
        );
        long work = addSaturating(
            addSaturating(multiplySaturating(addSaturating(states, transitions), WORK_PER_BUILT_STATE_OR_TRANSITION), bWork),
            addSaturating(scanWork, multiplySaturating(optionalCopies, addSaturating(a.accepts(), WORK_PER_OPTIONAL_COPY)))
        );
        return new Cost(states, transitions, Math.max(bBytes, builderStage), work);
    }

    /** Retained bytes of a finished automaton: two ints per state and three per transition, with growth headroom. */
    private static long retainedBytes(long states, long transitions) {
        return addSaturating(
            multiplySaturating(states, RETAINED_BYTES_PER_STATE),
            multiplySaturating(transitions, RETAINED_BYTES_PER_TRANSITION)
        );
    }

    /** Bytes an {@code Automaton.Builder} holds for {@code transitions}: four ints each. */
    private static long builderBytes(long transitions) {
        return multiplySaturating(transitions, BUILDER_BYTES_PER_TRANSITION);
    }

    /**
     * {@code count} identical copies of {@code a} concatenated, followed by {@code tail} (the operand's star, which matches the
     * empty string) when it is not null: exactly {@link #concatenateCost(Shape[], long)} over those pieces, in closed form so
     * that {@code a{1000000}} costs nothing to evaluate.
     * <p>
     * If the operand matches the empty string, copy k chains into every copy after it and then into the tail, so its accept
     * states each receive {@code (count - k) * initial} transitions plus the tail's initial ones; summed over the copies that
     * is {@code count * (count - 1) / 2 * initial}, plus {@code count} times the tail's. Otherwise each copy links only to
     * the next one, and the last to the tail: {@code (count - 1) * initial}, plus the tail's. For {@code x?{4}} that is
     * 4 * 3 / 2 = 6 links for each of the 2 accept states, 12 in all; for {@code [ab]{4}}, 3.
     */
    static Cost copiesCost(Shape a, int count, Shape tail) {
        long n = count;
        long states = multiplySaturating(n, a.states());
        long transitions = multiplySaturating(n, a.transitions());
        long tailInitial = 0;
        if (tail != null) {
            states = addSaturating(states, tail.states());
            transitions = addSaturating(transitions, tail.transitions());
            tailInitial = tail.initial();
        }
        // what the accept states of copy k receive: the initial transitions of the pieces chained after it
        long linked;
        if (a.nullable()) {
            // copy k (1-based) chains through the n - k copies after it, then into the tail, which also accepts empty
            long pairs = multiplySaturating(n, n - 1) / 2;
            linked = addSaturating(multiplySaturating(pairs, a.initial()), multiplySaturating(n, tailInitial));
        } else {
            // each copy but the last links to the next copy only; the last links to the tail
            linked = addSaturating(multiplySaturating(n - 1, a.initial()), tailInitial);
        }
        transitions = addSaturating(transitions, multiplySaturating(a.accepts(), linked));
        long listSize = tail == null ? n : n + 1;
        return Cost.of(states, transitions, multiplySaturating(listSize, REPEAT_BYTES_PER_COPY), listSize);
    }
}
