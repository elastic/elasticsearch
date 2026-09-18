/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.util.automaton;

import org.apache.lucene.internal.hppc.BitMixer;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.Transition;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a circuit-breaker-aware variant of {@link Operations#determinize(Automaton, int)}.
 * <p>
 * Lucene's {@code Operations.determinize} can allocate memory proportional to the powerset of
 * the NFA states, which grows exponentially for certain patterns (e.g. {@code .*a.*b.*c.*d.*}).
 * The standard effort-based work limit ({@code TooComplexToDeterminizeException}) bounds CPU
 * work but does not directly bound memory. This class periodically estimates the memory consumed
 * by the growing DFA and intermediate data structures, checking the circuit breaker so that
 * construction is aborted before an OOM can occur.
 * <p>
 * The helper classes below ({@code IntSet}, {@code FrozenIntSet}, {@code StateSet}, etc.) are
 * copied from Lucene's package-private internals because they cannot be accessed from outside
 * {@code org.apache.lucene.util.automaton}.
 *
 * @see MinimizationOperations
 */
public final class CircuitBreakingOperations {

    private CircuitBreakingOperations() {}

    /**
     * How often (in new-DFA-state increments) we re-estimate memory and check the breaker.
     * A value of 64 amortizes the per-check overhead while still catching runaway growth early.
     */
    private static final int CB_CHECK_INTERVAL = 64;

    /**
     * Approximate bytes consumed per new DFA state during subset construction. This accounts for:
     * <ul>
     *   <li>{@code HashMap.Node}: ~32 bytes (header + hash + key/value/next refs)</li>
     *   <li>{@code FrozenIntSet}: ~40 bytes (header + int[] ref + state + hashCode) plus the
     *       backing {@code int[]} whose size depends on the NFA subset size</li>
     *   <li>{@code Automaton.Builder} per-state storage: 8 bytes (2 ints in {@code states[]})</li>
     *   <li>Transitions (averaged): ~36 bytes (3 transitions per state × 12 bytes each)</li>
     * </ul>
     */
    private static final long ESTIMATED_BYTES_PER_STATE = 200L;

    /**
     * Live bytes per reachable product state: its worklist entry, its slot in the pair map and the state itself. Only live
     * memory is charged; the copies array growth leaves behind are the collector's.
     */
    private static final long PRODUCT_STATE_BYTES = 80L;
    /** Live bytes per transition: three ints plus growth headroom in the product, or one {@code Transition} object in a sorted input. */
    private static final long PRODUCT_TRANSITION_BYTES = 48L;
    /** Reserve in steps of this size rather than on every transition. */
    private static final long PRODUCT_CHARGE_STEP = 64 * 1024L;

    /**
     * Determinizes the given automaton, periodically checking the provided circuit breaker.
     * <p>
     * This is functionally equivalent to {@link Operations#determinize(Automaton, int)} but adds
     * memory accounting. Temporary memory reserved on the breaker during construction is released
     * before returning (or in the finally block on exception). The caller is responsible for
     * accounting the final automaton's {@code ramBytesUsed()} separately.
     *
     * @param a              the NFA to determinize
     * @param workLimit      maximum "effort" before throwing {@link TooComplexToDeterminizeException}
     * @param circuitBreaker the circuit breaker to check (must not be {@code null})
     * @param label          descriptive label for circuit breaker error messages
     * @return the determinized automaton
     * @throws TooComplexToDeterminizeException if effort exceeds {@code workLimit}
     * @throws org.elasticsearch.common.breaker.CircuitBreakingException if the breaker trips
     */
    public static Automaton determinize(Automaton a, int workLimit, CircuitBreaker circuitBreaker, String label) {
        assert circuitBreaker != null : "circuit breaker must not be null";

        if (a.isDeterministic()) {
            return a;
        }
        if (a.getNumStates() <= 1) {
            return a;
        }

        Automaton.Builder b = new Automaton.Builder();

        FrozenIntSet initialset = new FrozenIntSet(new int[] { 0 }, BitMixer.mix(0) + 1, 0);

        b.createState();

        ArrayDeque<FrozenIntSet> worklist = new ArrayDeque<>();
        Map<IntSet, Integer> newstate = new HashMap<>();

        worklist.add(initialset);

        b.setAccept(0, a.isAccept(0));
        newstate.put(initialset, 0);

        final PointTransitionSet points = new PointTransitionSet();

        final StateSet statesSet = new StateSet(5);

        Transition t = new Transition();

        long effortSpent = 0;

        // LUCENE-9981: approximate conversion from what used to be a limit on number of states,
        // to maximum "effort":
        long effortLimit = workLimit * (long) 10;

        int newStatesCreated = 0;
        long totalReserved = 0;

        try {
            while (worklist.size() > 0) {
                FrozenIntSet s = worklist.removeFirst();

                effortSpent += s.values.length;
                if (effortSpent >= effortLimit) {
                    throw new TooComplexToDeterminizeException(a, workLimit);
                }

                for (int i = 0; i < s.values.length; i++) {
                    final int s0 = s.values[i];
                    int numTransitions = a.getNumTransitions(s0);
                    a.initTransition(s0, t);
                    for (int j = 0; j < numTransitions; j++) {
                        a.getNextTransition(t);
                        points.add(t);
                    }
                }

                if (points.count == 0) {
                    continue;
                }

                points.sort();

                int lastPoint = -1;
                int accCount = 0;

                final int r = s.state;

                for (int i = 0; i < points.count; i++) {

                    final int point = points.points[i].point;

                    if (statesSet.size() > 0) {
                        assert lastPoint != -1;

                        Integer q = newstate.get(statesSet);
                        if (q == null) {
                            q = b.createState();
                            final FrozenIntSet p = statesSet.freeze(q);
                            worklist.add(p);
                            b.setAccept(q, accCount > 0);
                            newstate.put(p, q);

                            newStatesCreated++;
                            if (newStatesCreated % CB_CHECK_INTERVAL == 0) {
                                long estimatedNewBytes = CB_CHECK_INTERVAL * ESTIMATED_BYTES_PER_STATE;
                                circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedNewBytes, label);
                                totalReserved += estimatedNewBytes;
                            }
                        } else {
                            assert (accCount > 0 ? true : false) == b.isAccept(q)
                                : "accCount=" + accCount + " vs existing accept=" + b.isAccept(q) + " states=" + statesSet;
                        }

                        b.addTransition(r, q, lastPoint, point - 1);
                    }

                    int[] transitions = points.points[i].ends.transitions;
                    int limit = points.points[i].ends.next;
                    for (int j = 0; j < limit; j += 3) {
                        int dest = transitions[j];
                        statesSet.decr(dest);
                        accCount -= a.isAccept(dest) ? 1 : 0;
                    }
                    points.points[i].ends.next = 0;

                    transitions = points.points[i].starts.transitions;
                    limit = points.points[i].starts.next;
                    for (int j = 0; j < limit; j += 3) {
                        int dest = transitions[j];
                        statesSet.incr(dest);
                        accCount += a.isAccept(dest) ? 1 : 0;
                    }
                    lastPoint = point;
                    points.points[i].starts.next = 0;
                }
                points.reset();
                assert statesSet.size() == 0 : "size=" + statesSet.size();
            }
        } finally {
            if (totalReserved > 0) {
                circuitBreaker.addWithoutBreaking(-totalReserved, label);
            }
        }

        Automaton result = b.finish();
        assert result.isDeterministic();
        return result;
    }

    // ------------------------------------------------------------------
    // Package-private helper classes copied from Lucene (10.3.2) because
    // they are not accessible outside org.apache.lucene.util.automaton.
    // ------------------------------------------------------------------

    /**
     * {@link Operations#minus(Automaton, Automaton, int)} with the product construction charged to the breaker as it grows.
     * Lucene walks every reachable pair of states of the two automata and creates a state for each pair with the overlap
     * of their transitions, with no accounting of its own. How many pairs are reachable and how many ranges overlap is
     * only known once the walk is done, and a pair of many-range character classes costs orders of magnitude more per
     * state than two chains, so no estimate taken before the build is safe. Temporary memory is released before
     * returning; the caller accounts the result's {@code ramBytesUsed()}.
     *
     * @param excluded must already be deterministic
     */
    public static Automaton minus(Automaton a, Automaton excluded, CircuitBreaker circuitBreaker, String label) {
        assert excluded.isDeterministic() : "excluded must be determinized (and charged) by the caller";
        if (Operations.isEmpty(a) || a == excluded) {
            return Automata.makeEmpty();
        }
        if (Operations.isEmpty(excluded)) {
            return a;
        }
        // complement totalizes the exclude and copies it again to drop dead states; each copy is about its own size
        long complementReservation = 3 * excluded.ramBytesUsed();
        circuitBreaker.addEstimateBytesAndMaybeBreak(complementReservation, label);
        Automaton complement;
        try {
            complement = Operations.complement(excluded, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        } finally {
            circuitBreaker.addWithoutBreaking(-complementReservation, label);
        }
        return intersection(a, complement, circuitBreaker, label);
    }

    /** {@link Operations#intersection(Automaton, Automaton)}, charging each product state and transition as it is created. */
    static Automaton intersection(Automaton a1, Automaton a2, CircuitBreaker circuitBreaker, String label) {
        if (a1 == a2) {
            return a1;
        }
        if (a1.getNumStates() == 0) {
            return a1;
        }
        if (a2.getNumStates() == 0) {
            return a2;
        }
        long reserved = 0;
        long pending = (long) (a1.getNumTransitions() + a2.getNumTransitions()) * PRODUCT_TRANSITION_BYTES;
        try {
            circuitBreaker.addEstimateBytesAndMaybeBreak(pending, label);
            reserved += pending;
            pending = 0;
            Transition[][] transitions1 = a1.getSortedTransitions();
            Transition[][] transitions2 = a2.getSortedTransitions();
            Automaton c = new Automaton();
            c.createState();
            ArrayDeque<int[]> worklist = new ArrayDeque<>();
            LongIntHashMap newstates = new LongIntHashMap();
            worklist.add(new int[] { 0, 0, 0 });
            newstates.put(pairKey(0, 0), 0);
            while (worklist.isEmpty() == false) {
                int[] p = worklist.removeFirst();
                c.setAccept(p[0], a1.isAccept(p[1]) && a2.isAccept(p[2]));
                Transition[] t1 = transitions1[p[1]];
                Transition[] t2 = transitions2[p[2]];
                for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
                    while (b2 < t2.length && t2[b2].max < t1[n1].min) {
                        b2++;
                    }
                    for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++) {
                        if (t2[n2].max >= t1[n1].min) {
                            long key = pairKey(t1[n1].dest, t2[n2].dest);
                            int r;
                            if (newstates.containsKey(key)) {
                                r = newstates.get(key);
                            } else {
                                r = c.createState();
                                worklist.add(new int[] { r, t1[n1].dest, t2[n2].dest });
                                newstates.put(key, r);
                                pending += PRODUCT_STATE_BYTES;
                            }
                            c.addTransition(p[0], r, Math.max(t1[n1].min, t2[n2].min), Math.min(t1[n1].max, t2[n2].max));
                            pending += PRODUCT_TRANSITION_BYTES;
                            if (pending >= PRODUCT_CHARGE_STEP) {
                                circuitBreaker.addEstimateBytesAndMaybeBreak(pending, label);
                                reserved += pending;
                                pending = 0;
                            }
                        }
                    }
                }
            }
            c.finishState();
            // removeDeadStates copies the live part of c
            long copy = c.ramBytesUsed();
            circuitBreaker.addEstimateBytesAndMaybeBreak(copy, label);
            reserved += copy;
            return Operations.removeDeadStates(c);
        } finally {
            circuitBreaker.addWithoutBreaking(-reserved, label);
        }
    }

    private static long pairKey(int s1, int s2) {
        return ((long) s1 << 32) | (s2 & 0xffffffffL);
    }

    abstract static class IntSet {
        abstract int[] getArray();

        abstract int size();

        abstract long longHashCode();

        @Override
        public int hashCode() {
            return Long.hashCode(longHashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof IntSet) == false) return false;
            IntSet that = (IntSet) o;
            return longHashCode() == that.longHashCode() && Arrays.equals(getArray(), 0, size(), that.getArray(), 0, that.size());
        }
    }

    static final class FrozenIntSet extends IntSet {
        final int[] values;
        final int state;
        final long hashCode;

        FrozenIntSet(int[] values, long hashCode, int state) {
            this.values = values;
            this.state = state;
            this.hashCode = hashCode;
        }

        @Override
        int[] getArray() {
            return values;
        }

        @Override
        int size() {
            return values.length;
        }

        @Override
        long longHashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return Arrays.toString(values);
        }
    }

    static final class StateSet extends IntSet {
        private final IntIntHashMap inner;
        private long hashCode;
        private boolean hashUpdated = true;
        private boolean arrayUpdated = true;
        private int[] arrayCache = new int[0];

        StateSet(int capacity) {
            inner = new IntIntHashMap(capacity);
        }

        void incr(int state) {
            if (inner.addTo(state, 1) == 1) {
                keyChanged();
            }
        }

        void decr(int state) {
            assert inner.containsKey(state);
            int keyIndex = inner.indexOf(state);
            int count = inner.indexGet(keyIndex) - 1;
            if (count == 0) {
                inner.indexRemove(keyIndex);
                keyChanged();
            } else {
                inner.indexReplace(keyIndex, count);
            }
        }

        FrozenIntSet freeze(int state) {
            return new FrozenIntSet(getArray(), longHashCode(), state);
        }

        private void keyChanged() {
            hashUpdated = false;
            arrayUpdated = false;
        }

        @Override
        int[] getArray() {
            if (arrayUpdated) {
                return arrayCache;
            }
            arrayCache = new int[inner.size()];
            int i = 0;
            for (IntCursor key : inner.keys()) {
                arrayCache[i++] = key.value;
            }
            Arrays.sort(arrayCache);
            arrayUpdated = true;
            return arrayCache;
        }

        @Override
        int size() {
            return inner.size();
        }

        @Override
        long longHashCode() {
            if (hashUpdated) {
                return hashCode;
            }
            hashCode = inner.size();
            for (IntCursor key : inner.keys()) {
                hashCode += BitMixer.mix(key.value);
            }
            hashUpdated = true;
            return hashCode;
        }
    }

    static final class TransitionList {
        int[] transitions = new int[3];
        int next;

        void add(Transition t) {
            if (transitions.length < next + 3) {
                transitions = ArrayUtil.grow(transitions, next + 3);
            }
            transitions[next] = t.dest;
            transitions[next + 1] = t.min;
            transitions[next + 2] = t.max;
            next += 3;
        }
    }

    static final class PointTransitions implements Comparable<PointTransitions> {
        int point;
        final TransitionList ends = new TransitionList();
        final TransitionList starts = new TransitionList();

        @Override
        public int compareTo(PointTransitions other) {
            return point - other.point;
        }

        void reset(int point) {
            this.point = point;
            ends.next = 0;
            starts.next = 0;
        }

        @Override
        public boolean equals(Object other) {
            return ((PointTransitions) other).point == point;
        }

        @Override
        public int hashCode() {
            return point;
        }
    }

    static final class PointTransitionSet {
        int count;
        PointTransitions[] points = new PointTransitions[5];

        private static final int HASHMAP_CUTOVER = 30;
        private final IntObjectHashMap<PointTransitions> map = new IntObjectHashMap<>();
        private boolean useHash = false;

        private PointTransitions next(int point) {
            if (count == points.length) {
                final PointTransitions[] newArray = new PointTransitions[ArrayUtil.oversize(
                    1 + count,
                    RamUsageEstimator.NUM_BYTES_OBJECT_REF
                )];
                System.arraycopy(points, 0, newArray, 0, count);
                points = newArray;
            }
            PointTransitions points0 = points[count];
            if (points0 == null) {
                points0 = points[count] = new PointTransitions();
            }
            points0.reset(point);
            count++;
            return points0;
        }

        private PointTransitions find(int point) {
            if (useHash) {
                final Integer pi = point;
                PointTransitions p = map.get(pi);
                if (p == null) {
                    p = next(point);
                    map.put(pi, p);
                }
                return p;
            } else {
                for (int i = 0; i < count; i++) {
                    if (points[i].point == point) {
                        return points[i];
                    }
                }

                final PointTransitions p = next(point);
                if (count == HASHMAP_CUTOVER) {
                    assert map.size() == 0;
                    for (int i = 0; i < count; i++) {
                        map.put(points[i].point, points[i]);
                    }
                    useHash = true;
                }
                return p;
            }
        }

        void reset() {
            if (useHash) {
                map.clear();
                useHash = false;
            }
            count = 0;
        }

        void sort() {
            if (count > 1) ArrayUtil.timSort(points, 0, count);
        }

        void add(Transition t) {
            find(t.min).starts.add(t);
            find(1 + t.max).ends.add(t);
        }
    }
}
