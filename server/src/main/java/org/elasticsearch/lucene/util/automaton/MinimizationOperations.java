/*
 * @notice
 * Copyright (c) 2001-2009 Anders Moeller
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Sourced from: https://github.com/apache/lucene/blob/main/lucene/core/src/test/org/apache/lucene/util/automaton/MinimizationOperations.java
 */

package org.elasticsearch.lucene.util.automaton;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

import java.util.BitSet;
import java.util.LinkedList;

/**
 * Operations for minimizing automata.
 * <p>
 * Lucene 10 removed minimization, but Elasticsearch still requires it.
 * Minimization is critical in the security codebase to reduce the heap
 * usage of automata used for permission checks.
 * <p>
 * Copied of Lucene's AutomatonTestUtil
 */
public final class MinimizationOperations {

    private MinimizationOperations() {}

    /**
     * Minimizes (and determinizes if not already deterministic) the given automaton using Hopcroft's
     * algorithm.
     *
     * @param determinizeWorkLimit maximum effort to spend determinizing the automaton. Set higher to
     *     allow more complex queries and lower to prevent memory exhaustion. Use {@link
     *     Operations#DEFAULT_DETERMINIZE_WORK_LIMIT} as a decent default if you don't otherwise know
     *     what to specify.
     */
    public static Automaton minimize(Automaton a, int determinizeWorkLimit) {

        if (a.getNumStates() == 0 || (a.isAccept(0) == false && a.getNumTransitions(0) == 0)) {
            // Fastmatch for common case
            return new Automaton();
        }
        a = Operations.determinize(a, determinizeWorkLimit);
        // a.writeDot("adet");
        if (a.getNumTransitions(0) == 1) {
            Transition t = new Transition();
            a.getTransition(0, 0, t);
            if (t.dest == 0 && t.min == Character.MIN_CODE_POINT && t.max == Character.MAX_CODE_POINT) {
                // Accepts all strings
                return a;
            }
        }
        a = totalize(a);
        // a.writeDot("atot");

        // initialize data structures
        final int[] sigma = a.getStartPoints();
        final int sigmaLen = sigma.length, statesLen = a.getNumStates();

        final IntArrayList[][] reverse = new IntArrayList[statesLen][sigmaLen];
        final IntHashSet[] partition = new IntHashSet[statesLen];
        final IntArrayList[] splitblock = new IntArrayList[statesLen];
        final int[] block = new int[statesLen];
        final StateList[][] active = new StateList[statesLen][sigmaLen];
        final StateListNode[][] active2 = new StateListNode[statesLen][sigmaLen];
        final LinkedList<IntPair> pending = new LinkedList<>();
        final BitSet pending2 = new BitSet(sigmaLen * statesLen);
        final BitSet split = new BitSet(statesLen), refine = new BitSet(statesLen), refine2 = new BitSet(statesLen);
        for (int q = 0; q < statesLen; q++) {
            splitblock[q] = new IntArrayList();
            partition[q] = new IntHashSet();
            for (int x = 0; x < sigmaLen; x++) {
                active[q][x] = StateList.EMPTY;
            }
        }
        // find initial partition and reverse edges
        for (int q = 0; q < statesLen; q++) {
            // TODO moved the following into the loop because we cannot reset transition.transitionUpto (pkg private)
            Transition transition = new Transition();
            final int j = a.isAccept(q) ? 0 : 1;
            partition[j].add(q);
            block[q] = j;
            transition.source = q;
            // TODO we'd need to be able to access pkg private transition.transitionUpto if we want to optimize the following
            // transition.transitionUpto = -1;
            for (int x = 0; x < sigmaLen; x++) {
                final IntArrayList[] r = reverse[a.next(transition, sigma[x])];
                if (r[x] == null) {
                    r[x] = new IntArrayList();
                }
                r[x].add(q);
            }
        }
        // initialize active sets
        for (int j = 0; j <= 1; j++) {
            for (int x = 0; x < sigmaLen; x++) {
                for (IntCursor qCursor : partition[j]) {
                    int q = qCursor.value;
                    if (reverse[q][x] != null) {
                        StateList stateList = active[j][x];
                        if (stateList == StateList.EMPTY) {
                            stateList = new StateList();
                            active[j][x] = stateList;
                        }
                        active2[q][x] = stateList.add(q);
                    }
                }
            }
        }

        // initialize pending
        for (int x = 0; x < sigmaLen; x++) {
            final int j = (active[0][x].size <= active[1][x].size) ? 0 : 1;
            pending.add(new IntPair(j, x));
            pending2.set(x * statesLen + j);
        }

        // process pending until fixed point
        int k = 2;
        // System.out.println("start min");
        while (false == pending.isEmpty()) {
            // System.out.println(" cycle pending");
            final IntPair ip = pending.removeFirst();
            final int p = ip.n1;
            final int x = ip.n2;
            // System.out.println(" pop n1=" + ip.n1 + " n2=" + ip.n2);
            pending2.clear(x * statesLen + p);
            // find states that need to be split off their blocks
            for (StateListNode m = active[p][x].first; m != null; m = m.next) {
                final IntArrayList r = reverse[m.q][x];
                if (r != null) {
                    for (IntCursor iCursor : r) {
                        final int i = iCursor.value;
                        if (false == split.get(i)) {
                            split.set(i);
                            final int j = block[i];
                            splitblock[j].add(i);
                            if (false == refine2.get(j)) {
                                refine2.set(j);
                                refine.set(j);
                            }
                        }
                    }
                }
            }

            // refine blocks
            for (int j = refine.nextSetBit(0); j >= 0; j = refine.nextSetBit(j + 1)) {
                final IntArrayList sb = splitblock[j];
                if (sb.size() < partition[j].size()) {
                    final IntHashSet b1 = partition[j];
                    final IntHashSet b2 = partition[k];
                    for (IntCursor iCursor : sb) {
                        final int s = iCursor.value;
                        b1.remove(s);
                        b2.add(s);
                        block[s] = k;
                        for (int c = 0; c < sigmaLen; c++) {
                            final StateListNode sn = active2[s][c];
                            if (sn != null && sn.sl == active[j][c]) {
                                sn.remove();
                                StateList stateList = active[k][c];
                                if (stateList == StateList.EMPTY) {
                                    stateList = new StateList();
                                    active[k][c] = stateList;
                                }
                                active2[s][c] = stateList.add(s);
                            }
                        }
                    }
                    // update pending
                    for (int c = 0; c < sigmaLen; c++) {
                        final int aj = active[j][c].size, ak = active[k][c].size, ofs = c * statesLen;
                        if ((false == pending2.get(ofs + j)) && 0 < aj && aj <= ak) {
                            pending2.set(ofs + j);
                            pending.add(new IntPair(j, c));
                        } else {
                            pending2.set(ofs + k);
                            pending.add(new IntPair(k, c));
                        }
                    }
                    k++;
                }
                refine2.clear(j);
                for (IntCursor iCursor : sb) {
                    final int s = iCursor.value;
                    split.clear(s);
                }
                sb.clear();
            }
            refine.clear();
        }

        Automaton result = new Automaton();

        Transition t = new Transition();

        // System.out.println(" k=" + k);

        // make a new state for each equivalence class, set initial state
        int[] stateMap = new int[statesLen];
        int[] stateRep = new int[k];

        result.createState();

        // System.out.println("min: k=" + k);
        for (int n = 0; n < k; n++) {
            // System.out.println(" n=" + n);

            boolean isInitial = partition[n].contains(0);

            int newState;
            if (isInitial) {
                // System.out.println(" isInitial!");
                newState = 0;
            } else {
                newState = result.createState();
            }

            // System.out.println(" newState=" + newState);

            for (IntCursor qCursor : partition[n]) {
                int q = qCursor.value;
                stateMap[q] = newState;
                // System.out.println(" q=" + q + " isAccept?=" + a.isAccept(q));
                result.setAccept(newState, a.isAccept(q));
                stateRep[newState] = q; // select representative
            }
        }

        // build transitions and set acceptance
        for (int n = 0; n < k; n++) {
            int numTransitions = a.initTransition(stateRep[n], t);
            for (int i = 0; i < numTransitions; i++) {
                a.getNextTransition(t);
                // System.out.println(" add trans");
                result.addTransition(n, stateMap[t.dest], t.min, t.max);
            }
        }
        result.finishState();
        // System.out.println(result.getNumStates() + " states");

        return Operations.removeDeadStates(result);
    }

    record IntPair(int n1, int n2) {}

    static final class StateList {

        // Empty list that should never be mutated, used as a memory saving optimization instead of null
        // so we don't need to branch the read path in #minimize
        static final StateList EMPTY = new StateList();

        int size;

        StateListNode first, last;

        StateListNode add(int q) {
            assert this != EMPTY;
            return new StateListNode(q, this);
        }
    }

    static final class StateListNode {

        final int q;

        StateListNode next, prev;

        final StateList sl;

        StateListNode(int q, StateList sl) {
            this.q = q;
            this.sl = sl;
            if (sl.size++ == 0) sl.first = sl.last = this;
            else {
                sl.last.next = this;
                prev = sl.last;
                sl.last = this;
            }
        }

        void remove() {
            sl.size--;
            if (sl.first == this) sl.first = next;
            else prev.next = next;
            if (sl.last == this) sl.last = prev;
            else next.prev = prev;
        }
    }

    static Automaton totalize(Automaton a) {
        Automaton result = new Automaton();
        int numStates = a.getNumStates();
        for (int i = 0; i < numStates; i++) {
            result.createState();
            result.setAccept(i, a.isAccept(i));
        }

        int deadState = result.createState();
        result.addTransition(deadState, deadState, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);

        Transition t = new Transition();
        for (int i = 0; i < numStates; i++) {
            int maxi = Character.MIN_CODE_POINT;
            int count = a.initTransition(i, t);
            for (int j = 0; j < count; j++) {
                a.getNextTransition(t);
                result.addTransition(i, t.dest, t.min, t.max);
                if (t.min > maxi) {
                    result.addTransition(i, deadState, maxi, t.min - 1);
                }
                if (t.max + 1 > maxi) {
                    maxi = t.max + 1;
                }
            }

            if (maxi <= Character.MAX_CODE_POINT) {
                result.addTransition(i, deadState, maxi, Character.MAX_CODE_POINT);
            }
        }

        result.finishState();
        return result;
    }
}
