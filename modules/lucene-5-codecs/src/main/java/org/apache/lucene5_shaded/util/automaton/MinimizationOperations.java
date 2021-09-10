/*
 * dk.brics.automaton
 * 
 * Copyright (c) 2001-2009 Anders Moeller
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.lucene5_shaded.util.automaton;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Operations for minimizing automata.
 * 
 * @lucene.experimental
 */
final public class MinimizationOperations {
  
  private MinimizationOperations() {}

  /**
   * Minimizes (and determinizes if not already deterministic) the given
   * automaton using Hopcroft's algorighm.
   * @param maxDeterminizedStates maximum number of states determinizing the
   *  automaton can result in.  Set higher to allow more complex queries and
   *  lower to prevent memory exhaustion.
   */
  public static Automaton minimize(Automaton a, int maxDeterminizedStates) {
    if (a.getNumStates() == 0 || (a.isAccept(0) == false && a.getNumTransitions(0) == 0)) {
      // Fastmatch for common case
      return new Automaton();
    }
    a = Operations.determinize(a, maxDeterminizedStates);
    //a.writeDot("adet");
    if (a.getNumTransitions(0) == 1) {
      Transition t = new Transition();
      a.getTransition(0, 0, t);
      if (t.dest == 0 && t.min == Character.MIN_CODE_POINT
          && t.max == Character.MAX_CODE_POINT) {
        // Accepts all strings
        return a;
      }
    }
    a = Operations.totalize(a);
    //a.writeDot("atot");

    // initialize data structures
    final int[] sigma = a.getStartPoints();
    final int sigmaLen = sigma.length, statesLen = a.getNumStates();

    @SuppressWarnings({"rawtypes","unchecked"}) final ArrayList<Integer>[][] reverse =
      (ArrayList<Integer>[][]) new ArrayList[statesLen][sigmaLen];
    @SuppressWarnings({"rawtypes","unchecked"}) final HashSet<Integer>[] partition =
      (HashSet<Integer>[]) new HashSet[statesLen];
    @SuppressWarnings({"rawtypes","unchecked"}) final ArrayList<Integer>[] splitblock =
      (ArrayList<Integer>[]) new ArrayList[statesLen];
    final int[] block = new int[statesLen];
    final StateList[][] active = new StateList[statesLen][sigmaLen];
    final StateListNode[][] active2 = new StateListNode[statesLen][sigmaLen];
    final LinkedList<IntPair> pending = new LinkedList<>();
    final BitSet pending2 = new BitSet(sigmaLen*statesLen);
    final BitSet split = new BitSet(statesLen), 
      refine = new BitSet(statesLen), refine2 = new BitSet(statesLen);
    for (int q = 0; q < statesLen; q++) {
      splitblock[q] = new ArrayList<>();
      partition[q] = new HashSet<>();
      for (int x = 0; x < sigmaLen; x++) {
        active[q][x] = new StateList();
      }
    }
    // find initial partition and reverse edges
    for (int q = 0; q < statesLen; q++) {
      final int j = a.isAccept(q) ? 0 : 1;
      partition[j].add(q);
      block[q] = j;
      for (int x = 0; x < sigmaLen; x++) {
        final ArrayList<Integer>[] r = reverse[a.step(q, sigma[x])];
        if (r[x] == null) {
          r[x] = new ArrayList<>();
        }
        r[x].add(q);
      }
    }
    // initialize active sets
    for (int j = 0; j <= 1; j++) {
      for (int x = 0; x < sigmaLen; x++) {
        for (int q : partition[j]) {
          if (reverse[q][x] != null) {
            active2[q][x] = active[j][x].add(q);
          }
        }
      }
    }

    // initialize pending
    for (int x = 0; x < sigmaLen; x++) {
      final int j = (active[0][x].size <= active[1][x].size) ? 0 : 1;
      pending.add(new IntPair(j, x));
      pending2.set(x*statesLen + j);
    }

    // process pending until fixed point
    int k = 2;
    //System.out.println("start min");
    while (!pending.isEmpty()) {
      //System.out.println("  cycle pending");
      final IntPair ip = pending.removeFirst();
      final int p = ip.n1;
      final int x = ip.n2;
      //System.out.println("    pop n1=" + ip.n1 + " n2=" + ip.n2);
      pending2.clear(x*statesLen + p);
      // find states that need to be split off their blocks
      for (StateListNode m = active[p][x].first; m != null; m = m.next) {
        final ArrayList<Integer> r = reverse[m.q][x];
        if (r != null) {
          for (int i : r) {
            if (!split.get(i)) {
              split.set(i);
              final int j = block[i];
              splitblock[j].add(i);
              if (!refine2.get(j)) {
                refine2.set(j);
                refine.set(j);
              }
            }
          }
        }
      }

      // refine blocks
      for (int j = refine.nextSetBit(0); j >= 0; j = refine.nextSetBit(j+1)) {
        final ArrayList<Integer> sb = splitblock[j];
        if (sb.size() < partition[j].size()) {
          final HashSet<Integer> b1 = partition[j];
          final HashSet<Integer> b2 = partition[k];
          for (int s : sb) {
            b1.remove(s);
            b2.add(s);
            block[s] = k;
            for (int c = 0; c < sigmaLen; c++) {
              final StateListNode sn = active2[s][c];
              if (sn != null && sn.sl == active[j][c]) {
                sn.remove();
                active2[s][c] = active[k][c].add(s);
              }
            }
          }
          // update pending
          for (int c = 0; c < sigmaLen; c++) {
            final int aj = active[j][c].size,
              ak = active[k][c].size,
              ofs = c*statesLen;
            if (!pending2.get(ofs + j) && 0 < aj && aj <= ak) {
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
        for (int s : sb) {
          split.clear(s);
        }
        sb.clear();
      }
      refine.clear();
    }

    Automaton result = new Automaton();

    Transition t = new Transition();

    //System.out.println("  k=" + k);

    // make a new state for each equivalence class, set initial state
    int[] stateMap = new int[statesLen];
    int[] stateRep = new int[k];

    result.createState();

    //System.out.println("min: k=" + k);
    for (int n = 0; n < k; n++) {
      //System.out.println("    n=" + n);

      boolean isInitial = false;
      for (int q : partition[n]) {
        if (q == 0) {
          isInitial = true;
          //System.out.println("    isInitial!");
          break;
        }
      }

      int newState;
      if (isInitial) {
        newState = 0;
      } else {
        newState = result.createState();
      }

      //System.out.println("  newState=" + newState);

      for (int q : partition[n]) {
        stateMap[q] = newState;
        //System.out.println("      q=" + q + " isAccept?=" + a.isAccept(q));
        result.setAccept(newState, a.isAccept(q));
        stateRep[newState] = q;   // select representative
      }
    }

    // build transitions and set acceptance
    for (int n = 0; n < k; n++) {
      int numTransitions = a.initTransition(stateRep[n], t);
      for(int i=0;i<numTransitions;i++) {
        a.getNextTransition(t);
        //System.out.println("  add trans");
        result.addTransition(n, stateMap[t.dest], t.min, t.max);
      }
    }
    result.finishState();
    //System.out.println(result.getNumStates() + " states");

    return Operations.removeDeadStates(result);
  }
  
  static final class IntPair {
    
    final int n1, n2;
    
    IntPair(int n1, int n2) {
      this.n1 = n1;
      this.n2 = n2;
    }
  }
  
  static final class StateList {
    
    int size;
    
    StateListNode first, last;
    
    StateListNode add(int q) {
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
}
