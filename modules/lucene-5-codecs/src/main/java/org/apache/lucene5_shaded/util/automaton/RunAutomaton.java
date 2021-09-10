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

import java.util.Arrays;

/**
 * Finite-state automaton with fast run operation.
 * 
 * @lucene.experimental
 */
public abstract class RunAutomaton {
  final Automaton automaton;
  final int maxInterval;
  final int size;
  final boolean[] accept;
  final int initial;
  final int[] transitions; // delta(state,c) = transitions[state*points.length +
                     // getCharClass(c)]
  final int[] points; // char interval start points
  final int[] classmap; // map from char number to class class
  
  /**
   * Returns a string representation of this automaton.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("initial state: ").append(initial).append("\n");
    for (int i = 0; i < size; i++) {
      b.append("state " + i);
      if (accept[i]) b.append(" [accept]:\n");
      else b.append(" [reject]:\n");
      for (int j = 0; j < points.length; j++) {
        int k = transitions[i * points.length + j];
        if (k != -1) {
          int min = points[j];
          int max;
          if (j + 1 < points.length) max = (points[j + 1] - 1);
          else max = maxInterval;
          b.append(" ");
          Automaton.appendCharString(min, b);
          if (min != max) {
            b.append("-");
            Automaton.appendCharString(max, b);
          }
          b.append(" -> ").append(k).append("\n");
        }
      }
    }
    return b.toString();
  }
  
  /**
   * Returns number of states in automaton.
   */
  public final int getSize() {
    return size;
  }
  
  /**
   * Returns acceptance status for given state.
   */
  public final boolean isAccept(int state) {
    return accept[state];
  }
  
  /**
   * Returns initial state.
   */
  public final int getInitialState() {
    return initial;
  }
  
  /**
   * Returns array of codepoint class interval start points. The array should
   * not be modified by the caller.
   */
  public final int[] getCharIntervals() {
    return points.clone();
  }
  
  /**
   * Gets character class of given codepoint
   */
  final int getCharClass(int c) {
    return Operations.findIndex(c, points);
  }

  /**
   * Constructs a new <code>RunAutomaton</code> from a deterministic
   * <code>Automaton</code>.
   * 
   * @param a an automaton
   */
  public RunAutomaton(Automaton a, int maxInterval, boolean tableize) {
    this(a, maxInterval, tableize, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Constructs a new <code>RunAutomaton</code> from a deterministic
   * <code>Automaton</code>.
   * 
   * @param a an automaton
   * @param maxDeterminizedStates maximum number of states that can be created
   *   while determinizing a
   */
  public RunAutomaton(Automaton a, int maxInterval, boolean tableize,
      int maxDeterminizedStates) {
    this.maxInterval = maxInterval;
    a = Operations.determinize(a, maxDeterminizedStates);
    this.automaton = a;
    points = a.getStartPoints();
    initial = 0;
    size = Math.max(1,a.getNumStates());
    accept = new boolean[size];
    transitions = new int[size * points.length];
    Arrays.fill(transitions, -1);
    for (int n=0;n<size;n++) {
      accept[n] = a.isAccept(n);
      for (int c = 0; c < points.length; c++) {
        int dest = a.step(n, points[c]);
        assert dest == -1 || dest < size;
        transitions[n * points.length + c] = dest;
      }
    }

    /*
     * Set alphabet table for optimal run performance.
     */
    if (tableize) {
      classmap = new int[maxInterval + 1];
      int i = 0;
      for (int j = 0; j <= maxInterval; j++) {
        if (i + 1 < points.length && j == points[i + 1]) {
          i++;
        }
        classmap[j] = i;
      }
    } else {
      classmap = null;
    }
  }
  
  /**
   * Returns the state obtained by reading the given char from the given state.
   * Returns -1 if not obtaining any such state. (If the original
   * <code>Automaton</code> had no dead states, -1 is returned here if and only
   * if a dead state is entered in an equivalent automaton with a total
   * transition function.)
   */
  public final int step(int state, int c) {
    if (classmap == null) {
      return transitions[state * points.length + getCharClass(c)];
    } else {
      return transitions[state * points.length + classmap[c]];
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + initial;
    result = prime * result + maxInterval;
    result = prime * result + points.length;
    result = prime * result + size;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    RunAutomaton other = (RunAutomaton) obj;
    if (initial != other.initial) return false;
    if (maxInterval != other.maxInterval) return false;
    if (size != other.size) return false;
    if (!Arrays.equals(points, other.points)) return false;
    if (!Arrays.equals(accept, other.accept)) return false;
    if (!Arrays.equals(transitions, other.transitions)) return false;
    return true;
  }
}
