/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.util.automaton;


import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.IntsRef;
import org.apache.lucene5_shaded.util.IntsRefBuilder;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

import java.util.BitSet;

/**
 * Iterates all accepted strings.
 *
 * <p>If the {@link Automaton} has cycles then this iterator may throw an {@code
 * IllegalArgumentException}, but this is not guaranteed!
 *
 * <p>Be aware that the iteration order is implementation dependent
 * and may change across releases.
 *
 * <p>If the automaton is not determinized then it's possible this iterator
 * will return duplicates.
 *
 * @lucene.experimental
 */
public class FiniteStringsIterator {
  /**
   * Empty string.
   */
  private static final IntsRef EMPTY = new IntsRef();

  /**
   * Automaton to create finite string from.
   */
  private final Automaton a;

  /**
   * Tracks which states are in the current path, for cycle detection.
   */
  private final BitSet pathStates;

  /**
   * Builder for current finite string.
   */
  private final IntsRefBuilder string;

  /**
   * Stack to hold our current state in the recursion/iteration.
   */
  private PathNode[] nodes;

  /**
   * Emit empty string?.
   */
  private boolean emitEmptyString;

  /**
   * Constructor.
   *
   * @param a Automaton to create finite string from.
   */
  public FiniteStringsIterator(Automaton a) {
    this.a = a;
    this.nodes = new PathNode[16];
    for (int i = 0, end = nodes.length; i < end; i++) {
      nodes[i] = new PathNode();
    }
    this.string = new IntsRefBuilder();
    this.pathStates = new BitSet(a.getNumStates());
    this.string.setLength(0);
    this.emitEmptyString = a.isAccept(0);

    // Start iteration with node 0.
    if (a.getNumTransitions(0) > 0) {
      pathStates.set(0);
      nodes[0].resetState(a, 0);
      string.append(0);
    }
  }

  /**
   * Generate next finite string.
   * The return value is just valid until the next call of this method!
   *
   * @return Finite string or null, if no more finite strings are available.
   */
  public IntsRef next() {
    // Special case the empty string, as usual:
    if (emitEmptyString) {
      emitEmptyString = false;
      return EMPTY;
    }

    for (int depth = string.length(); depth > 0;) {
      PathNode node = nodes[depth-1];

      // Get next label leaving the current node:
      int label = node.nextLabel(a);
      if (label != -1) {
        string.setIntAt(depth - 1, label);

        int to = node.to;
        if (a.getNumTransitions(to) != 0) {
          // Now recurse: the destination of this transition has outgoing transitions:
          if (pathStates.get(to)) {
            throw new IllegalArgumentException("automaton has cycles");
          }
          pathStates.set(to);

          // Push node onto stack:
          growStack(depth);
          nodes[depth].resetState(a, to);
          depth++;
          string.setLength(depth);
          string.grow(depth);
        } else if (a.isAccept(to)) {
          // This transition leads to an accept state, so we save the current string:
          return string.get();
        }
      } else {
        // No more transitions leaving this state, pop/return back to previous state:
        int state = node.state;
        assert pathStates.get(state);
        pathStates.clear(state);
        depth--;
        string.setLength(depth);

        if (a.isAccept(state)) {
          // This transition leads to an accept state, so we save the current string:
          return string.get();
        }
      }
    }

    // Finished iteration.
    return null;
  }

  /**
   * Grow path stack, if required.
   */
  private void growStack(int depth) {
    if (nodes.length == depth) {
      PathNode[] newNodes = new PathNode[ArrayUtil.oversize(nodes.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
      for (int i = depth, end = newNodes.length; i < end; i++) {
        newNodes[i] = new PathNode();
      }
      nodes = newNodes;
    }
  }

  /**
   * Nodes for path stack.
   */
  private static class PathNode {

    /** Which state the path node ends on, whose
     *  transitions we are enumerating. */
    public int state;

    /** Which state the current transition leads to. */
    public int to;

    /** Which transition we are on. */
    public int transition;

    /** Which label we are on, in the min-max range of the
     *  current Transition */
    public int label;

    private final Transition t = new Transition();

    public void resetState(Automaton a, int state) {
      assert a.getNumTransitions(state) != 0;
      this.state = state;
      transition = 0;
      a.getTransition(state, 0, t);
      label = t.min;
      to = t.dest;
    }

    /** Returns next label of current transition, or
     *  advances to next transition and returns its first
     *  label, if current one is exhausted.  If there are
     *  no more transitions, returns -1. */
    public int nextLabel(Automaton a) {
      if (label > t.max) {
        // We've exhaused the current transition's labels;
        // move to next transitions:
        transition++;
        if (transition >= a.getNumTransitions(state)) {
          // We're done iterating transitions leaving this state
          label = -1;
          return -1;
        }
        a.getTransition(state, transition, t);
        label = t.min;
        to = t.dest;
      }
      return label++;
    }
  }
}
