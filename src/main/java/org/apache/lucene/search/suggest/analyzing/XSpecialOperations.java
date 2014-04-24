/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search.suggest.analyzing;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.Version;

// TODO Lucene 4.9: remove this once we upgrade; see
// LUCENE-5628

class XSpecialOperations {

  static {
    assert Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_47: "Remove this code once we upgrade to a Lucene version where LUCENE-5628 is fixed (likely 4.9)";
  }

  private static class PathNode {

    /** Which state the path node ends on, whose
     *  transitions we are enumerating. */
    public State state;

    /** Which transition we are on. */
    public int transition;

    /** Which label we are on, in the min-max range of the
     *  current Transition */
    public int label;

    public void resetState(State state) {
      assert state.numTransitions() != 0;
      this.state = state;
      transition = 0;
      label = state.transitionsArray[0].getMin();
    }

    /** Returns true if there is another transition. */
    public boolean nextTransition() {
      transition++;
      if (transition < state.numTransitions()) {
        label = state.transitionsArray[transition].getMin();
        return true;
      } else {
        return false;
      }
    }
  }

  private static PathNode getNode(PathNode[] nodes, int index) {
    assert index < nodes.length;
    if (nodes[index] == null) {
      nodes[index] = new PathNode();
    }
    return nodes[index];
  }

  /** Returns the set of accepted strings, up to at most
   *  <code>limit</code> strings. If more than <code>limit</code> 
   *  strings are accepted, the first limit strings found are returned. If <code>limit</code>&lt;0, then 
   *  the limit is infinite.  If the {@link Automaton} has
   *  cycles then this method might throw {@code
   *  IllegalArgumentException} but that is not guaranteed
   *  when the limit is not 0. */
  public static Set<IntsRef> getFiniteStrings(Automaton a, int limit) {
    Set<IntsRef> strings = new HashSet<>();

    if (a.getSingleton() != null) {
      // Easy case: automaton accepts only 1 string
      if (limit > 0) {
        strings.add(Util.toUTF32(a.getSingleton(), new IntsRef()));
      }
    } else {

      if (a.getInitialState().isAccept()) {
        // Special case the empty string, as usual:
        strings.add(new IntsRef());
      }

      // a.getNumberedStates();

      if (a.getInitialState().numTransitions() > 0 && (limit <= 0 || strings.size() < limit)) {

        // TODO: we could use state numbers here and just
        // alloc array, but asking for states array can be
        // costly (it's lazily computed):

        // Tracks which states are in the current path, for
        // cycle detection:
        Set<State> pathStates = new HashSet<>();

        // Stack to hold our current state in the
        // recursion/iteration:
        PathNode[] nodes = new PathNode[4];

        pathStates.add(a.getInitialState());
        PathNode root = getNode(nodes, 0);
        root.resetState(a.getInitialState());

        IntsRef string = new IntsRef(4);
        string.length = 1;

        while (true) {

          PathNode node = nodes[string.length-1];
          if (node.transition < node.state.numTransitions()) {
            // This node still has more transitions to
            // iterate
            Transition t = node.state.transitionsArray[node.transition];
            if (node.label <= t.getMax()) {
              // This transition still has more labels to
              // iterate
              string.ints[string.length-1] = node.label++;

              if (t.getDest().isAccept()) {
                // This transition leads to an accept state,
                // so we save the current string:
                strings.add(IntsRef.deepCopyOf(string));
                if (strings.size() == limit) {
                  break;
                }
              }

              if (t.getDest().numTransitions() != 0) {
                // The destination of this transition has
                // outgoing transitions, so we recurse:
                if (pathStates.contains(t.getDest())) {
                  throw new IllegalArgumentException("automaton has cycles");
                }
                if (nodes.length == string.length) {
                  PathNode[] newNodes = new PathNode[ArrayUtil.oversize(nodes.length+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                  System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
                  nodes = newNodes;
                }
                pathStates.add(t.getDest());
                getNode(nodes, string.length).resetState(t.getDest());
                string.length++;
                string.grow(string.length);
                continue;
              } else if (node.label <= t.getMax()) {
                // Cycle to next label for this transition
                continue;
              } else if (node.nextTransition()) {
                // Cycle to next transition leaving the
                // current state
                continue;
              }
            } else if (node.nextTransition()) {
              // We used up the min-max of the current
              // transition; move to the next transition
              continue;
            }
          }

          // No more transitions leaving this state,
          // pop/return back to previous state:
          assert pathStates.contains(node.state);
          pathStates.remove(node.state);
          string.length--;
          if (string.length == 0) {
            // Done
            break;
          }
        }
      }
    }

    return strings;
  }
}
