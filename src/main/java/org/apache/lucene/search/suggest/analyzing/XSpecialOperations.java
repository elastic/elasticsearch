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

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.Version;

class XSpecialOperations {

  // TODO Lucene 4.9: remove this once we upgrade; see
  // LUCENE-5628

  static {
    assert Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_48: "Remove this code once we upgrade to Lucene 4.9 where LUCENE-5628 is fixed";
  }

  private static class PathNode {

    /** Which state the path node ends on, whose
     *  transitions we are enumerating. */
    public State state;

    /** Which state the current transition leads to. */
    public State to;

    /** Which transition we are on. */
    public int transition;

    /** Which label we are on, in the min-max range of the
     *  current Transition */
    public int label;

    public void resetState(State state) {
      assert state.numTransitions() != 0;
      this.state = state;
      transition = 0;
      Transition t = state.transitionsArray[transition];
      label = t.getMin();
      to = t.getDest();
    }

    /** Returns next label of current transition, or
     *  advances to next transition and returns its first
     *  label, if current one is exhausted.  If there are
     *  no more transitions, returns -1. */
    public int nextLabel() {
      if (label > state.transitionsArray[transition].getMax()) {
        // We've exhaused the current transition's labels;
        // move to next transitions:
        transition++;
        if (transition >= state.numTransitions()) {
          // We're done iterating transitions leaving this state
          return -1;
        }
        Transition t = state.transitionsArray[transition];
        label = t.getMin();
        to = t.getDest();
      }
      return label++;
    }
  }

  private static PathNode getNode(PathNode[] nodes, int index) {
    assert index < nodes.length;
    if (nodes[index] == null) {
      nodes[index] = new PathNode();
    }
    return nodes[index];
  }

  // TODO: this is a dangerous method ... Automaton could be
  // huge ... and it's better in general for caller to
  // enumerate & process in a single walk:

  /** Returns the set of accepted strings, up to at most
   *  <code>limit</code> strings. If more than <code>limit</code> 
   *  strings are accepted, the first limit strings found are returned. If <code>limit</code> == -1, then 
   *  the limit is infinite.  If the {@link Automaton} has
   *  cycles then this method might throw {@code
   *  IllegalArgumentException} but that is not guaranteed
   *  when the limit is set. */
  public static Set<IntsRef> getFiniteStrings(Automaton a, int limit) {
    Set<IntsRef> results = new HashSet<>();

    if (limit == -1 || limit > 0) {
      // OK
    } else {
      throw new IllegalArgumentException("limit must be -1 (which means no limit), or > 0; got: " + limit);
    }

    if (a.getSingleton() != null) {
      // Easy case: automaton accepts only 1 string
      results.add(Util.toUTF32(a.getSingleton(), new IntsRef()));
    } else {

      if (a.getInitialState().isAccept()) {
        // Special case the empty string, as usual:
        results.add(new IntsRef());
      }

      if (a.getInitialState().numTransitions() > 0 && (limit == -1 || results.size() < limit)) {

        // TODO: we could use state numbers here and just
        // alloc array, but asking for states array can be
        // costly (it's lazily computed):

        // Tracks which states are in the current path, for
        // cycle detection:
        Set<State> pathStates = Collections.newSetFromMap(new IdentityHashMap<State,Boolean>());

        // Stack to hold our current state in the
        // recursion/iteration:
        PathNode[] nodes = new PathNode[4];

        pathStates.add(a.getInitialState());
        PathNode root = getNode(nodes, 0);
        root.resetState(a.getInitialState());

        IntsRef string = new IntsRef(1);
        string.length = 1;

        while (string.length > 0) {

          PathNode node = nodes[string.length-1];

          // Get next label leaving the current node:
          int label = node.nextLabel();

          if (label != -1) {
            string.ints[string.length-1] = label;

            if (node.to.isAccept()) {
              // This transition leads to an accept state,
              // so we save the current string:
              results.add(IntsRef.deepCopyOf(string));
              if (results.size() == limit) {
                break;
              }
            }

            if (node.to.numTransitions() != 0) {
              // Now recurse: the destination of this transition has
              // outgoing transitions:
              if (pathStates.contains(node.to)) {
                throw new IllegalArgumentException("automaton has cycles");
              }
              pathStates.add(node.to);

              // Push node onto stack:
              if (nodes.length == string.length) {
                PathNode[] newNodes = new PathNode[ArrayUtil.oversize(nodes.length+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
                nodes = newNodes;
              }
              getNode(nodes, string.length).resetState(node.to);
              string.length++;
              string.grow(string.length);
            }
          } else {
            // No more transitions leaving this state,
            // pop/return back to previous state:
            assert pathStates.contains(node.state);
            pathStates.remove(node.state);
            string.length--;
          }
        }
      }
    }

    return results;
  }
}
