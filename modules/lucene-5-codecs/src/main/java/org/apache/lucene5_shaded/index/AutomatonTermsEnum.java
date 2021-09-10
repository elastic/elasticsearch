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
package org.apache.lucene5_shaded.index;


import java.io.IOException;

import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.IntsRefBuilder;
import org.apache.lucene5_shaded.util.StringHelper;
import org.apache.lucene5_shaded.util.automaton.Automaton;
import org.apache.lucene5_shaded.util.automaton.ByteRunAutomaton;
import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;
import org.apache.lucene5_shaded.util.automaton.Transition;

/**
 * A FilteredTermsEnum that enumerates terms based upon what is accepted by a
 * DFA.
 * <p>
 * The algorithm is such:
 * <ol>
 *   <li>As long as matches are successful, keep reading sequentially.
 *   <li>When a match fails, skip to the next string in lexicographic order that
 * does not enter a reject state.
 * </ol>
 * <p>
 * The algorithm does not attempt to actually skip to the next string that is
 * completely accepted. This is not possible when the language accepted by the
 * FSM is not finite (i.e. * operator).
 * </p>
 * @lucene.internal
 */
public class AutomatonTermsEnum extends FilteredTermsEnum {
  // a tableized array-based form of the DFA
  private final ByteRunAutomaton runAutomaton;
  // common suffix of the automaton
  private final BytesRef commonSuffixRef;
  // true if the automaton accepts a finite language
  private final boolean finite;
  // array of sorted transitions for each state, indexed by state number
  private final Automaton automaton;
  // for path tracking: each long records gen when we last
  // visited the state; we use gens to avoid having to clear
  private final long[] visited;
  private long curGen;
  // the reference used for seeking forwards through the term dictionary
  private final BytesRefBuilder seekBytesRef = new BytesRefBuilder(); 
  // true if we are enumerating an infinite portion of the DFA.
  // in this case it is faster to drive the query based on the terms dictionary.
  // when this is true, linearUpperBound indicate the end of range
  // of terms where we should simply do sequential reads instead.
  private boolean linear = false;
  private final BytesRef linearUpperBound = new BytesRef(10);

  /**
   * Construct an enumerator based upon an automaton, enumerating the specified
   * field, working on a supplied TermsEnum
   *
   * @lucene.experimental 
   * @param compiled CompiledAutomaton
   */
  public AutomatonTermsEnum(TermsEnum tenum, CompiledAutomaton compiled) {
    super(tenum);
    this.finite = compiled.finite;
    this.runAutomaton = compiled.runAutomaton;
    assert this.runAutomaton != null;
    this.commonSuffixRef = compiled.commonSuffixRef;
    this.automaton = compiled.automaton;

    // used for path tracking, where each bit is a numbered state.
    visited = new long[runAutomaton.getSize()];
  }
  
  /**
   * Returns true if the term matches the automaton. Also stashes away the term
   * to assist with smart enumeration.
   */
  @Override
  protected AcceptStatus accept(final BytesRef term) {
    if (commonSuffixRef == null || StringHelper.endsWith(term, commonSuffixRef)) {
      if (runAutomaton.run(term.bytes, term.offset, term.length))
        return linear ? AcceptStatus.YES : AcceptStatus.YES_AND_SEEK;
      else
        return (linear && term.compareTo(linearUpperBound) < 0) ? 
            AcceptStatus.NO : AcceptStatus.NO_AND_SEEK;
    } else {
      return (linear && term.compareTo(linearUpperBound) < 0) ? 
          AcceptStatus.NO : AcceptStatus.NO_AND_SEEK;
    }
  }
  
  @Override
  protected BytesRef nextSeekTerm(final BytesRef term) throws IOException {
    //System.out.println("ATE.nextSeekTerm term=" + term);
    if (term == null) {
      assert seekBytesRef.length() == 0;
      // return the empty term, as it's valid
      if (runAutomaton.isAccept(runAutomaton.getInitialState())) {   
        return seekBytesRef.get();
      }
    } else {
      seekBytesRef.copyBytes(term);
    }

    // seek to the next possible string;
    if (nextString()) {
      return seekBytesRef.get();  // reposition
    } else {
      return null;          // no more possible strings can match
    }
  }

  private Transition transition = new Transition();

  /**
   * Sets the enum to operate in linear fashion, as we have found
   * a looping transition at position: we set an upper bound and 
   * act like a TermRangeQuery for this portion of the term space.
   */
  private void setLinear(int position) {
    assert linear == false;
    
    int state = runAutomaton.getInitialState();
    assert state == 0;
    int maxInterval = 0xff;
    //System.out.println("setLinear pos=" + position + " seekbytesRef=" + seekBytesRef);
    for (int i = 0; i < position; i++) {
      state = runAutomaton.step(state, seekBytesRef.byteAt(i) & 0xff);
      assert state >= 0: "state=" + state;
    }
    final int numTransitions = automaton.getNumTransitions(state);
    automaton.initTransition(state, transition);
    for (int i = 0; i < numTransitions; i++) {
      automaton.getNextTransition(transition);
      if (transition.min <= (seekBytesRef.byteAt(position) & 0xff) && 
          (seekBytesRef.byteAt(position) & 0xff) <= transition.max) {
        maxInterval = transition.max;
        break;
      }
    }
    // 0xff terms don't get the optimization... not worth the trouble.
    if (maxInterval != 0xff)
      maxInterval++;
    int length = position + 1; /* position + maxTransition */
    if (linearUpperBound.bytes.length < length)
      linearUpperBound.bytes = new byte[length];
    System.arraycopy(seekBytesRef.bytes(), 0, linearUpperBound.bytes, 0, position);
    linearUpperBound.bytes[position] = (byte) maxInterval;
    linearUpperBound.length = length;
    
    linear = true;
  }

  private final IntsRefBuilder savedStates = new IntsRefBuilder();
  
  /**
   * Increments the byte buffer to the next String in binary order after s that will not put
   * the machine into a reject state. If such a string does not exist, returns
   * false.
   * 
   * The correctness of this method depends upon the automaton being deterministic,
   * and having no transitions to dead states.
   * 
   * @return true if more possible solutions exist for the DFA
   */
  private boolean nextString() {
    int state;
    int pos = 0;
    savedStates.grow(seekBytesRef.length()+1);
    savedStates.setIntAt(0, runAutomaton.getInitialState());
    
    while (true) {
      curGen++;
      linear = false;
      // walk the automaton until a character is rejected.
      for (state = savedStates.intAt(pos); pos < seekBytesRef.length(); pos++) {
        visited[state] = curGen;
        int nextState = runAutomaton.step(state, seekBytesRef.byteAt(pos) & 0xff);
        if (nextState == -1)
          break;
        savedStates.setIntAt(pos+1, nextState);
        // we found a loop, record it for faster enumeration
        if (!finite && !linear && visited[nextState] == curGen) {
          setLinear(pos);
        }
        state = nextState;
      }

      // take the useful portion, and the last non-reject state, and attempt to
      // append characters that will match.
      if (nextString(state, pos)) {
        return true;
      } else { /* no more solutions exist from this useful portion, backtrack */
        if ((pos = backtrack(pos)) < 0) /* no more solutions at all */
          return false;
        final int newState = runAutomaton.step(savedStates.intAt(pos), seekBytesRef.byteAt(pos) & 0xff);
        if (newState >= 0 && runAutomaton.isAccept(newState))
          /* String is good to go as-is */
          return true;
        /* else advance further */
        // TODO: paranoia? if we backtrack thru an infinite DFA, the loop detection is important!
        // for now, restart from scratch for all infinite DFAs 
        if (!finite) pos = 0;
      }
    }
  }
  
  /**
   * Returns the next String in lexicographic order that will not put
   * the machine into a reject state. 
   * 
   * This method traverses the DFA from the given position in the String,
   * starting at the given state.
   * 
   * If this cannot satisfy the machine, returns false. This method will
   * walk the minimal path, in lexicographic order, as long as possible.
   * 
   * If this method returns false, then there might still be more solutions,
   * it is necessary to backtrack to find out.
   * 
   * @param state current non-reject state
   * @param position useful portion of the string
   * @return true if more possible solutions exist for the DFA from this
   *         position
   */
  private boolean nextString(int state, int position) {
    /* 
     * the next lexicographic character must be greater than the existing
     * character, if it exists.
     */
    int c = 0;
    if (position < seekBytesRef.length()) {
      c = seekBytesRef.byteAt(position) & 0xff;
      // if the next byte is 0xff and is not part of the useful portion,
      // then by definition it puts us in a reject state, and therefore this
      // path is dead. there cannot be any higher transitions. backtrack.
      if (c++ == 0xff)
        return false;
    }

    seekBytesRef.setLength(position);
    visited[state] = curGen;

    final int numTransitions = automaton.getNumTransitions(state);
    automaton.initTransition(state, transition);
    // find the minimal path (lexicographic order) that is >= c
    
    for (int i = 0; i < numTransitions; i++) {
      automaton.getNextTransition(transition);
      if (transition.max >= c) {
        int nextChar = Math.max(c, transition.min);
        // append either the next sequential char, or the minimum transition
        seekBytesRef.grow(seekBytesRef.length() + 1);
        seekBytesRef.append((byte) nextChar);
        state = transition.dest;
        /* 
         * as long as is possible, continue down the minimal path in
         * lexicographic order. if a loop or accept state is encountered, stop.
         */
        while (visited[state] != curGen && !runAutomaton.isAccept(state)) {
          visited[state] = curGen;
          /* 
           * Note: we work with a DFA with no transitions to dead states.
           * so the below is ok, if it is not an accept state,
           * then there MUST be at least one transition.
           */
          automaton.initTransition(state, transition);
          automaton.getNextTransition(transition);
          state = transition.dest;
          
          // append the minimum transition
          seekBytesRef.grow(seekBytesRef.length() + 1);
          seekBytesRef.append((byte) transition.min);
          
          // we found a loop, record it for faster enumeration
          if (!finite && !linear && visited[state] == curGen) {
            setLinear(seekBytesRef.length()-1);
          }
        }
        return true;
      }
    }
    return false;
  }
  
  /**
   * Attempts to backtrack thru the string after encountering a dead end
   * at some given position. Returns false if no more possible strings 
   * can match.
   * 
   * @param position current position in the input String
   * @return {@code position >= 0} if more possible solutions exist for the DFA
   */
  private int backtrack(int position) {
    while (position-- > 0) {
      int nextChar = seekBytesRef.byteAt(position) & 0xff;
      // if a character is 0xff it's a dead-end too,
      // because there is no higher character in binary sort order.
      if (nextChar++ != 0xff) {
        seekBytesRef.setByteAt(position, (byte) nextChar);
        seekBytesRef.setLength(position+1);
        return position;
      }
    }
    return -1; /* all solutions exhausted */
  }
}
