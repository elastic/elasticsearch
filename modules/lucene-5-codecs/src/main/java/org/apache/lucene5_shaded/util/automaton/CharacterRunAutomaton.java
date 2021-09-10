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


/**
 * Automaton representation for matching char[].
 */
public class CharacterRunAutomaton extends RunAutomaton {
  /**
   * Construct with a default number of maxDeterminizedStates.
   */
  public CharacterRunAutomaton(Automaton a) {
    this(a, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Construct specifying maxDeterminizedStates.
   * @param a Automaton to match
   * @param maxDeterminizedStates maximum number of states that the automataon
   *   can have once determinized.  If more states are required to determinize
   *   it then a TooComplexToDeterminizeException is thrown.
   */ 
  public CharacterRunAutomaton(Automaton a, int maxDeterminizedStates) {
    super(a, Character.MAX_CODE_POINT, false, maxDeterminizedStates);
  }

  /**
   * Returns true if the given string is accepted by this automaton.
   */
  public boolean run(String s) {
    int p = initial;
    int l = s.length();
    for (int i = 0, cp = 0; i < l; i += Character.charCount(cp)) {
      p = step(p, cp = s.codePointAt(i));
      if (p == -1) return false;
    }
    return accept[p];
  }
  
  /**
   * Returns true if the given string is accepted by this automaton
   */
  public boolean run(char[] s, int offset, int length) {
    int p = initial;
    int l = offset + length;
    for (int i = offset, cp = 0; i < l; i += Character.charCount(cp)) {
      p = step(p, cp = Character.codePointAt(s, i, l));
      if (p == -1) return false;
    }
    return accept[p];
  }
}
