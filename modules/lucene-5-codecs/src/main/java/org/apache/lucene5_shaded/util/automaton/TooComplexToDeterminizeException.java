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
 * This exception is thrown when determinizing an automaton would result in one
 * has too many states.
 */
public class TooComplexToDeterminizeException extends RuntimeException {
  private transient final Automaton automaton;
  private transient final RegExp regExp;
  private transient final int maxDeterminizedStates;

  /** Use this constructor when the RegExp failed to convert to an automaton. */
  public TooComplexToDeterminizeException(RegExp regExp, TooComplexToDeterminizeException cause) {
    super("Determinizing " + regExp.getOriginalString() + " would result in more than " +
      cause.maxDeterminizedStates + " states.", cause);
    this.regExp = regExp;
    this.automaton = cause.automaton;
    this.maxDeterminizedStates = cause.maxDeterminizedStates;
  }

  /** Use this constructor when the automaton failed to determinize. */
  public TooComplexToDeterminizeException(Automaton automaton, int maxDeterminizedStates) {
    super("Determinizing automaton with " + automaton.getNumStates() + " states and " + automaton.getNumTransitions() + " transitions would result in more than " + maxDeterminizedStates + " states.");
    this.automaton = automaton;
    this.regExp = null;
    this.maxDeterminizedStates = maxDeterminizedStates;
  }

  /** Returns the automaton that caused this exception, if any. */
  public Automaton getAutomaton() {
    return automaton;
  }

  /**
   * Return the RegExp that caused this exception if any.
   */
  public RegExp getRegExp() {
    return regExp;
  }

  /** Get the maximum number of allowed determinized states. */
  public int getMaxDeterminizedStates() {
    return maxDeterminizedStates;
  }
}
