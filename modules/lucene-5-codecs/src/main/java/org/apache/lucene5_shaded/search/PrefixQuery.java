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
package org.apache.lucene5_shaded.search;


import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.automaton.Automaton;

/** A Query that matches documents containing terms with a specified prefix. A PrefixQuery
 * is built by QueryParser for input like <code>app*</code>.
 *
 * <p>This query uses the {@link
 * MultiTermQuery#CONSTANT_SCORE_REWRITE}
 * rewrite method. */

public class PrefixQuery extends AutomatonQuery {

  /** Constructs a query for terms starting with <code>prefix</code>. */
  public PrefixQuery(Term prefix) {
    // It's OK to pass unlimited maxDeterminizedStates: the automaton is born small and determinized:
    super(prefix, toAutomaton(prefix.bytes()), Integer.MAX_VALUE, true);
    if (prefix == null) {
      throw new NullPointerException("prefix cannot be null");
    }
  }

  /** Build an automaton accepting all terms with the specified prefix. */
  public static Automaton toAutomaton(BytesRef prefix) {
    Automaton automaton = new Automaton();
    int lastState = automaton.createState();
    for(int i=0;i<prefix.length;i++) {
      int state = automaton.createState();
      automaton.addTransition(lastState, state, prefix.bytes[prefix.offset+i]&0xff);
      lastState = state;
    }
    automaton.setAccept(lastState, true);
    automaton.addTransition(lastState, lastState, 0, 255);
    automaton.finishState();
    assert automaton.isDeterministic();
    return automaton;
  }

  /** Returns the prefix of this query. */
  public Term getPrefix() {
    return term;
  }
  
  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!getField().equals(field)) {
      buffer.append(getField());
      buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append('*');
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + term.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    // super.equals() ensures we are the same class
    PrefixQuery other = (PrefixQuery) obj;
    if (!term.equals(other.term)) {
      return false;
    }
    return true;
  }
}
