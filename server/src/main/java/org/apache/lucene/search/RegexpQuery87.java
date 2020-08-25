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
package org.apache.lucene.search;


import org.apache.lucene.index.Term;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.Operations;

/**
 * Copy of Lucene's RegExpQuery class coming in 8.7 with case
 * insensitive search option
 * @deprecated 
 */
@Deprecated 
public class RegexpQuery87 extends AutomatonQuery {
  /**
   * A provider that provides no named automata
   */
  private static AutomatonProvider defaultProvider = new AutomatonProvider() {
    @Override
    public Automaton getAutomaton(String name) {
      return null;
    }
  };
  
  /**
   * Constructs a query for terms matching <code>term</code>.
   * <p>
   * By default, all regular expression features are enabled.
   * </p>
   * 
   * @param term regular expression.
   */
  public RegexpQuery87(Term term) {
    this(term, RegExp87.ALL);
  }
  
  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param flags optional RegExp features from {@link RegExp87}
   */
  public RegexpQuery87(Term term, int flags) {
    this(term, flags, defaultProvider,
      Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param flags optional RegExp syntax features from {@link RegExp87}
   * @param maxDeterminizedStates maximum number of states that compiling the
   *  automaton for the regexp can result in.  Set higher to allow more complex
   *  queries and lower to prevent memory exhaustion.
   */
  public RegexpQuery87(Term term, int flags, int maxDeterminizedStates) {
    this(term, flags, defaultProvider, maxDeterminizedStates);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param syntax_flags optional RegExp syntax features from {@link RegExp87}
   *  automaton for the regexp can result in.  Set higher to allow more complex
   *  queries and lower to prevent memory exhaustion.
   * @param match_flags boolean 'or' of match behavior options such as case insensitivity
   * @param maxDeterminizedStates maximum number of states that compiling the
   */
  public RegexpQuery87(Term term, int syntax_flags, int match_flags, int maxDeterminizedStates) {
    this(term, syntax_flags, match_flags, defaultProvider, maxDeterminizedStates);
  }
  
  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param syntax_flags optional RegExp features from {@link RegExp87}
   * @param provider custom AutomatonProvider for named automata
   * @param maxDeterminizedStates maximum number of states that compiling the
   *  automaton for the regexp can result in.  Set higher to allow more complex
   *  queries and lower to prevent memory exhaustion.
   */
  public RegexpQuery87(Term term, int syntax_flags, AutomatonProvider provider,
      int maxDeterminizedStates) {
    this(term, syntax_flags, 0, provider, maxDeterminizedStates);
  }
  
  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param syntax_flags optional RegExp features from {@link RegExp87}
   * @param match_flags boolean 'or' of match behavior options such as case insensitivity
   * @param provider custom AutomatonProvider for named automata
   * @param maxDeterminizedStates maximum number of states that compiling the
   *  automaton for the regexp can result in.  Set higher to allow more complex
   *  queries and lower to prevent memory exhaustion.
   */
  public RegexpQuery87(Term term, int syntax_flags, int match_flags, AutomatonProvider provider,
      int maxDeterminizedStates) {
    super(term,
          new RegExp87(term.text(), syntax_flags, match_flags).toAutomaton(
                       provider, maxDeterminizedStates), maxDeterminizedStates);
  }

  /** Returns the regexp of this query wrapped in a Term. */
  public Term getRegexp() {
    return term;
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append('/');
    buffer.append(term.text());
    buffer.append('/');
    return buffer.toString();
  }
}
