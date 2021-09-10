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
import org.apache.lucene5_shaded.util.ToStringUtils;
import org.apache.lucene5_shaded.util.automaton.Automaton;
import org.apache.lucene5_shaded.util.automaton.AutomatonProvider;
import org.apache.lucene5_shaded.util.automaton.Operations;
import org.apache.lucene5_shaded.util.automaton.RegExp;

/**
 * A fast regular expression query based on the
 * {@link org.apache.lucene5_shaded.util.automaton} package.
 * <ul>
 * <li>Comparisons are <a
 * href="http://tusker.org/regex/regex_benchmark.html">fast</a>
 * <li>The term dictionary is enumerated in an intelligent way, to avoid
 * comparisons. See {@link AutomatonQuery} for more details.
 * </ul>
 * <p>
 * The supported syntax is documented in the {@link RegExp} class.
 * Note this might be different than other regular expression implementations.
 * For some alternatives with different syntax, look under the sandbox.
 * </p>
 * <p>
 * Note this query can be slow, as it needs to iterate over many terms. In order
 * to prevent extremely slow RegexpQueries, a Regexp term should not start with
 * the expression <code>.*</code>
 * 
 * @see RegExp
 * @lucene.experimental
 */
public class RegexpQuery extends AutomatonQuery {
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
  public RegexpQuery(Term term) {
    this(term, RegExp.ALL);
  }
  
  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param flags optional RegExp features from {@link RegExp}
   */
  public RegexpQuery(Term term, int flags) {
    this(term, flags, defaultProvider,
      Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param flags optional RegExp features from {@link RegExp}
   * @param maxDeterminizedStates maximum number of states that compiling the
   *  automaton for the regexp can result in.  Set higher to allow more complex
   *  queries and lower to prevent memory exhaustion.
   */
  public RegexpQuery(Term term, int flags, int maxDeterminizedStates) {
    this(term, flags, defaultProvider, maxDeterminizedStates);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   * 
   * @param term regular expression.
   * @param flags optional RegExp features from {@link RegExp}
   * @param provider custom AutomatonProvider for named automata
   * @param maxDeterminizedStates maximum number of states that compiling the
   *  automaton for the regexp can result in.  Set higher to allow more complex
   *  queries and lower to prevent memory exhaustion.
   */
  public RegexpQuery(Term term, int flags, AutomatonProvider provider,
      int maxDeterminizedStates) {
    super(term,
          new RegExp(term.text(), flags).toAutomaton(
                       provider, maxDeterminizedStates), maxDeterminizedStates);
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
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
}
