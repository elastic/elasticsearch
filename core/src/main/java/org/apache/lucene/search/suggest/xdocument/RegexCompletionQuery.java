package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import java.io.IOException;

/**
 * A {@link CompletionQuery} which takes a regular expression
 * as the prefix of the query term.
 *
 * <p>
 * Example usage of querying a prefix of 'sug' and 'sub'
 * as a regular expression against a suggest field 'suggest_field':
 *
 * <pre class="prettyprint">
 *  CompletionQuery query = new RegexCompletionQuery(new Term("suggest_field", "su[g|b]"));
 * </pre>
 *
 * <p>
 * See {@link RegExp} for the supported regular expression
 * syntax
 *
 * @lucene.experimental
 */
public class RegexCompletionQuery extends CompletionQuery {

  private final int flags;
  private final int maxDeterminizedStates;

  /**
   * Calls {@link RegexCompletionQuery#RegexCompletionQuery(Term, Filter)}
   * with no filter
   */
  public RegexCompletionQuery(Term term) {
    this(term, null);
  }

  /**
   * Calls {@link RegexCompletionQuery#RegexCompletionQuery(Term, int, int, Filter)}
   * enabling all optional regex syntax and <code>maxDeterminizedStates</code> of
   * {@value Operations#DEFAULT_MAX_DETERMINIZED_STATES}
   */
  public RegexCompletionQuery(Term term, Filter filter) {
    this(term, RegExp.ALL, Operations.DEFAULT_MAX_DETERMINIZED_STATES, filter);
  }
  /**
   * Calls {@link RegexCompletionQuery#RegexCompletionQuery(Term, int, int, Filter)}
   * with no filter
   */
  public RegexCompletionQuery(Term term, int flags, int maxDeterminizedStates) {
    this(term, flags, maxDeterminizedStates, null);
  }

  /**
   * Constructs a regular expression completion query
   *
   * @param term query is run against {@link Term#field()} and {@link Term#text()}
   *             is interpreted as a regular expression
   * @param flags used as syntax_flag in {@link RegExp#RegExp(String, int)}
   * @param maxDeterminizedStates used in {@link RegExp#toAutomaton(int)}
   * @param filter used to query on a sub set of documents
   */
  public RegexCompletionQuery(Term term, int flags, int maxDeterminizedStates, Filter filter) {
    super(term, filter);
    this.flags = flags;
    this.maxDeterminizedStates = maxDeterminizedStates;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new CompletionWeight(this, new RegExp(getTerm().text(), flags).toAutomaton(maxDeterminizedStates));
  }
}
