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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A {@link CompletionQuery} which takes an {@link Analyzer}
 * to analyze the prefix of the query term.
 * <p>
 * Example usage of querying an analyzed prefix 'sugg'
 * against a field 'suggest_field' is as follows:
 *
 * <pre class="prettyprint">
 *  CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
 * </pre>
 * @lucene.experimental
 */
public class PrefixCompletionQuery extends CompletionQuery {
  /** Used to analyze the term text */
  protected final CompletionAnalyzer analyzer;

  /**
   * Calls {@link PrefixCompletionQuery#PrefixCompletionQuery(Analyzer, Term, Filter)}
   * with no filter
   */
  public PrefixCompletionQuery(Analyzer analyzer, Term term) {
    this(analyzer, term, null);
  }

  /**
   * Constructs an analyzed prefix completion query
   *
   * @param analyzer used to analyze the provided {@link Term#text()}
   * @param term query is run against {@link Term#field()} and {@link Term#text()}
   *             is analyzed with <code>analyzer</code>
   * @param filter used to query on a sub set of documents
   */
  public PrefixCompletionQuery(Analyzer analyzer, Term term, Filter filter) {
    super(term, filter);
    if (!(analyzer instanceof CompletionAnalyzer)) {
      this.analyzer = new CompletionAnalyzer(analyzer);
    } else {
      this.analyzer = (CompletionAnalyzer) analyzer;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    CompletionTokenStream stream = (CompletionTokenStream) analyzer.tokenStream(getField(), getTerm().text());
    return new CompletionWeight(this, stream.toAutomaton());
  }
}
