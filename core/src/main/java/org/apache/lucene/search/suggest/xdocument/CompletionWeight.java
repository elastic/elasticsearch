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
package org.apache.lucene.search.suggest.xdocument;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.BitsProducer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;

import java.io.IOException;
import java.util.Set;

/**
 * Expert: the Weight for CompletionQuery, used to
 * score and explain these queries.
 *
 * Subclasses can override {@link #setNextMatch(IntsRef)},
 * {@link #boost()} and {@link #context()}
 * to calculate the boost and extract the context of
 * a matched path prefix.
 *
 * @lucene.experimental
 */
public class CompletionWeight extends Weight {
  private final CompletionQuery completionQuery;
  private final Automaton automaton;

  /**
   * Creates a weight for <code>query</code> with an <code>automaton</code>,
   * using the <code>reader</code> for index stats
   */
  public CompletionWeight(final CompletionQuery query, final Automaton automaton) throws IOException {
    super(query);
    this.completionQuery = query;
    this.automaton = automaton;
  }

  /**
   * Returns the automaton specified
   * by the {@link CompletionQuery}
   *
   * @return query automaton
   */
  public Automaton getAutomaton() {
    return automaton;
  }

  @Override
  public BulkScorer bulkScorer(final LeafReaderContext context) throws IOException {
    final LeafReader reader = context.reader();
    final Terms terms;
    final NRTSuggester suggester;
    if ((terms = reader.terms(completionQuery.getField())) == null) {
      return null;
    }
    if (terms instanceof CompletionTerms) {
      CompletionTerms completionTerms = (CompletionTerms) terms;
      if ((suggester = completionTerms.suggester()) == null) {
        // a segment can have a null suggester
        // i.e. no FST was built
        return null;
      }
    } else {
      throw new IllegalArgumentException(completionQuery.getField() + " is not a SuggestField");
    }

    BitsProducer filter = completionQuery.getFilter();
    Bits filteredDocs = null;
    if (filter != null) {
      filteredDocs = filter.getBits(context);
      if (filteredDocs.getClass() == Bits.MatchNoBits.class) {
        return null;
      }
    }
    return new CompletionScorer(this, suggester, reader, filteredDocs, filter != null, automaton);
  }

  /**
   * Set for every partial path in the index that matched the query
   * automaton.
   *
   * Subclasses should override {@link #boost()} and {@link #context()}
   * to return an appropriate value with respect to the current pathPrefix.
   *
   * @param pathPrefix the prefix of a matched path
   */
  protected void setNextMatch(IntsRef pathPrefix) {
  }

  /**
   * Returns the boost of the partial path set by {@link #setNextMatch(IntsRef)}
   *
   * @return suggestion query-time boost
   */
  protected float boost() {
    return 0;
  }

  /**
   * Returns the context of the partial path set by {@link #setNextMatch(IntsRef)}
   *
   * @return suggestion context
   */
  protected CharSequence context() {
    return null;
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    // no-op
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    //TODO
    return null;
  }

  @Override
  public float getValueForNormalization() throws IOException {
    return 0;
  }

  @Override
  public void normalize(float norm, float boost) {
  }
}
