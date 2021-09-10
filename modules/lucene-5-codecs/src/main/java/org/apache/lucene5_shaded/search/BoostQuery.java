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


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Term;

/**
 * A {@link Query} wrapper that allows to give a boost to the wrapped query.
 * Boost values that are less than one will give less importance to this
 * query compared to other ones while values that are greater than one will
 * give more importance to the scores returned by this query.
 */
public final class BoostQuery extends Query {

  /** By default we enclose the wrapped query within parenthesis, but this is
   *  not required for all queries, so we use a whitelist of queries that don't
   *  need parenthesis to have a better toString(). */
  private static final Set<Class<? extends Query>> NO_PARENS_REQUIRED_QUERIES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          MatchAllDocsQuery.class,
          TermQuery.class,
          PhraseQuery.class,
          MultiPhraseQuery.class,
          ConstantScoreQuery.class,
          TermRangeQuery.class,
          NumericRangeQuery.class,
          PrefixQuery.class,
          FuzzyQuery.class,
          WildcardQuery.class,
          RegexpQuery.class
      )));

  private final Query query;

  /** Sole constructor: wrap {@code query} in such a way that the produced
   *  scores will be boosted by {@code boost}. */
  public BoostQuery(Query query, float boost) {
    this.query = Objects.requireNonNull(query);
    setBoost(boost);
  }

  /**
   * Return the wrapped {@link Query}.
   */
  public Query getQuery() {
    return query;
  }

  @Override
  public float getBoost() {
    // overridden to remove the deprecation warning
    return super.getBoost();
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj) == false) {
      return false;
    }
    BoostQuery that = (BoostQuery) obj;
    return query.equals(that.query);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + query.hashCode();
    return h;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = query.rewrite(reader);

    if (getBoost() == 1f) {
      return rewritten;
    }

    if (rewritten.getClass() == BoostQuery.class) {
      BoostQuery in = (BoostQuery) rewritten;
      return new BoostQuery(in.query, getBoost() * in.getBoost());
    }

    if (getBoost() == 0f && rewritten.getClass() != ConstantScoreQuery.class) {
      // so that we pass needScores=false
      return new BoostQuery(new ConstantScoreQuery(rewritten), 0f);
    }

    if (query != rewritten) {
      return new BoostQuery(rewritten, getBoost());
    }

    return this;
  }

  @Override
  public String toString(String field) {
    boolean needsParens = NO_PARENS_REQUIRED_QUERIES.contains(query.getClass()) == false;
    StringBuilder builder = new StringBuilder();
    if (needsParens) {
      builder.append("(");
    }
    builder.append(query.toString(field));
    if (needsParens) {
      builder.append(")");
    }
    builder.append("^");
    builder.append(getBoost());
    return builder.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight weight = query.createWeight(searcher, needsScores);
    if (needsScores == false) {
      return weight;
    }
    // Apply the query boost, this may impact the return value of getValueForNormalization()
    weight.normalize(1f, getBoost());
    return new Weight(this) {

      @Override
      public void extractTerms(Set<Term> terms) {
        weight.extractTerms(terms);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return weight.explain(context, doc);
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return weight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float boost) {
        weight.normalize(norm, BoostQuery.this.getBoost() * boost);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return weight.scorer(context);
      }
      
      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        return weight.bulkScorer(context);
      }
    };
  }

}
