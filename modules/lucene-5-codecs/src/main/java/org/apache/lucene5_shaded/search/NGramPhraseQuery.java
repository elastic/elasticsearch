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
import java.util.Objects;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * This is a {@link PhraseQuery} which is optimized for n-gram phrase query.
 * For example, when you query "ABCD" on a 2-gram field, you may want to use
 * NGramPhraseQuery rather than {@link PhraseQuery}, because NGramPhraseQuery
 * will {@link #rewrite(IndexReader)} the query to "AB/0 CD/2", while {@link PhraseQuery}
 * will query "AB/0 BC/1 CD/2" (where term/position).
 *
 */
public class NGramPhraseQuery extends Query {

  private final int n;
  private final PhraseQuery phraseQuery;
  
  /**
   * Constructor that takes gram size.
   * @param n n-gram size
   */
  public NGramPhraseQuery(int n, PhraseQuery query) {
    super();
    this.n = n;
    this.phraseQuery = Objects.requireNonNull(query);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    final Term[] terms = phraseQuery.getTerms();
    final int[] positions = phraseQuery.getPositions();

    boolean isOptimizable = phraseQuery.getSlop() == 0
        && n >= 2 // non-overlap n-gram cannot be optimized
        && terms.length >= 3; // short ones can't be optimized

    if (isOptimizable) {
      for (int i = 1; i < positions.length; ++i) {
        if (positions[i] != positions[i-1] + 1) {
          isOptimizable = false;
          break;
        }
      }
    }
    
    if (isOptimizable == false) {
      return phraseQuery.rewrite(reader);
    }

    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    for (int i = 0; i < terms.length; ++i) {
      if (i % n == 0 || i == terms.length - 1) {
        builder.add(terms[i], i);
      }
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    NGramPhraseQuery other = (NGramPhraseQuery) o;
    return n == other.n && phraseQuery.equals(other.phraseQuery);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + phraseQuery.hashCode();
    h = 31 * h + n;
    return h;
  }

  /** Return the list of terms. */
  public Term[] getTerms() {
    return phraseQuery.getTerms();
  }

  /** Return the list of relative positions that each term should appear at. */
  public int[] getPositions() {
    return phraseQuery.getPositions();
  }

  @Override
  public String toString(String field) {
    return phraseQuery.toString(field) + ToStringUtils.boost(getBoost());
  }
}
