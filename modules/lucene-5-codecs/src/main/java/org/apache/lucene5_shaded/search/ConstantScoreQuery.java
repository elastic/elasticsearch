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
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * A query that wraps another query and simply returns a constant score equal to
 * 1 for every document that matches the query.
 * It therefore simply strips of all scores and always returns 1.
 */
public final class ConstantScoreQuery extends Query {
  private final Query query;

  /** Strips off scores from the passed in Query. The hits will get a constant score
   * of 1. */
  public ConstantScoreQuery(Query query) {
    this.query = Objects.requireNonNull(query, "Query must not be null");
  }

  /** Returns the encapsulated query. */
  public Query getQuery() {
    return query;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }

    Query rewritten = query.rewrite(reader);

    if (rewritten != query) {
      return new ConstantScoreQuery(rewritten);
    }

    if (rewritten.getClass() == ConstantScoreQuery.class) {
      return rewritten;
    }

    if (rewritten.getClass() == BoostQuery.class) {
      return new ConstantScoreQuery(((BoostQuery) rewritten).getQuery());
    }

    return super.rewrite(reader);
  }

  /** We return this as our {@link BulkScorer} so that if the CSQ
   *  wraps a query with its own optimized top-level
   *  scorer (e.g. BooleanScorer) we can use that
   *  top-level scorer. */
  protected class ConstantBulkScorer extends BulkScorer {
    final BulkScorer bulkScorer;
    final Weight weight;
    final float theScore;

    public ConstantBulkScorer(BulkScorer bulkScorer, Weight weight, float theScore) {
      this.bulkScorer = bulkScorer;
      this.weight = weight;
      this.theScore = theScore;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
      return bulkScorer.score(wrapCollector(collector), acceptDocs, min, max);
    }

    private LeafCollector wrapCollector(LeafCollector collector) {
      return new FilterLeafCollector(collector) {
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          // we must wrap again here, but using the scorer passed in as parameter:
          in.setScorer(new FilterScorer(scorer) {
            @Override
            public float score() throws IOException {
              return theScore;
            }
            @Override
            public int freq() throws IOException {
              return 1;
            }
          });
        }
      };
    }

    @Override
    public long cost() {
      return bulkScorer.cost();
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight innerWeight = searcher.createWeight(query, false);
    if (needsScores) {
      return new ConstantScoreWeight(this) {

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
          final BulkScorer innerScorer = innerWeight.bulkScorer(context);
          if (innerScorer == null) {
            return null;
          }
          return new ConstantBulkScorer(innerScorer, this, score());
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final Scorer innerScorer = innerWeight.scorer(context);
          if (innerScorer == null) {
            return null;
          }
          final float score = score();
          return new FilterScorer(innerScorer) {
            @Override
            public float score() throws IOException {
              return score;
            }
            @Override
            public int freq() throws IOException {
              return 1;
            }
            @Override
            public Collection<ChildScorer> getChildren() {
              return Collections.singleton(new ChildScorer(innerScorer, "constant"));
            }
          };
        }

      };
    } else {
      return innerWeight;
    }
  }

  @Override
  public String toString(String field) {
    return new StringBuilder("ConstantScore(")
      .append(query.toString(field))
      .append(')')
      .append(ToStringUtils.boost(getBoost()))
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!super.equals(o))
      return false;
    if (o instanceof ConstantScoreQuery) {
      final ConstantScoreQuery other = (ConstantScoreQuery) o;
      return this.query.equals(other.query);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }

}
