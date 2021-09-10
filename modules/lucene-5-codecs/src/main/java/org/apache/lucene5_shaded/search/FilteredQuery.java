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
import java.util.Set;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.search.BooleanClause.Occur;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.ToStringUtils;


/**
 * A query that applies a filter to the results of another query.
 *
 * <p>Note: the bits are retrieved from the filter each time this
 * query is used in a search - use a CachingWrapperFilter to avoid
 * regenerating the bits every time.
 * @since   1.4
 * @see     CachingWrapperQuery
 * @deprecated FilteredQuery will be removed in Lucene 6.0. It should
 *             be replaced with a {@link BooleanQuery} with one
 *             {@link Occur#MUST} clause for the query and one
 *             {@link Occur#FILTER} clause for the filter.
 */
@Deprecated
public class FilteredQuery extends Query {

  private final Query query;
  private final Filter filter;
  private final FilterStrategy strategy;

  /**
   * Constructs a new query which applies a filter to the results of the original query.
   * {@link Filter#getDocIdSet} will be called every time this query is used in a search.
   * @param query  Query to be filtered, cannot be <code>null</code>.
   * @param filter Filter to apply to query results, cannot be <code>null</code>.
   */
  public FilteredQuery(Query query, Filter filter) {
    this(query, filter, RANDOM_ACCESS_FILTER_STRATEGY);
  }
  
  /**
   * Expert: Constructs a new query which applies a filter to the results of the original query.
   * {@link Filter#getDocIdSet} will be called every time this query is used in a search.
   * @param query  Query to be filtered, cannot be <code>null</code>.
   * @param filter Filter to apply to query results, cannot be <code>null</code>.
   * @param strategy a filter strategy used to create a filtered scorer. 
   * 
   * @see FilterStrategy
   */
  public FilteredQuery(Query query, Filter filter, FilterStrategy strategy) {
    this.strategy = Objects.requireNonNull(strategy, "FilterStrategy must not be null");
    this.query = Objects.requireNonNull(query, "Query must not be null");
    this.filter = Objects.requireNonNull(filter, "Filter must not be null");
  }
  
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(query, Occur.MUST);
    builder.add(strategy.rewrite(filter), Occur.FILTER);
    return builder.build();
  }

  /** Returns this FilteredQuery's (unfiltered) Query */
  public final Query getQuery() {
    return query;
  }

  /** Returns this FilteredQuery's filter */
  public final Filter getFilter() {
    return filter;
  }
  
  /** Returns this FilteredQuery's {@link FilterStrategy} */
  public FilterStrategy getFilterStrategy() {
    return this.strategy;
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString (String s) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("filtered(");
    buffer.append(query.toString(s));
    buffer.append(")->");
    buffer.append(filter);
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!super.equals(o))
      return false;
    assert o instanceof FilteredQuery;
    final FilteredQuery fq = (FilteredQuery) o;
    return fq.query.equals(this.query) && fq.filter.equals(this.filter) && fq.strategy.equals(this.strategy);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + strategy.hashCode();
    hash = hash * 31 + query.hashCode();
    hash = hash * 31 + filter.hashCode();
    return hash;
  }
  
  /**
   * A {@link FilterStrategy} that conditionally uses a random access filter if
   * the given {@link DocIdSet} supports random access (returns a non-null value
   * from {@link DocIdSet#bits()}) and
   * {@link RandomAccessFilterStrategy#useRandomAccess(Bits, long)} returns
   * <code>true</code>. Otherwise this strategy falls back to a "zig-zag join" (
   * {@link FilteredQuery#LEAP_FROG_FILTER_FIRST_STRATEGY}) strategy.
   * 
   * <p>
   * Note: this strategy is the default strategy in {@link FilteredQuery}
   * </p>
   */
  public static final FilterStrategy RANDOM_ACCESS_FILTER_STRATEGY = new RandomAccessFilterStrategy();
  
  /**
   * A filter strategy that uses a "leap-frog" approach (also called "zig-zag join").
   * In spite of the name of this constant, which one will be iterated first depends
   * on the {@link DocIdSetIterator#cost() cost} of the filter compared to the query.
   */
  public static final FilterStrategy LEAP_FROG_FILTER_FIRST_STRATEGY = new RandomAccessFilterStrategy() {
    protected boolean useRandomAccess(Bits bits, long filterCost) {
      return false;
    }
  };
  
  /**
   * A filter strategy that uses a "leap-frog" approach (also called "zig-zag join").
   * In spite of the name of this constant, which one will be iterated first depends
   * on the {@link DocIdSetIterator#cost() cost} of the filter compared to the query.
   */
  public static final FilterStrategy LEAP_FROG_QUERY_FIRST_STRATEGY = LEAP_FROG_FILTER_FIRST_STRATEGY;
  
  /**
   * A filter strategy that advances the Query or rather its {@link Scorer} first and consults the
   * filter {@link DocIdSet} for each matched document.
   * <p>
   * Note: this strategy requires a {@link DocIdSet#bits()} to return a non-null value. Otherwise
   * this strategy falls back to {@link FilteredQuery#LEAP_FROG_QUERY_FIRST_STRATEGY}
   * </p>
   * <p>
   * Use this strategy if the filter computation is more expensive than document
   * scoring or if the filter has a linear running time to compute the next
   * matching doc like exact geo distances.
   * </p>
   */
  public static final FilterStrategy QUERY_FIRST_FILTER_STRATEGY = new RandomAccessFilterStrategy() {
    @Override
    boolean alwaysUseRandomAccess() {
      return true;
    }
  };
  
  /** Abstract class that defines how the filter ({@link DocIdSet}) applied during document collection. */
  public static abstract class FilterStrategy {

    /** Rewrite the filter. */
    public abstract Query rewrite(Filter filter);

  }

  /**
   * A {@link FilterStrategy} that conditionally uses a random access filter if
   * the given {@link DocIdSet} supports random access (returns a non-null value
   * from {@link DocIdSet#bits()}) and
   * {@link RandomAccessFilterStrategy#useRandomAccess(Bits, long)} returns
   * <code>true</code>. Otherwise this strategy falls back to a "zig-zag join" (
   * {@link FilteredQuery#LEAP_FROG_FILTER_FIRST_STRATEGY}) strategy .
   */
  public static class RandomAccessFilterStrategy extends FilterStrategy {

    @Override
    public Query rewrite(Filter filter) {
      return new RandomAccessFilterWrapperQuery(filter, this);
    }

    /**
     * Expert: decides if a filter should be executed as "random-access" or not.
     * random-access means the filter "filters" in a similar way as deleted docs are filtered
     * in Lucene. This is faster when the filter accepts many documents.
     * However, when the filter is very sparse, it can be faster to execute the query+filter
     * as a conjunction in some cases.
     * 
     * The default implementation returns <code>true</code> if the filter matches more than 1%
     * of documents
     * 
     * @lucene.internal
     */
    protected boolean useRandomAccess(Bits bits, long filterCost) {
      // if the filter matches more than 1% of documents, we use random-access
      return filterCost * 100 > bits.length();
    }

    // back door for QUERY_FIRST_FILTER_STRATEGY, when this returns true we
    // will try to use the random-access API regardless of the iterator
    boolean alwaysUseRandomAccess() {
      return false;
    }
  }

  private static class RandomAccessFilterWrapperQuery extends Query {

    final Filter filter;
    final RandomAccessFilterStrategy strategy;

    private RandomAccessFilterWrapperQuery(Filter filter, RandomAccessFilterStrategy strategy) {
      this.filter = Objects.requireNonNull(filter);
      this.strategy = Objects.requireNonNull(strategy);
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      RandomAccessFilterWrapperQuery that = (RandomAccessFilterWrapperQuery) obj;
      return filter.equals(that.filter) && strategy.equals(that.strategy);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + Objects.hash(filter, strategy);
    }

    @Override
    public String toString(String field) {
      return filter.toString(field);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new Weight(this) {

        @Override
        public void extractTerms(Set<Term> terms) {}

        @Override
        public float getValueForNormalization() throws IOException {
          return 0f;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {}

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          final Scorer s = scorer(context);
          final boolean match;
          if (s == null) {
            match = false;
          } else {
            final TwoPhaseIterator twoPhase = s.twoPhaseIterator();
            if (twoPhase == null) {
              match = s.iterator().advance(doc) == doc;
            } else {
              match = twoPhase.approximation().advance(doc) == doc && twoPhase.matches();
            }
          }
          if (match) {
            assert s.score() == 0f;
            return Explanation.match(0f, "Match on id " + doc);
          } else {
            return Explanation.match(0f, "No match on id " + doc);
          }
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final DocIdSet set = filter.getDocIdSet(context, null);
          if (set == null) {
            return null;
          }
          final Bits bits = set.bits();
          boolean useRandomAccess = bits != null && strategy.alwaysUseRandomAccess();
          final DocIdSetIterator iterator;
          if (useRandomAccess) {
            // we don't need the iterator
            iterator = null;
          } else {
            iterator = set.iterator();
            if (iterator == null) {
              return null;
            }
            if (bits != null) {
              useRandomAccess = strategy.useRandomAccess(bits, iterator.cost());
            }
          }

          if (useRandomAccess) {
            // use the random-access API
            final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
            final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
              @Override
              public boolean matches() throws IOException {
                final int doc = approximation.docID();
                return bits.get(doc);
              }
              @Override
              public float matchCost() {
                return 10; // TODO use cost of bits.get()
              }
            };
            return new ConstantScoreScorer(this, 0f, twoPhase);
          } else {
            // use the iterator API
            return new ConstantScoreScorer(this, 0f, iterator);
          }
        }

      };
    }

  }

}
