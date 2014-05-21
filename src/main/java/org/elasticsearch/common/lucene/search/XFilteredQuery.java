package org.elasticsearch.common.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.FilteredQuery.FilterStrategy;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;

import java.io.IOException;
import java.util.Set;


/**
 * A query that applies a filter to the results of another query.
 * <p/>
 * <p>Note: the bits are retrieved from the filter each time this
 * query is used in a search - use a CachingWrapperFilter to avoid
 * regenerating the bits every time.
 *
 * @see CachingWrapperFilter
 * @since 1.4
 */
// Changes are marked with //CHANGE:
// Delegate to FilteredQuery - this version fixes the bug in LUCENE-4705 and uses ApplyAcceptedDocsFilter internally
public final class XFilteredQuery extends Query {
    private final Filter rawFilter;
    private final FilteredQuery delegate;
    private final FilterStrategy strategy;

    /**
     * Constructs a new query which applies a filter to the results of the original query.
     * {@link Filter#getDocIdSet} will be called every time this query is used in a search.
     *
     * @param query  Query to be filtered, cannot be <code>null</code>.
     * @param filter Filter to apply to query results, cannot be <code>null</code>.
     */
    public XFilteredQuery(Query query, Filter filter) {
        this(query, filter, FilteredQuery.RANDOM_ACCESS_FILTER_STRATEGY);
    }

    /**
     * Expert: Constructs a new query which applies a filter to the results of the original query.
     * {@link Filter#getDocIdSet} will be called every time this query is used in a search.
     *
     * @param query    Query to be filtered, cannot be <code>null</code>.
     * @param filter   Filter to apply to query results, cannot be <code>null</code>.
     * @param strategy a filter strategy used to create a filtered scorer.
     * @see FilterStrategy
     */
    public XFilteredQuery(Query query, Filter filter, FilterStrategy strategy) {
        this(new FilteredQuery(query, new ApplyAcceptedDocsFilter(filter), strategy), filter, strategy);
    }

    private XFilteredQuery(FilteredQuery delegate, Filter filter, FilterStrategy strategy) {
        this.delegate = delegate;
        // CHANGE: we need to wrap it in post application of accepted docs
        this.rawFilter = filter;
        this.strategy = strategy;
    }

    /**
     * Returns a Weight that applies the filter to the enclosed query's Weight.
     * This is accomplished by overriding the Scorer returned by the Weight.
     */
    @Override
    public Weight createWeight(final IndexSearcher searcher) throws IOException {
        return delegate.createWeight(searcher);
    }

    /**
     * Rewrites the query. If the wrapped is an instance of
     * {@link MatchAllDocsQuery} it returns a {@link ConstantScoreQuery}. Otherwise
     * it returns a new {@code FilteredQuery} wrapping the rewritten query.
     */
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query query = delegate.getQuery();
        final Query queryRewritten = query.rewrite(reader);

        // CHANGE: if we push back to Lucene, would love to have an extension for "isMatchAllQuery"
        if (queryRewritten instanceof MatchAllDocsQuery || Queries.isConstantMatchAllQuery(queryRewritten)) {
            // Special case: If the query is a MatchAllDocsQuery, we only
            // return a CSQ(filter).
            final Query rewritten = new ConstantScoreQuery(delegate.getFilter());
            // Combine boost of MatchAllDocsQuery and the wrapped rewritten query:
            rewritten.setBoost(delegate.getBoost() * queryRewritten.getBoost());
            return rewritten;
        }

        if (queryRewritten != query) {
            // rewrite to a new FilteredQuery wrapping the rewritten query
            final Query rewritten = new XFilteredQuery(queryRewritten, rawFilter, strategy);
            rewritten.setBoost(delegate.getBoost());
            return rewritten;
        } else {
            // nothing to rewrite, we are done!
            return this;
        }
    }

    @Override
    public void setBoost(float b) {
        delegate.setBoost(b);
    }

    @Override
    public float getBoost() {
        return delegate.getBoost();
    }

    /**
     * Returns this FilteredQuery's (unfiltered) Query
     */
    public final Query getQuery() {
        return delegate.getQuery();
    }

    /**
     * Returns this FilteredQuery's filter
     */
    public final Filter getFilter() {
        // CHANGE: unwrap the accepted docs filter
        if (rawFilter instanceof ApplyAcceptedDocsFilter) {
            return ((ApplyAcceptedDocsFilter) rawFilter).filter();
        }
        return rawFilter;
    }

    // inherit javadoc
    @Override
    public void extractTerms(Set<Term> terms) {
        delegate.extractTerms(terms);
    }

    /**
     * Prints a user-readable version of this query.
     */
    @Override
    public String toString(String s) {
        return delegate.toString(s);
    }

    /**
     * Returns true iff <code>o</code> is equal to this.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof XFilteredQuery)) {
            return false;
        } else {
            return delegate.equals(((XFilteredQuery)o).delegate);
        }
    }

    /**
     * Returns a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    // CHANGE: Add custom random access strategy, allowing to set the threshold
    // CHANGE: Add filter first filter strategy
    public static final FilterStrategy ALWAYS_RANDOM_ACCESS_FILTER_STRATEGY = new CustomRandomAccessFilterStrategy(0);

    public static final CustomRandomAccessFilterStrategy CUSTOM_FILTER_STRATEGY = new CustomRandomAccessFilterStrategy();

    /**
     * Extends {@link org.apache.lucene.search.FilteredQuery.RandomAccessFilterStrategy}.
     * <p/>
     * Adds a threshold value, which defaults to -1. When set to -1, it will check if the filter docSet is
     * *not*  a fast docSet, and if not, it will use {@link FilteredQuery#QUERY_FIRST_FILTER_STRATEGY} (since
     * the assumption is that its a "slow" filter and better computed only on whatever matched the query).
     * <p/>
     * If the threshold value is 0, it always tries to pass "down" the filter as acceptDocs, and it the filter
     * can't be represented as Bits (never really), then it uses {@link FilteredQuery#LEAP_FROG_QUERY_FIRST_STRATEGY}.
     * <p/>
     * If the above conditions are not met, then it reverts to the {@link FilteredQuery.RandomAccessFilterStrategy} logic,
     * with the threshold used to control {@link #useRandomAccess(org.apache.lucene.util.Bits, int)}.
     */
    public static class CustomRandomAccessFilterStrategy extends FilteredQuery.RandomAccessFilterStrategy {

        private final int threshold;

        public CustomRandomAccessFilterStrategy() {
            this.threshold = -1;
        }

        public CustomRandomAccessFilterStrategy(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public Scorer filteredScorer(AtomicReaderContext context, Weight weight, DocIdSet docIdSet) throws IOException {
            // CHANGE: If threshold is 0, always pass down the accept docs, don't pay the price of calling nextDoc even...
            if (threshold == 0) {
                final Bits filterAcceptDocs = docIdSet.bits();
                if (filterAcceptDocs != null) {
                    return weight.scorer(context, filterAcceptDocs);
                } else {
                    return FilteredQuery.LEAP_FROG_QUERY_FIRST_STRATEGY.filteredScorer(context, weight, docIdSet);
                }
            }

            // CHANGE: handle "default" value
            if (threshold == -1) {
                // default  value, don't iterate on only apply filter after query if its not a "fast" docIdSet
                if (!DocIdSets.isFastIterator(ApplyAcceptedDocsFilter.unwrap(docIdSet))) {
                    return FilteredQuery.QUERY_FIRST_FILTER_STRATEGY.filteredScorer(context, weight, docIdSet);
                }
            }

            return super.filteredScorer(context, weight, docIdSet);
        }

        /**
         * Expert: decides if a filter should be executed as "random-access" or not.
         * random-access means the filter "filters" in a similar way as deleted docs are filtered
         * in Lucene. This is faster when the filter accepts many documents.
         * However, when the filter is very sparse, it can be faster to execute the query+filter
         * as a conjunction in some cases.
         * <p/>
         * The default implementation returns <code>true</code> if the first document accepted by the
         * filter is < threshold, if threshold is -1 (the default), then it checks for < 100.
         */
        protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
            // "default"
            if (threshold == -1) {
                return firstFilterDoc < 100;
            }
            //TODO once we have a cost API on filters and scorers we should rethink this heuristic
            return firstFilterDoc < threshold;
        }
    }

    @Override
    public Query clone() {
        return new XFilteredQuery((FilteredQuery) delegate.clone(), rawFilter, strategy);
    }

}
