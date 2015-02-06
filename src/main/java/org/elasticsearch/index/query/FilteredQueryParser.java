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

package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCachingPolicy;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.FilteredQuery.FilterStrategy;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class FilteredQueryParser implements QueryParser {

    public static final String NAME = "filtered";

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
        public Scorer filteredScorer(LeafReaderContext context, Weight weight, DocIdSet docIdSet, boolean needsScores) throws IOException {
            // CHANGE: If threshold is 0, always pass down the accept docs, don't pay the price of calling nextDoc even...
            final Bits filterAcceptDocs = docIdSet.bits();
            if (threshold == 0) {
                if (filterAcceptDocs != null) {
                    return weight.scorer(context, filterAcceptDocs, needsScores);
                } else {
                    return FilteredQuery.LEAP_FROG_QUERY_FIRST_STRATEGY.filteredScorer(context, weight, docIdSet, needsScores);
                }
            }

            // CHANGE: handle "default" value
            if (threshold == -1) {
                // default  value, don't iterate on only apply filter after query if its not a "fast" docIdSet
                // TODO: is there a way we could avoid creating an iterator here?
                if (filterAcceptDocs != null && DocIdSets.isBroken(docIdSet.iterator())) {
                    return FilteredQuery.QUERY_FIRST_FILTER_STRATEGY.filteredScorer(context, weight, docIdSet, needsScores);
                }
            }

            return super.filteredScorer(context, weight, docIdSet, needsScores);
        }

        @Override
        protected boolean useRandomAccess(Bits bits, long filterCost) {
          int multiplier = threshold;
          if (threshold == -1) {
              // default
              multiplier = 100;
          }
          return filterCost * multiplier > bits.length();
        }
    }

    @Inject
    public FilteredQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = Queries.newMatchAllQuery();
        Filter filter = null;
        boolean filterFound = false;
        float boost = 1.0f;
        FilterCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        FilteredQuery.FilterStrategy filterStrategy = CUSTOM_FILTER_STRATEGY;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else if ("filter".equals(currentFieldName)) {
                    filterFound = true;
                    filter = parseContext.parseInnerFilter();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[filtered] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("strategy".equals(currentFieldName)) {
                    String value = parser.text();
                    if ("query_first".equals(value) || "queryFirst".equals(value)) {
                        filterStrategy = FilteredQuery.QUERY_FIRST_FILTER_STRATEGY;
                    } else if ("random_access_always".equals(value) || "randomAccessAlways".equals(value)) {
                        filterStrategy = ALWAYS_RANDOM_ACCESS_FILTER_STRATEGY;
                    } else if ("leap_frog".equals(value) || "leapFrog".equals(value)) {
                        filterStrategy = FilteredQuery.LEAP_FROG_QUERY_FIRST_STRATEGY;
                    } else if (value.startsWith("random_access_")) {
                        int threshold = Integer.parseInt(value.substring("random_access_".length()));
                        filterStrategy = new CustomRandomAccessFilterStrategy(threshold);
                    } else if (value.startsWith("randomAccess")) {
                        int threshold = Integer.parseInt(value.substring("randomAccess".length()));
                        filterStrategy = new CustomRandomAccessFilterStrategy(threshold);
                    } else if ("leap_frog_query_first".equals(value) || "leapFrogQueryFirst".equals(value)) {
                        filterStrategy = FilteredQuery.LEAP_FROG_QUERY_FIRST_STRATEGY;
                    } else if ("leap_frog_filter_first".equals(value) || "leapFrogFilterFirst".equals(value)) {
                        filterStrategy = FilteredQuery.LEAP_FROG_FILTER_FIRST_STRATEGY;
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[filtered] strategy value not supported [" + value + "]");
                    }
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[filtered] query does not support [" + currentFieldName + "]");
                }
            }
        }

        // parsed internally, but returned null during parsing...
        if (query == null) {
            return null;
        }

        if (filter == null) {
            if (!filterFound) {
                // we allow for null filter, so it makes compositions on the client side to be simpler
                return query;
            } else {
                // even if the filter is not found, and its null, we should simply ignore it, and go
                // by the query
                return query;
            }
        }
        if (filter == Queries.MATCH_ALL_FILTER) {
            // this is an instance of match all filter, just execute the query
            return query;
        }

        // cache if required
        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }

        // if its a match_all query, use constant_score
        if (Queries.isConstantMatchAllQuery(query)) {
            Query q = new ConstantScoreQuery(filter);
            q.setBoost(boost);
            return q;
        }

        FilteredQuery filteredQuery = new FilteredQuery(query, filter, filterStrategy);
        filteredQuery.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, filteredQuery);
        }
        return filteredQuery;
    }
}
