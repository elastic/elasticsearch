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
package org.elasticsearch.search.aggregations.bucket.exclude;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class ExcludeAggregator extends GlobalAggregator {

    private final Filter filter;

    private Bits bits;

    public ExcludeAggregator(String name,
                             Filter filter,
                             AggregatorFactories factories,
                             AggregationContext aggregationContext) {
        super(name, factories, aggregationContext);

        this.filter = filter;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        if (filter != null) {
            try {
                bits = DocIdSets.toSafeBits(reader.reader(), filter.getDocIdSet(reader, reader.reader().getLiveDocs()));
            } catch (IOException ioe) {
                throw new AggregationExecutionException("Failed to aggregate exclude aggregator [" + name + "]", ioe);
            }
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0 : "exclude aggregator can only be a top level aggregator";
        if (filter != null && bits.get(doc)) {
            collectBucket(doc, owningBucketOrdinal);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0 : "exclude aggregator can only be a top level aggregator";
        return new InternalExclude(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException("exclude aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations");
    }

    public static class Factory extends AggregatorFactory {

        private boolean excludeQuery = false;

        private Set<String> excludeFilters;

        public Factory(String name, boolean excludeQuery, Set<String> excludeFilters) {
            super(name, InternalFilter.TYPE.name());

            this.excludeQuery = excludeQuery;
            this.excludeFilters = excludeFilters;
        }

        protected void checkExcludeQuery(List<Filter> filters, Query query) {
            if(!excludeQuery) {
                filters.add(Queries.wrap(query));
            }
        }

        protected void checkExcludeFilters(List<Filter> filters,ParsedQuery parsedQuery, Filter filter) {
            if(filter instanceof AndFilter) {
                AndFilter andFilter = (AndFilter) filter;

                Set<Filter> excludeFiltersSet = new HashSet<>();
                for(Map.Entry<String,Filter> entry : parsedQuery.namedFilters().entrySet()) {
                    if(excludeFilters.contains(entry.getKey())) {
                        excludeFiltersSet.add(entry.getValue());
                    }
                }

                for(Filter innerFilter : andFilter.filters()) {
                    if(!excludeFiltersSet.contains(innerFilter)) {
                        filters.add(innerFilter);
                    }
                }
            } else {
                throw new AggregationExecutionException("Aggregation [" + InternalExclude.TYPE + "] requires the query top level " +
                        "filter to be of type AndFilter");
            }
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            if (parent != null) {
                throw new AggregationExecutionException("Aggregation [" + parent.name() + "] cannot have a exclude " +
                        "sub-aggregation [" + name + "]. Exclude aggregations can only be defined as top level aggregations");
            }

            ParsedQuery parsedQuery = context.searchContext().parsedQuery();
            Query query = parsedQuery.query();
            ArrayList<Filter> filters = newArrayList();

            if(query instanceof XFilteredQuery) {
                XFilteredQuery filteredQuery = (XFilteredQuery) query;

                Query innerQuery = filteredQuery.getQuery();
                Filter filter = filteredQuery.getFilter();

                checkExcludeQuery(filters, innerQuery);
                checkExcludeFilters(filters, parsedQuery, filter);

            } else if(query instanceof XConstantScoreQuery) {
                XConstantScoreQuery constantQuery = (XConstantScoreQuery) query;

                Filter filter = constantQuery.getFilter();

                checkExcludeFilters(filters, parsedQuery, filter);
            } else {
                checkExcludeQuery(filters, query);
            }

            if(filters.isEmpty()) {
                return new ExcludeAggregator(name, Queries.MATCH_ALL_FILTER, factories, context);
            } else {
                return new ExcludeAggregator(name, new AndFilter(filters), factories, context);
            }
        }

    }
}


