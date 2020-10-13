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

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.compareUnsigned;

public class FiltersAggregatorFactory extends AggregatorFactory {

    private final String[] keys;
    private final Query[] filters;
    private Weight[] weights;
    private final boolean keyed;
    private final boolean otherBucket;
    private final String otherBucketKey;

    public FiltersAggregatorFactory(String name, List<KeyedFilter> filters, boolean keyed, boolean otherBucket,
                                    String otherBucketKey, AggregationContext context, AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactories, Map<String, Object> metadata) throws IOException {
        super(name, context, parent, subFactories, metadata);
        this.keyed = keyed;
        this.otherBucket = otherBucket;
        this.otherBucketKey = otherBucketKey;
        keys = new String[filters.size()];
        this.filters = new Query[filters.size()];
        for (int i = 0; i < filters.size(); ++i) {
            KeyedFilter keyedFilter = filters.get(i);
            this.keys[i] = keyedFilter.key();
            this.filters[i] = context.buildQuery(keyedFilter.filter());
        }
    }

    /**
     * Returns the {@link Weight}s for this filter aggregation, creating it if
     * necessary. This is done lazily so that the {@link Weight}s are only
     * created if the aggregation collects documents reducing the overhead of
     * the aggregation in the case where no documents are collected.
     * <p>
     * Note that as aggregations are initialized and executed in a serial manner,
     * no concurrency considerations are necessary here.
     */
    public Weight[] getWeights(Query query, SearchContext searchContext) {
        if (weights == null) {
            try {
                IndexSearcher contextSearcher = searchContext.searcher();
                weights = new Weight[filters.length];
                for (int i = 0; i < filters.length; ++i) {
                    Query filter = filterMatchingBoth(query, filters[i]);
                    this.weights[i] = contextSearcher.createWeight(contextSearcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1);
                }
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to initialse filters for aggregation [" + name() + "]", e);
            }
        }
        return weights;
    }

    private Query filterMatchingBoth(Query lhs, Query rhs) {
        if (lhs instanceof MatchAllDocsQuery) {
            return rhs;
        }
        if (rhs instanceof MatchAllDocsQuery) {
            return lhs;
        }
        Query unwrappedLhs = unwrap(lhs);
        Query unwrappedRhs = unwrap(rhs);
        LogManager.getLogger().error("ADSFDSAF {} {}", unwrappedLhs, unwrappedRhs);
        LogManager.getLogger().error("ADSFDSAF {} {}", unwrappedLhs instanceof PointRangeQuery, unwrappedRhs instanceof PointRangeQuery);
        if (unwrappedLhs instanceof PointRangeQuery && unwrappedRhs instanceof PointRangeQuery) {
            PointRangeQuery merged = mergePointRangeQueries((PointRangeQuery) unwrappedLhs, (PointRangeQuery) unwrappedRhs);
            LogManager.getLogger().error("ADSFDSAF {}", merged);
            if (merged != null) {
                // TODO rewrap?
                return merged;
            }
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(lhs, BooleanClause.Occur.MUST);
        builder.add(rhs, BooleanClause.Occur.MUST);
        return builder.build();
    }

    private Query unwrap(Query query) {
        if (query instanceof IndexSortSortedNumericDocValuesRangeQuery) {
            query = ((IndexSortSortedNumericDocValuesRangeQuery) query).getFallbackQuery();
        }
        if (query instanceof IndexOrDocValuesQuery) {
            query = ((IndexOrDocValuesQuery) query).getIndexQuery();
        }
        return query;
    }

    private PointRangeQuery mergePointRangeQueries(PointRangeQuery lhs, PointRangeQuery rhs) {
        if (lhs.getField() != rhs.getField() || lhs.getNumDims() != rhs.getNumDims() || lhs.getBytesPerDim() != rhs.getBytesPerDim()) {
            return null;
        }
        byte[] lower = mergePoint(lhs.getLowerPoint(), rhs.getLowerPoint(), lhs.getNumDims(), lhs.getBytesPerDim(), true);
        LogManager.getLogger().error("ADSFDSAF {}", LongPoint.decodeDimension(lower, 0));
        if (lower == null) {
            return null;
        }
        byte[] upper = mergePoint(lhs.getUpperPoint(), rhs.getUpperPoint(), lhs.getNumDims(), lhs.getBytesPerDim(), false);
        LogManager.getLogger().error("ADSFDSAF {}", LongPoint.decodeDimension(upper, 0));
        if (upper == null) {
            return null;
        }
        return new PointRangeQuery(lhs.getField(), lower, upper, lhs.getNumDims()) {
            @Override
            protected String toString(int dimension, byte[] value) {
                // Stolen from Lucene's Binary range query. It'd be best to delegate, but the method isn't visible.
                StringBuilder sb = new StringBuilder();
                sb.append("binary(");
                for (int i = 0; i < value.length; i++) {
                    if (i > 0) {
                        sb.append(' ');
                    }
                    sb.append(Integer.toHexString(value[i] & 0xFF));
                }
                sb.append(')');
                return sb.toString();
            }
        };
    }

    /**
     * Figure out if lhs's lower point is lower in all dimensions than
     * rhs's lower point or if it is further. Return null if it is closer
     * in some dimensions and further in others.
     */
    private byte[] mergePoint(byte[] lhs, byte[] rhs, int numDims, int bytesPerDim, boolean mergingLower) {
        int runningCmp = 0;
        for (int dim = 0; dim < numDims; dim++) {
            int cmp = cmpDim(lhs, rhs, dim, bytesPerDim);
            if (runningCmp == 0) {
                // Previous dimensions were all equal
                runningCmp = cmp;
                continue;
            }
            if (cmp == 0) {
                // This dimension has the same value.
                continue;
            }
            if ((runningCmp ^ cmp) < 0) {
                // Signs differ so this dimension doesn't compare the same way as the previous ones so we can't merge.
                return null;
            }
        }
        if (runningCmp < 0) {
            // lhs is lower
            return mergingLower ? rhs : lhs;
        }
        return mergingLower ? lhs : rhs;
    }

    private int cmpDim(byte[] lhs, byte[] rhs, int dim, int bytesPerDim) {
        int offset = dim * bytesPerDim;
        return compareUnsigned(lhs, offset, offset + bytesPerDim, rhs, offset, offset + bytesPerDim);
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                        Aggregator parent,
                                        CardinalityUpperBound cardinality,
                                        Map<String, Object> metadata) throws IOException {
        return FiltersAggregator.build(name, factories, keys, query -> getWeights(query, searchContext), keyed,
            otherBucket ? otherBucketKey : null, searchContext, parent, cardinality, metadata);
    }


}
