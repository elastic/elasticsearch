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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class FiltersAggregator extends BucketsAggregator {

    static class KeyedFilter {

        final String key;
        final Query filter;

        KeyedFilter(String key, Query filter) {
            this.key = key;
            this.filter = filter;
        }
    }

    private final String[] keys;
    private final Weight[] filters;
    private final boolean keyed;
    private final boolean showOtherBucket;
    private final String otherBucketKey;
    private final int totalNumKeys;

    public FiltersAggregator(String name, AggregatorFactories factories, List<KeyedFilter> filters, boolean keyed, String otherBucketKey,
            AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.keyed = keyed;
        this.keys = new String[filters.size()];
        this.filters = new Weight[filters.size()];
        this.showOtherBucket = otherBucketKey != null;
        this.otherBucketKey = otherBucketKey;
        if (showOtherBucket) {
            this.totalNumKeys = filters.size() + 1;
        } else {
            this.totalNumKeys = filters.size();
        }
        for (int i = 0; i < filters.size(); ++i) {
            KeyedFilter keyedFilter = filters.get(i);
            this.keys[i] = keyedFilter.key;
            this.filters[i] = aggregationContext.searchContext().searcher().createNormalizedWeight(keyedFilter.filter, false);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits[] bits = new Bits[filters.length];
        for (int i = 0; i < filters.length; ++i) {
            bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filters[i].scorer(ctx));
        }
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                boolean matched = false;
                for (int i = 0; i < bits.length; i++) {
                    if (bits[i].get(doc)) {
                        collectBucket(sub, doc, bucketOrd(bucket, i));
                        matched = true;
                    }
                }
                if (showOtherBucket && !matched) {
                    collectBucket(sub, doc, bucketOrd(bucket, bits.length));
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        List<InternalFilters.Bucket> buckets = new ArrayList<>(filters.length);
        for (int i = 0; i < keys.length; i++) {
            long bucketOrd = bucketOrd(owningBucketOrdinal, i);
            InternalFilters.Bucket bucket = new InternalFilters.Bucket(keys[i], bucketDocCount(bucketOrd), bucketAggregations(bucketOrd), keyed);
            buckets.add(bucket);
        }
        // other bucket
        if (showOtherBucket) {
            long bucketOrd = bucketOrd(owningBucketOrdinal, keys.length);
            InternalFilters.Bucket bucket = new InternalFilters.Bucket(otherBucketKey, bucketDocCount(bucketOrd),
                    bucketAggregations(bucketOrd), keyed);
            buckets.add(bucket);
        }
        return new InternalFilters(name, buckets, keyed, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalFilters.Bucket> buckets = new ArrayList<>(filters.length);
        for (int i = 0; i < keys.length; i++) {
            InternalFilters.Bucket bucket = new InternalFilters.Bucket(keys[i], 0, subAggs, keyed);
            buckets.add(bucket);
        }
        return new InternalFilters(name, buckets, keyed, pipelineAggregators(), metaData());
    }

    final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
        return owningBucketOrdinal * totalNumKeys + filterOrd;
    }

    public static class Factory extends AggregatorFactory {

        private final List<KeyedFilter> filters;
        private boolean keyed;
        private String otherBucketKey;

        public Factory(String name, List<KeyedFilter> filters, boolean keyed, String otherBucketKey) {
            super(name, InternalFilters.TYPE.name());
            this.filters = filters;
            this.keyed = keyed;
            this.otherBucketKey = otherBucketKey;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return new FiltersAggregator(name, factories, filters, keyed, otherBucketKey, context, parent, pipelineAggregators, metaData);
        }
    }

}
