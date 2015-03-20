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

import com.google.common.collect.Lists;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class FiltersAggregator extends BucketsAggregator {

    static class KeyedFilter {

        final String key;
        final Filter filter;

        KeyedFilter(String key, Filter filter) {
            this.key = key;
            this.filter = filter;
        }
    }

    private final KeyedFilter[] filters;
    private final boolean keyed;

    public FiltersAggregator(String name, AggregatorFactories factories, List<KeyedFilter> filters, boolean keyed, AggregationContext aggregationContext,
            Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, metaData);
        this.keyed = keyed;
        this.filters = filters.toArray(new KeyedFilter[filters.size()]);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        // TODO: use the iterator if the filter does not support random access
        // no need to provide deleted docs to the filter
        final Bits[] bits = new Bits[filters.length];
        for (int i = 0; i < filters.length; ++i) {
            bits[i] = DocIdSets.asSequentialAccessBits(ctx.reader().maxDoc(), filters[i].filter.getDocIdSet(ctx, null));
        }
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                for (int i = 0; i < bits.length; i++) {
                    if (bits[i].get(doc)) {
                        collectBucket(sub, doc, bucketOrd(bucket, i));
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        List<InternalFilters.Bucket> buckets = Lists.newArrayListWithCapacity(filters.length);
        for (int i = 0; i < filters.length; i++) {
            KeyedFilter filter = filters[i];
            long bucketOrd = bucketOrd(owningBucketOrdinal, i);
            InternalFilters.Bucket bucket = new InternalFilters.Bucket(filter.key, bucketDocCount(bucketOrd), bucketAggregations(bucketOrd), keyed);
            buckets.add(bucket);
        }
        return new InternalFilters(name, buckets, keyed, metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalFilters.Bucket> buckets = Lists.newArrayListWithCapacity(filters.length);
        for (int i = 0; i < filters.length; i++) {
            InternalFilters.Bucket bucket = new InternalFilters.Bucket(filters[i].key, 0, subAggs, keyed);
            buckets.add(bucket);
        }
        return new InternalFilters(name, buckets, keyed, metaData());
    }

    final long bucketOrd(long owningBucketOrdinal, int filterOrd) {
        return owningBucketOrdinal * filters.length + filterOrd;
    }

    public static class Factory extends AggregatorFactory {

        private final List<KeyedFilter> filters;
        private boolean keyed;

        public Factory(String name, List<KeyedFilter> filters, boolean keyed) {
            super(name, InternalFilters.TYPE.name());
            this.filters = filters;
            this.keyed = keyed;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            return new FiltersAggregator(name, factories, filters, keyed, context, parent, metaData);
        }
    }

}
