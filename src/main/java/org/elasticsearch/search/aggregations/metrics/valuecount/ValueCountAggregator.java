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
package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;

import java.io.IOException;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 *
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance aggregates the
 * counts for all buckets owned by the parent aggregator)
 */
public class ValueCountAggregator extends MetricsAggregator.SingleValue {

    private final BytesValuesSource valuesSource;

    // a count per bucket
    LongArray counts;

    public ValueCountAggregator(String name, long expectedBucketsCount, BytesValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent) {
        super(name, 0, aggregationContext, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            // expectedBucketsCount == 0 means it's a top level bucket
            final long initialSize = expectedBucketsCount < 2 ? 1 : expectedBucketsCount;
            counts = BigArrays.newLongArray(initialSize, context.pageCacheRecycler(), true);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        BytesValues values = valuesSource.bytesValues();
        if (values == null) {
            return;
        }
        counts = BigArrays.grow(counts, owningBucketOrdinal + 1);
        counts.increment(owningBucketOrdinal, values.setDocument(doc));
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalValueCount(name, 0);
        }
        assert owningBucketOrdinal < counts.size();
        return new InternalValueCount(name, counts.get(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalValueCount(name, 0l);
    }

    @Override
    public void doRelease() {
        Releasables.release(counts);
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<BytesValuesSource> {

        public Factory(String name, ValuesSourceConfig<BytesValuesSource> valuesSourceBuilder) {
            super(name, InternalValueCount.TYPE.name(), valuesSourceBuilder);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new ValueCountAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(BytesValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new ValueCountAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }

    }

}
