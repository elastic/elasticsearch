/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.avg;

import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

import java.io.IOException;

/**
 *
 */
public class AvgAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;

    private LongArray counts;
    private DoubleArray sums;


    public AvgAggregator(String name, long estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            counts = BigArrays.newLongArray(initialSize, context.pageCacheRecycler(), true);
            sums = BigArrays.newDoubleArray(initialSize, context.pageCacheRecycler(), true);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "if value source is null, collect should never be called";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        counts = BigArrays.grow(counts, owningBucketOrdinal + 1);
        sums = BigArrays.grow(sums, owningBucketOrdinal + 1);

        final int valueCount = values.setDocument(doc);
        counts.increment(owningBucketOrdinal, valueCount);
        double sum = 0;
        for (int i = 0; i < valueCount; i++) {
            sum += values.nextValue();
        }
        sums.increment(owningBucketOrdinal, sum);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null || owningBucketOrdinal >= counts.size()) {
            return new InternalAvg(name, 0l, 0);
        }
        return new InternalAvg(name, sums.get(owningBucketOrdinal), counts.get(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalAvg(name, 0.0, 0l);
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        public Factory(String name, String type, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig) {
            super(name, type, valuesSourceConfig);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new AvgAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new AvgAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }

    @Override
    public void doRelease() {
        Releasables.release(counts, sums);
    }

}
