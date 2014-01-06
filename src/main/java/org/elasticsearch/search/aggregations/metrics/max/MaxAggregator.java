/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations.metrics.max;

import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
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
public class MaxAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;

    private DoubleArray maxes;

    public MaxAggregator(String name, long estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            maxes = BigArrays.newDoubleArray(initialSize, context.pageCacheRecycler(), false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "collect should only be called when value source is not null";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        if (owningBucketOrdinal >= maxes.size()) {
            long from = maxes.size();
            maxes = BigArrays.grow(maxes, owningBucketOrdinal + 1);
            maxes.fill(from, maxes.size(), Double.NEGATIVE_INFINITY);
        }

        final int valueCount = values.setDocument(doc);
        double max = maxes.get(owningBucketOrdinal);
        for (int i = 0; i < valueCount; i++) {
            max = Math.max(max, values.nextValue());
        }
        maxes.set(owningBucketOrdinal, max);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalMax(name, Double.NEGATIVE_INFINITY);
        }
        assert owningBucketOrdinal < maxes.size();
        return new InternalMax(name, maxes.get(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMax(name, Double.NEGATIVE_INFINITY);
    }

    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig) {
            super(name, InternalMax.TYPE.name(), valuesSourceConfig);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new MaxAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new MaxAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }

    @Override
    public void doRelease() {
        Releasables.release(maxes);
    }
}
