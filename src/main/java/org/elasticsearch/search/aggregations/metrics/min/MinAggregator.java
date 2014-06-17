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
package org.elasticsearch.search.aggregations.metrics.min;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

/**
 *
 */
public class MinAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private DoubleValues values;

    private DoubleArray mins;

    public MinAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valuesSource, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            mins = bigArrays.newDoubleArray(initialSize, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.doubleValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        if (values.setDocument(doc) == 0) {
            return;
        }

        if (owningBucketOrdinal >= mins.size()) {
            long from = mins.size();
            mins = bigArrays.grow(mins, owningBucketOrdinal + 1);
            mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
        }

        mins.set(owningBucketOrdinal, Math.min(values.nextValue(), mins.get(owningBucketOrdinal)));
    }

    @Override
    public double metric(long owningBucketOrd) {
        return valuesSource == null ? Double.POSITIVE_INFINITY : mins.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalMin(name, Double.POSITIVE_INFINITY);
        }
        assert owningBucketOrdinal < mins.size();
        return new InternalMin(name, mins.get(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMin(name, Double.POSITIVE_INFINITY);
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig) {
            super(name, InternalMin.TYPE.name(), valuesSourceConfig);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new MinAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new MinAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }

    @Override
    public void doClose() {
        Releasables.close(mins);
    }
}
