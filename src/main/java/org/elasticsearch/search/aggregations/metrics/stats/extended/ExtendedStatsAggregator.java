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
package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
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
public class ExtendedStatsAggregator extends NumericMetricsAggregator.MultiValue {

    private final ValuesSource.Numeric valuesSource;
    private SortedNumericDoubleValues values;

    private LongArray counts;
    private DoubleArray sums;
    private DoubleArray mins;
    private DoubleArray maxes;
    private DoubleArray sumOfSqrs;

    public ExtendedStatsAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valuesSource, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            counts = bigArrays.newLongArray(initialSize, true);
            sums = bigArrays.newDoubleArray(initialSize, true);
            mins = bigArrays.newDoubleArray(initialSize, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
            maxes = bigArrays.newDoubleArray(initialSize, false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
            sumOfSqrs = bigArrays.newDoubleArray(initialSize, true);
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
        if (owningBucketOrdinal >= counts.size()) {
            final long from = counts.size();
            final long overSize = BigArrays.overSize(owningBucketOrdinal + 1);
            counts = bigArrays.resize(counts, overSize);
            sums = bigArrays.resize(sums, overSize);
            mins = bigArrays.resize(mins, overSize);
            maxes = bigArrays.resize(maxes, overSize);
            sumOfSqrs = bigArrays.resize(sumOfSqrs, overSize);
            mins.fill(from, overSize, Double.POSITIVE_INFINITY);
            maxes.fill(from, overSize, Double.NEGATIVE_INFINITY);
        }

        values.setDocument(doc);
        final int valuesCount = values.count();
        counts.increment(owningBucketOrdinal, valuesCount);
        double sum = 0;
        double sumOfSqr = 0;
        double min = mins.get(owningBucketOrdinal);
        double max = maxes.get(owningBucketOrdinal);
        for (int i = 0; i < valuesCount; i++) {
            double value = values.valueAt(i);
            sum += value;
            sumOfSqr += value * value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        sums.increment(owningBucketOrdinal, sum);
        sumOfSqrs.increment(owningBucketOrdinal, sumOfSqr);
        mins.set(owningBucketOrdinal, min);
        maxes.set(owningBucketOrdinal, max);
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            InternalExtendedStats.Metrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        switch(InternalExtendedStats.Metrics.resolve(name)) {
            case count: return valuesSource == null ? 0 : counts.get(owningBucketOrd);
            case sum: return valuesSource == null ? 0 : sums.get(owningBucketOrd);
            case min: return valuesSource == null ? Double.POSITIVE_INFINITY : mins.get(owningBucketOrd);
            case max: return valuesSource == null ? Double.NEGATIVE_INFINITY : maxes.get(owningBucketOrd);
            case avg: return valuesSource == null ? Double.NaN : sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
            case sum_of_squares: return valuesSource == null ? 0 : sumOfSqrs.get(owningBucketOrd);
            case variance: return valuesSource == null ? Double.NaN : variance(owningBucketOrd);
            case std_deviation: return valuesSource == null ? Double.NaN : Math.sqrt(variance(owningBucketOrd));
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    private double variance(long owningBucketOrd) {
        double sum = sums.get(owningBucketOrd);
        long count = counts.get(owningBucketOrd);
        return (sumOfSqrs.get(owningBucketOrd) - ((sum * sum) / count)) / count;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalExtendedStats(name, 0, 0d, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d);
        }
        assert owningBucketOrdinal < counts.size();
        return new InternalExtendedStats(name, counts.get(owningBucketOrdinal), sums.get(owningBucketOrdinal), mins.get(owningBucketOrdinal),
                maxes.get(owningBucketOrdinal), sumOfSqrs.get(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalExtendedStats(name, 0, 0d, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d);
    }

    @Override
    public void doClose() {
        Releasables.close(counts, maxes, mins, sumOfSqrs, sums);
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig) {
            super(name, InternalExtendedStats.TYPE.name(), valuesSourceConfig);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new ExtendedStatsAggregator(name, 0, null, aggregationContext, parent);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new ExtendedStatsAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent);
        }
    }
}
