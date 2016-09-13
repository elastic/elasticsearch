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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ExtendedStatsAggregator extends NumericMetricsAggregator.MultiValue {

    public static final ParseField SIGMA_FIELD = new ParseField("sigma");

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;
    final double sigma;

    LongArray counts;
    DoubleArray sums;
    DoubleArray mins;
    DoubleArray maxes;
    DoubleArray sumOfSqrs;

    public ExtendedStatsAggregator(String name, ValuesSource.Numeric valuesSource, DocValueFormat formatter,
            AggregationContext context, Aggregator parent, double sigma, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData)
            throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.format = formatter;
        this.sigma = sigma;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            counts = bigArrays.newLongArray(1, true);
            sums = bigArrays.newDoubleArray(1, true);
            mins = bigArrays.newDoubleArray(1, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
            maxes = bigArrays.newDoubleArray(1, false);
            maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
            sumOfSqrs = bigArrays.newDoubleArray(1, true);
        }
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= counts.size()) {
                    final long from = counts.size();
                    final long overSize = BigArrays.overSize(bucket + 1);
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
                counts.increment(bucket, valuesCount);
                double sum = 0;
                double sumOfSqr = 0;
                double min = mins.get(bucket);
                double max = maxes.get(bucket);
                for (int i = 0; i < valuesCount; i++) {
                    double value = values.valueAt(i);
                    sum += value;
                    sumOfSqr += value * value;
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
                sums.increment(bucket, sum);
                sumOfSqrs.increment(bucket, sumOfSqr);
                mins.set(bucket, min);
                maxes.set(bucket, max);
            }

        };
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
        if (valuesSource == null || owningBucketOrd >= counts.size()) {
            switch(InternalExtendedStats.Metrics.resolve(name)) {
                case count: return 0;
                case sum: return 0;
                case min: return Double.POSITIVE_INFINITY;
                case max: return Double.NEGATIVE_INFINITY;
                case avg: return Double.NaN;
                case sum_of_squares: return 0;
                case variance: return Double.NaN;
                case std_deviation: return Double.NaN;
                case std_upper: return Double.NaN;
                case std_lower: return Double.NaN;
                default:
                    throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
            }
        }
        switch(InternalExtendedStats.Metrics.resolve(name)) {
            case count: return counts.get(owningBucketOrd);
            case sum: return sums.get(owningBucketOrd);
            case min: return mins.get(owningBucketOrd);
            case max: return maxes.get(owningBucketOrd);
            case avg: return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
            case sum_of_squares: return sumOfSqrs.get(owningBucketOrd);
            case variance: return variance(owningBucketOrd);
            case std_deviation: return Math.sqrt(variance(owningBucketOrd));
            case std_upper:
                return (sums.get(owningBucketOrd) / counts.get(owningBucketOrd)) + (Math.sqrt(variance(owningBucketOrd)) * this.sigma);
            case std_lower:
                return (sums.get(owningBucketOrd) / counts.get(owningBucketOrd)) - (Math.sqrt(variance(owningBucketOrd)) * this.sigma);
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    private double variance(long owningBucketOrd) {
        double sum = sums.get(owningBucketOrd);
        long count = counts.get(owningBucketOrd);
        return (sumOfSqrs.get(owningBucketOrd) - ((sum * sum) / count)) / count;
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalExtendedStats(name, counts.get(bucket), sums.get(bucket),
                mins.get(bucket), maxes.get(bucket), sumOfSqrs.get(bucket), sigma, format,
                pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalExtendedStats(name, 0, 0d, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d, sigma, format, pipelineAggregators(),
                metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, maxes, mins, sumOfSqrs, sums);
    }
}
