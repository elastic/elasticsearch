/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Map;

class ExtendedStatsAggregator extends NumericMetricsAggregator.MultiDoubleValue {

    static final ParseField SIGMA_FIELD = new ParseField("sigma");

    final DocValueFormat format;
    final double sigma;

    LongArray counts;
    DoubleArray sums;
    DoubleArray compensations;
    DoubleArray mins;
    DoubleArray maxes;
    DoubleArray sumOfSqrs;
    DoubleArray compensationOfSqrs;

    ExtendedStatsAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double sigma,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, context, parent, metadata);
        assert config.hasValues();
        this.format = config.format();
        this.sigma = sigma;
        final BigArrays bigArrays = context.bigArrays();
        counts = bigArrays.newLongArray(1, true);
        sums = bigArrays.newDoubleArray(1, true);
        compensations = bigArrays.newDoubleArray(1, true);
        mins = bigArrays.newDoubleArray(1, false);
        mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        maxes = bigArrays.newDoubleArray(1, false);
        maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        sumOfSqrs = bigArrays.newDoubleArray(1, true);
        compensationOfSqrs = bigArrays.newDoubleArray(1, true);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, final LeafBucketCollector sub) {
        final CompensatedSum compensatedSum = new CompensatedSum(0, 0);
        final CompensatedSum compensatedSumOfSqr = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    final int valuesCount = values.docValueCount();
                    counts.increment(bucket, valuesCount);
                    double min = mins.get(bucket);
                    double max = maxes.get(bucket);
                    // Compute the sum and sum of squires for double values with Kahan summation algorithm
                    // which is more accurate than naive summation.
                    compensatedSum.reset(sums.get(bucket), compensations.get(bucket));
                    compensatedSumOfSqr.reset(sumOfSqrs.get(bucket), compensationOfSqrs.get(bucket));

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        compensatedSum.add(value);
                        compensatedSumOfSqr.add(value * value);
                        min = Math.min(min, value);
                        max = Math.max(max, value);
                    }

                    sums.set(bucket, compensatedSum.value());
                    compensations.set(bucket, compensatedSum.delta());
                    sumOfSqrs.set(bucket, compensatedSumOfSqr.value());
                    compensationOfSqrs.set(bucket, compensatedSumOfSqr.delta());
                    mins.set(bucket, min);
                    maxes.set(bucket, max);
                }
            }
        };
    }

    @Override
    protected LeafBucketCollector getLeafCollector(NumericDoubleValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeGrow(bucket);
                    final double value = values.doubleValue();
                    counts.increment(bucket, 1L);
                    SumAggregator.computeSum(bucket, value, sums, compensations);
                    SumAggregator.computeSum(bucket, value * value, sumOfSqrs, compensationOfSqrs);
                    StatsAggregator.updateMinsAndMaxes(bucket, value, mins, maxes);
                }
            }

        };
    }

    private void maybeGrow(long bucket) {
        if (bucket >= counts.size()) {
            final long from = counts.size();
            final long overSize = BigArrays.overSize(bucket + 1);
            var bigArrays = bigArrays();
            counts = bigArrays.resize(counts, overSize);
            sums = bigArrays.resize(sums, overSize);
            compensations = bigArrays.resize(compensations, overSize);
            mins = bigArrays.resize(mins, overSize);
            maxes = bigArrays.resize(maxes, overSize);
            sumOfSqrs = bigArrays.resize(sumOfSqrs, overSize);
            compensationOfSqrs = bigArrays.resize(compensationOfSqrs, overSize);
            mins.fill(from, overSize, Double.POSITIVE_INFINITY);
            maxes.fill(from, overSize, Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public boolean hasMetric(String name) {
        return InternalExtendedStats.Metrics.hasMetric(name);
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        if (owningBucketOrd >= counts.size()) {
            return switch (InternalExtendedStats.Metrics.resolve(name)) {
                case count, sum_of_squares, sum -> 0;
                case min -> Double.POSITIVE_INFINITY;
                case max -> Double.NEGATIVE_INFINITY;
                case avg, variance, variance_population, variance_sampling, std_deviation, std_deviation_population, std_deviation_sampling,
                    std_upper, std_lower -> Double.NaN;
                default -> throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
            };
        }
        return switch (InternalExtendedStats.Metrics.resolve(name)) {
            case count -> counts.get(owningBucketOrd);
            case sum -> sums.get(owningBucketOrd);
            case min -> mins.get(owningBucketOrd);
            case max -> maxes.get(owningBucketOrd);
            case avg -> sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
            case sum_of_squares -> sumOfSqrs.get(owningBucketOrd);
            case variance -> variance(owningBucketOrd);
            case variance_population -> variancePopulation(owningBucketOrd);
            case variance_sampling -> varianceSampling(owningBucketOrd);
            case std_deviation, std_deviation_population, std_deviation_sampling -> Math.sqrt(variance(owningBucketOrd));
            case std_upper -> (sums.get(owningBucketOrd) / counts.get(owningBucketOrd)) + (Math.sqrt(variance(owningBucketOrd))
                * this.sigma);
            case std_lower -> (sums.get(owningBucketOrd) / counts.get(owningBucketOrd)) - (Math.sqrt(variance(owningBucketOrd))
                * this.sigma);
            default -> throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        };
    }

    private double variance(long owningBucketOrd) {
        return variancePopulation(owningBucketOrd);
    }

    private double variancePopulation(long owningBucketOrd) {
        double sum = sums.get(owningBucketOrd);
        long count = counts.get(owningBucketOrd);
        double variance = (sumOfSqrs.get(owningBucketOrd) - ((sum * sum) / count)) / count;
        return variance < 0 ? 0 : variance;
    }

    private double varianceSampling(long owningBucketOrd) {
        double sum = sums.get(owningBucketOrd);
        long count = counts.get(owningBucketOrd);
        double variance = (sumOfSqrs.get(owningBucketOrd) - ((sum * sum) / count)) / (count - 1);
        return variance < 0 ? 0 : variance;
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalExtendedStats(
            name,
            counts.get(bucket),
            sums.get(bucket),
            mins.get(bucket),
            maxes.get(bucket),
            sumOfSqrs.get(bucket),
            sigma,
            format,
            metadata()
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalExtendedStats.empty(name, sigma, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, maxes, mins, sumOfSqrs, compensationOfSqrs, sums, compensations);
    }
}
