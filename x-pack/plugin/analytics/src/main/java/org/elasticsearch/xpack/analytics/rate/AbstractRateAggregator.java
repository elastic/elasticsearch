/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.SizedBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AbstractRateAggregator extends NumericMetricsAggregator.SingleValue {

    protected final ValuesSource valuesSource;
    private final DocValueFormat format;
    private final Rounding.DateTimeUnit rateUnit;
    protected final RateMode rateMode;
    private final CachedSupplier<RateDivisorCalculator> rateDivisorCalculator;

    protected DoubleArray sums;
    protected DoubleArray compensations;

    public AbstractRateAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        Rounding.DateTimeUnit rateUnit,
        RateMode rateMode,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSourceConfig.getValuesSource();
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            sums = bigArrays().newDoubleArray(1, true);
            compensations = bigArrays().newDoubleArray(1, true);
            if (rateMode == null) {
                rateMode = RateMode.SUM;
            }
        }
        this.rateUnit = rateUnit;
        this.rateMode = rateMode;
        this.rateDivisorCalculator = new CachedSupplier<>(this::constructRateDivisor);
    }

    private RateDivisorCalculator constructRateDivisor() {
        SizedBucketAggregator sizedBucketAggregator = null;
        boolean isParent = false;
        for (Aggregator ancestor = parent; ancestor != null; ancestor = ancestor.parent()) {
            if (ancestor instanceof SizedBucketAggregator) {
                sizedBucketAggregator = (SizedBucketAggregator) ancestor;
                isParent = ancestor == parent;
                break;
            }
        }
        if (sizedBucketAggregator == null) {
            for (Aggregator ancestor = parent; ancestor != null; ancestor = ancestor.parent()) {
                if (ancestor instanceof CompositeAggregator) {
                    List<SizedBucketAggregator> compositeSizedBucketAggregators = ((CompositeAggregator) ancestor)
                        .getSizedBucketAggregators();
                    if (compositeSizedBucketAggregators.size() != 1) {
                        throw new IllegalArgumentException(
                            "the ancestor composite aggregation [" + ancestor.name() + "] must have exactly one date_histogram group by"
                        );
                    }
                    sizedBucketAggregator = compositeSizedBucketAggregators.get(0);
                    isParent = ancestor == parent;
                    break;
                }
            }
        }
        if (sizedBucketAggregator == null) {
            throw new IllegalArgumentException(
                "The rate aggregation can only be used inside a date histogram aggregation or "
                    + "composite aggregation with exactly one date histogram value source"
            );
        }
        return new RateDivisorCalculator(sizedBucketAggregator, isParent, rateUnit);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (rateDivisorCalculator == null || valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd) / rateDivisorCalculator.get().divisor(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalRate(name, sums.get(bucket), rateDivisorCalculator.get().divisor(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRate(name, 0.0, 1.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }

    private static class RateDivisorCalculator {
        private final SizedBucketAggregator delegate;
        private final boolean isParent;
        private final Rounding.DateTimeUnit rateUnit;

        RateDivisorCalculator(SizedBucketAggregator delegate, boolean isParent, Rounding.DateTimeUnit rateUnit) {
            this.delegate = delegate;
            this.isParent = isParent;
            this.rateUnit = rateUnit;
        }

        private double divisor(long owningBucketOrd) {
            return isParent ? delegate.bucketSize(owningBucketOrd, rateUnit) : delegate.bucketSize(rateUnit);
        }
    }
}
