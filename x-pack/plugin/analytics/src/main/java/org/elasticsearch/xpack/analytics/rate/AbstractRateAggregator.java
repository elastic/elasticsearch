/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.SizedBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractRateAggregator extends NumericMetricsAggregator.SingleValue {

    protected final ValuesSource valuesSource;
    private final DocValueFormat format;
    private final Rounding.DateTimeUnit rateUnit;
    protected final RateMode rateMode;
    private final SizedBucketAggregator sizedBucketAggregator;
    protected final boolean computeWithDocCount;

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
        this.sizedBucketAggregator = findSizedBucketAncestor();
        // If no fields or scripts have been defined in the agg, rate should be computed based on bucket doc_counts
        this.computeWithDocCount = valuesSourceConfig.fieldContext() == null && valuesSourceConfig.script() == null;
    }

    private SizedBucketAggregator findSizedBucketAncestor() {
        SizedBucketAggregator aggregator = null;
        for (Aggregator ancestor = parent; ancestor != null; ancestor = ancestor.parent()) {
            if (ancestor instanceof SizedBucketAggregator) {
                aggregator = (SizedBucketAggregator) ancestor;
                break;
            }
        }
        if (aggregator == null) {
            throw new IllegalArgumentException(
                "The rate aggregation can only be used inside a date histogram aggregation or "
                    + "composite aggregation with one date histogram value source"
            );
        }
        return aggregator;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (sizedBucketAggregator == null || valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd) / divisor(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalRate(name, sums.get(bucket), divisor(bucket), format, metadata());
    }

    private double divisor(long owningBucketOrd) {
        if (sizedBucketAggregator == parent) {
            return sizedBucketAggregator.bucketSize(owningBucketOrd, rateUnit);
        } else {
            return sizedBucketAggregator.bucketSize(rateUnit);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRate(name, 0.0, 1.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }
}
