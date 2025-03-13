/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;

import java.io.IOException;
import java.util.Map;

public abstract class TTestAggregator<T extends TTestState> extends NumericMetricsAggregator.SingleValue {

    protected final MultiValuesSource.NumericMultiValuesSource valuesSources;
    protected final int tails;

    private final DocValueFormat format;

    TTestAggregator(
        String name,
        MultiValuesSource.NumericMultiValuesSource valuesSources,
        int tails,
        DocValueFormat format,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSources = valuesSources;
        this.tails = tails;
        this.format = format;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSources != null && valuesSources.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    protected abstract T getState(long bucket);

    protected abstract T getEmptyState();

    protected abstract long size();

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null || bucket >= size()) {
            return buildEmptyAggregation();
        }
        return new InternalTTest(name, getState(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTTest(name, getEmptyState(), format, metadata());
    }

    @Override
    public double metric(long owningBucketOrd) {
        return getState(owningBucketOrd).getValue();
    }
}
