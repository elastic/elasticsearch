/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.HistogramUnionState;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractBoxplotAggregator extends NumericMetricsAggregator.MultiValue {

    protected final ValuesSource valuesSource;
    protected final DocValueFormat format;
    protected ObjectArray<HistogramUnionState> states;
    protected final double compression;
    protected final TDigestExecutionHint executionHint;

    protected AbstractBoxplotAggregator(
        String name,
        ValuesSourceConfig config,
        DocValueFormat formatter,
        double compression,
        TDigestExecutionHint executionHint,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert config.hasValues();
        this.valuesSource = config.getValuesSource();
        this.format = formatter;
        this.compression = compression;
        this.executionHint = executionHint;
        states = context.bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    protected HistogramUnionState getExistingOrNewHistogram(final BigArrays bigArrays, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        HistogramUnionState state = states.get(bucket);
        if (state == null) {
            state = HistogramUnionState.create(HistogramUnionState.NOOP_BREAKER, executionHint, compression);
            states.set(bucket, state);
        }
        return state;
    }

    @Override
    public boolean hasMetric(String name) {
        return InternalBoxplot.Metrics.hasMetric(name);
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        HistogramUnionState state = null;
        if (owningBucketOrd < states.size()) {
            state = states.get(owningBucketOrd);
        }
        return InternalBoxplot.Metrics.resolve(name).value(state);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        HistogramUnionState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalBoxplot(name, state, format, metadata());
        }
    }

    protected HistogramUnionState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        return states.get(bucketOrd);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalBoxplot.empty(name, compression, executionHint, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(states);
    }
}
