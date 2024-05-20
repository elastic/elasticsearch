/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

/**
 * An {@link NumericMetricsAggregator.SingleValue} that is not collected, this can typically be used when running
 * an aggregation over a field that doesn't have a mapping.
 *
 * see {@link org.elasticsearch.search.aggregations.NonCollectingAggregator}
 */
public final class NonCollectingSingleMetricAggregator extends NumericMetricsAggregator.SingleValue {

    private final InternalNumericMetricsAggregation.SingleValue emptyAggregation;

    /**
     * Build a {@linkplain NonCollectingSingleMetricAggregator} for {@link NumericMetricsAggregator.SingleValue} aggregators.
     */
    public NonCollectingSingleMetricAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        InternalNumericMetricsAggregation.SingleValue emptyAggregation,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.emptyAggregation = emptyAggregation;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        // the framework will automatically eliminate it
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        return buildEmptyAggregation();
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return emptyAggregation;
    }

    @Override
    public double metric(long owningBucketOrd) {
        return emptyAggregation.value();
    }
}
