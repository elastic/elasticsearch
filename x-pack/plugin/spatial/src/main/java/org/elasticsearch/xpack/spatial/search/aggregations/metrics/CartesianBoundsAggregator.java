/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.BoundsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * A metric aggregator that computes a cartesian-bounds from a {@code point} type field
 */
public final class CartesianBoundsAggregator extends BoundsAggregator<CartesianPoint> {

    public CartesianBoundsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, valuesSourceConfig, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final CartesianPointValuesSource.MultiCartesianPointValues values = ((CartesianPointValuesSource) valuesSource).pointValues(
            aggCtx.getLeafReaderContext()
        );
        return getLeafBucketCollector(values, sub);
    }

    @Override
    protected InternalCartesianBounds makeInternalBounds(
        String name,
        double top,
        double bottom,
        double posLeft,
        double posRight,
        double negLeft,
        double negRight,
        Map<String, Object> metadata
    ) {
        return new InternalCartesianBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, metadata);
    }
}
