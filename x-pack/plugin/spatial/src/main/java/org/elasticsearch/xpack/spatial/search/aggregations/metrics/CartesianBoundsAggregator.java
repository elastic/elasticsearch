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
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * A metric aggregator that computes a cartesian-bounds from a {@code point} type field
 */
public final class CartesianBoundsAggregator extends CartesianBoundsAggregatorBase {
    private final CartesianPointValuesSource valuesSource;

    public CartesianBoundsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (CartesianPointValuesSource) valuesSourceConfig.getValuesSource();
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        final CartesianPointValuesSource.MultiCartesianPointValues values = valuesSource.pointValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeResize(bucket);
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; ++i) {
                        CartesianPoint value = values.nextValue();
                        addBounds(bucket, value.getY(), value.getY(), value.getX(), value.getX());
                    }
                }
            }
        };
    }
}
