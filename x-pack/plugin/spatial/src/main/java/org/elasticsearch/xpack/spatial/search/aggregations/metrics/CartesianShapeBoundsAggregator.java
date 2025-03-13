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
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * A metric aggregator that computes a cartesian-bounds from a {@code shape} type field
 */
public final class CartesianShapeBoundsAggregator extends CartesianBoundsAggregatorBase {
    private final CartesianShapeValuesSource valuesSource;

    public CartesianShapeBoundsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (CartesianShapeValuesSource) valuesSourceConfig.getValuesSource();
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        CartesianShapeValues values = valuesSource.shapeValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeResize(bucket);
                    CartesianShapeValues.CartesianShapeValue value = values.value();
                    ShapeValues.BoundingBox bounds = value.boundingBox();
                    addBounds(bucket, bounds.top, bounds.bottom, bounds.minX(), bounds.maxX());
                }
            }
        };
    }
}
