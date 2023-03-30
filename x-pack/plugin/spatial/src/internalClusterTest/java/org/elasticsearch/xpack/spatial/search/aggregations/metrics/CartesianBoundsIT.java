/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.metrics.SpatialBounds;
import org.elasticsearch.search.aggregations.metrics.SpatialBoundsAggregationTestBase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.Collection;
import java.util.Collections;

@ESIntegTestCase.SuiteScopeTestCase
public class CartesianBoundsIT extends SpatialBoundsAggregationTestBase<CartesianPoint> {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    @Override
    protected String aggName() {
        return "cartesianBounds";
    }

    @Override
    protected CartesianBoundsAggregationBuilder boundsAgg(String aggName, String fieldName) {
        return new CartesianBoundsAggregationBuilder(aggName).field(fieldName);
    }

    @Override
    protected void assertBoundsLimits(SpatialBounds<CartesianPoint> spatialBounds) {
        // Cartesian does not have specific bounds limits like geo data does
    }

    @Override
    protected String fieldTypeName() {
        return "point";
    }

    @Override
    protected CartesianPoint makePoint(double x, double y) {
        return new CartesianPoint((float) x, (float) y);
    }

    @Override
    protected CartesianPoint randomPoint() {
        Point point = ShapeTestUtils.randomPointNotExtreme(false);
        return makePoint(point.getX(), point.getY());
    }

    @Override
    protected void resetX(SpatialPoint point, double x) {
        ((CartesianPoint) point).resetX((float) x);
    }

    @Override
    protected void resetY(SpatialPoint point, double y) {
        ((CartesianPoint) point).resetY((float) y);
    }

    @Override
    protected CartesianPoint reset(SpatialPoint point, double x, double y) {
        return ((CartesianPoint) point).reset((float) x, (float) y);
    }
}
