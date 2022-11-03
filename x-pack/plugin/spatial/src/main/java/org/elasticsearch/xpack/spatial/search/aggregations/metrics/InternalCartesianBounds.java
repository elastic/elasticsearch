/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.metrics.InternalBounds;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.Map;

public class InternalCartesianBounds extends InternalBounds<CartesianPoint> {

    public InternalCartesianBounds(
        String name,
        double top,
        double bottom,
        double posLeft,
        double posRight,
        double negLeft,
        double negRight,
        Map<String, Object> metadata
    ) {
        super(name, top, bottom, posLeft, posRight, negLeft, negRight, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalCartesianBounds(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return CartesianBoundsAggregationBuilder.NAME;
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
        return new InternalCartesianBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, getMetadata());
    }

    @Override
    protected Object selectCoordinate(String coordinateString, CartesianPoint cornerPoint) {
        return switch (coordinateString) {
            case "x" -> cornerPoint.getX();
            case "y" -> cornerPoint.getY();
            default -> throw new IllegalArgumentException("Found unknown path element [" + coordinateString + "] in [" + getName() + "]");
        };
    }

    @Override
    protected BoundingBox<CartesianPoint> resolveBoundingBox() {
        if (Double.isInfinite(top)) {
            return null;
        } else if (Double.isInfinite(posLeft)) {
            return new CartesianBoundingBox(new CartesianPoint(negLeft, top), new CartesianPoint(negRight, bottom));
        } else if (Double.isInfinite(negLeft)) {
            return new CartesianBoundingBox(new CartesianPoint(posLeft, top), new CartesianPoint(posRight, bottom));
        } else {
            return new CartesianBoundingBox(new CartesianPoint(negLeft, top), new CartesianPoint(posRight, bottom));
        }
    }
}
