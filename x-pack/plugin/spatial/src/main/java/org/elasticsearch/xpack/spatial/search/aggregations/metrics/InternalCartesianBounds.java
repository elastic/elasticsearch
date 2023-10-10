/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalBounds;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class InternalCartesianBounds extends InternalBounds<CartesianPoint> implements CartesianBounds {
    public final double left;
    public final double right;

    public InternalCartesianBounds(String name, double top, double bottom, double left, double right, Map<String, Object> metadata) {
        super(name, top, bottom, metadata);
        this.left = left;
        this.right = right;
    }

    /**
     * Read from a stream.
     */
    public InternalCartesianBounds(StreamInput in) throws IOException {
        super(in);
        left = in.readDouble();
        right = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeDouble(left);
        out.writeDouble(right);
    }

    static InternalCartesianBounds empty(String name, Map<String, Object> metadata) {
        return new InternalCartesianBounds(
            name,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            metadata
        );
    }

    @Override
    public String getWriteableName() {
        return CartesianBoundsAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;

        for (InternalAggregation aggregation : aggregations) {
            InternalCartesianBounds bounds = (InternalCartesianBounds) aggregation;
            top = max(top, bounds.top);
            bottom = min(bottom, bounds.bottom);
            left = min(left, bounds.left);
            right = max(right, bounds.right);
        }
        return new InternalCartesianBounds(name, top, bottom, left, right, getMetadata());
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
        } else {
            return new CartesianBoundingBox(new CartesianPoint(left, top), new CartesianPoint(right, bottom));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalCartesianBounds other = (InternalCartesianBounds) obj;
        return top == other.top && bottom == other.bottom && left == other.left && right == other.right;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bottom, left, right);
    }
}
