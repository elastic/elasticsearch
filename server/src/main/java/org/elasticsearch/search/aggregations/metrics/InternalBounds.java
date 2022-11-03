/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class InternalBounds<T extends SpatialPoint> extends InternalAggregation implements SpatialBounds<T> {
    public final double top;
    public final double bottom;
    public final double posLeft;
    public final double posRight;
    public final double negLeft;
    public final double negRight;

    public InternalBounds(
        String name,
        double top,
        double bottom,
        double posLeft,
        double posRight,
        double negLeft,
        double negRight,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.top = top;
        this.bottom = bottom;
        this.posLeft = posLeft;
        this.posRight = posRight;
        this.negLeft = negLeft;
        this.negRight = negRight;
    }

    /**
     * Read from a stream.
     */
    public InternalBounds(StreamInput in) throws IOException {
        super(in);
        top = in.readDouble();
        bottom = in.readDouble();
        posLeft = in.readDouble();
        posRight = in.readDouble();
        negLeft = in.readDouble();
        negRight = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(top);
        out.writeDouble(bottom);
        out.writeDouble(posLeft);
        out.writeDouble(posRight);
        out.writeDouble(negLeft);
        out.writeDouble(negRight);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;

        for (InternalAggregation aggregation : aggregations) {
            InternalBounds<?> bounds = (InternalBounds<?>) aggregation;

            if (bounds.top > top) {
                top = bounds.top;
            }
            if (bounds.bottom < bottom) {
                bottom = bounds.bottom;
            }
            if (bounds.posLeft < posLeft) {
                posLeft = bounds.posLeft;
            }
            if (bounds.posRight > posRight) {
                posRight = bounds.posRight;
            }
            if (bounds.negLeft < negLeft) {
                negLeft = bounds.negLeft;
            }
            if (bounds.negRight > negRight) {
                negRight = bounds.negRight;
            }
        }
        return makeInternalBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, getMetadata());
    }

    protected abstract InternalBounds<T> makeInternalBounds(
        String name,
        double top,
        double bottom,
        double posLeft,
        double posRight,
        double negLeft,
        double negRight,
        Map<String, Object> metadata
    );

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            BoundingBox<T> bbox = resolveBoundingBox();
            String bBoxSide = path.get(0);
            return switch (bBoxSide) {
                case "top" -> bbox.top();
                case "left" -> bbox.left();
                case "bottom" -> bbox.bottom();
                case "right" -> bbox.right();
                default -> throw new IllegalArgumentException("Found unknown path element [" + bBoxSide + "] in [" + getName() + "]");
            };
        } else if (path.size() == 2) {
            BoundingBox<T> bbox = resolveBoundingBox();
            T cornerPoint = null;
            String cornerString = path.get(0);
            cornerPoint = switch (cornerString) {
                case "top_left" -> bbox.topLeft();
                case "bottom_right" -> bbox.bottomRight();
                default -> throw new IllegalArgumentException("Found unknown path element [" + cornerString + "] in [" + getName() + "]");
            };
            return selectCoordinate(path.get(1), cornerPoint);
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    protected abstract Object selectCoordinate(String coordinateString, T cornerPoint);

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        BoundingBox<T> bbox = resolveBoundingBox();
        if (bbox != null) {
            builder.startObject(GeoBoundingBox.BOUNDS_FIELD.getPreferredName());
            bbox.toXContentFragment(builder);
            builder.endObject();
        }
        return builder;
    }

    protected abstract BoundingBox<T> resolveBoundingBox();

    @Override
    public T topLeft() {
        BoundingBox<T> bbox = resolveBoundingBox();
        if (bbox == null) {
            return null;
        } else {
            return bbox.topLeft();
        }
    }

    @Override
    public T bottomRight() {
        BoundingBox<T> bbox = resolveBoundingBox();
        if (bbox == null) {
            return null;
        } else {
            return bbox.bottomRight();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBounds<?> other = (InternalBounds<?>) obj;
        return top == other.top
            && bottom == other.bottom
            && posLeft == other.posLeft
            && posRight == other.posRight
            && negLeft == other.negLeft
            && negRight == other.negRight;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bottom, posLeft, posRight, negLeft, negRight);
    }
}
