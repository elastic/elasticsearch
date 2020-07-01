/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalBounds extends InternalAggregation implements Bounds {

    public final CartesianBoundingBox box;

    public InternalBounds(String name, float top, float bottom, float posLeft, float posRight, Map<String, Object> metadata) {
        super(name, metadata);
        CartesianPoint topLeft = new CartesianPoint(posLeft, top);
        CartesianPoint bottomRight = new CartesianPoint(posRight, bottom);
        this.box = new CartesianBoundingBox(topLeft, bottomRight);
    }

    /**
     * Read from a stream.
     */
    public InternalBounds(StreamInput in) throws IOException {
        super(in);
        this.box = new CartesianBoundingBox(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        box.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return BoundsAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        float top = Float.NEGATIVE_INFINITY;
        float bottom = Float.POSITIVE_INFINITY;
        float posLeft = Float.POSITIVE_INFINITY;
        float posRight = Float.NEGATIVE_INFINITY;

        for (InternalAggregation aggregation : aggregations) {
            InternalBounds bounds = (InternalBounds) aggregation;

            if (box.isUnbounded()) {
                continue;
            }
            if (bounds.box.top() > top) {
                top = bounds.box.top();
            }
            if (bounds.box.bottom() < bottom) {
                bottom = bounds.box.bottom();
            }
            if (bounds.box.left() < posLeft) {
                posLeft = bounds.box.left();
            }
            if (bounds.box.right() > posRight) {
                posRight = bounds.box.right();
            }
        }
        return new InternalBounds(name, top, bottom, posLeft, posRight, getMetadata());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String bBoxSide = path.get(0);
            switch (bBoxSide) {
            case "top":
                return box.top();
            case "left":
                return box.left();
            case "bottom":
                return box.bottom();
            case "right":
                return box.right();
            default:
                throw new IllegalArgumentException("Found unknown path element [" + bBoxSide + "] in [" + getName() + "]");
            }
        } else if (path.size() == 2) {
            CartesianPoint cornerPoint;
            String cornerString = path.get(0);
            switch (cornerString) {
            case "top_left":
                cornerPoint = box.topLeft();
                break;
            case "bottom_right":
                cornerPoint = box.bottomRight();
                break;
            default:
                throw new IllegalArgumentException("Found unknown path element [" + cornerString + "] in [" + getName() + "]");
            }
            String xyString = path.get(1);
            switch (xyString) {
            case "x":
                return cornerPoint.getX();
            case "y":
                return cornerPoint.getY();
            default:
                throw new IllegalArgumentException("Found unknown path element [" + xyString + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (box.isUnbounded() == false) {
            box.toXContent(builder, params);
        }
        return builder;
    }


    @Override
    public CartesianPoint topLeft() {
        if (box.isUnbounded()) {
            return null;
        }
        return box.topLeft();
    }

    @Override
    public CartesianPoint bottomRight() {
        if (box.isUnbounded()) {
            return null;
        }
        return box.bottomRight();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBounds other = (InternalBounds) obj;
        return box.equals(other.box);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), box);
    }
}
