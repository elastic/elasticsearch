/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalGeoBounds extends InternalAggregation implements GeoBounds {
    public final double top;
    public final double bottom;
    public final double posLeft;
    public final double posRight;
    public final double negLeft;
    public final double negRight;
    public final boolean wrapLongitude;

    public InternalGeoBounds(String name, double top, double bottom, double posLeft, double posRight,
                             double negLeft, double negRight, boolean wrapLongitude, Map<String, Object> metadata) {
        super(name, metadata);
        this.top = top;
        this.bottom = bottom;
        this.posLeft = posLeft;
        this.posRight = posRight;
        this.negLeft = negLeft;
        this.negRight = negRight;
        this.wrapLongitude = wrapLongitude;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoBounds(StreamInput in) throws IOException {
        super(in);
        top = in.readDouble();
        bottom = in.readDouble();
        posLeft = in.readDouble();
        posRight = in.readDouble();
        negLeft = in.readDouble();
        negRight = in.readDouble();
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(top);
        out.writeDouble(bottom);
        out.writeDouble(posLeft);
        out.writeDouble(posRight);
        out.writeDouble(negLeft);
        out.writeDouble(negRight);
        out.writeBoolean(wrapLongitude);
    }

    @Override
    public String getWriteableName() {
        return GeoBoundsAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;

        for (InternalAggregation aggregation : aggregations) {
            InternalGeoBounds bounds = (InternalGeoBounds) aggregation;

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
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, getMetadata());
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
            GeoBoundingBox geoBoundingBox = resolveGeoBoundingBox();
            String bBoxSide = path.get(0);
            switch (bBoxSide) {
            case "top":
                return geoBoundingBox.top();
            case "left":
                return geoBoundingBox.left();
            case "bottom":
                return geoBoundingBox.bottom();
            case "right":
                return geoBoundingBox.right();
            default:
                throw new IllegalArgumentException("Found unknown path element [" + bBoxSide + "] in [" + getName() + "]");
            }
        } else if (path.size() == 2) {
            GeoBoundingBox geoBoundingBox = resolveGeoBoundingBox();
            GeoPoint cornerPoint = null;
            String cornerString = path.get(0);
            switch (cornerString) {
            case "top_left":
                cornerPoint = geoBoundingBox.topLeft();
                break;
            case "bottom_right":
                cornerPoint = geoBoundingBox.bottomRight();
                break;
            default:
                throw new IllegalArgumentException("Found unknown path element [" + cornerString + "] in [" + getName() + "]");
            }
            String latLonString = path.get(1);
            switch (latLonString) {
            case "lat":
                return cornerPoint.lat();
            case "lon":
                return cornerPoint.lon();
            default:
                throw new IllegalArgumentException("Found unknown path element [" + latLonString + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        GeoBoundingBox bbox = resolveGeoBoundingBox();
        if (bbox != null) {
            builder.startObject(GeoBoundingBox.BOUNDS_FIELD.getPreferredName());
            bbox.toXContentFragment(builder, true);
            builder.endObject();
        }
        return builder;
    }

    private GeoBoundingBox resolveGeoBoundingBox() {
        if (Double.isInfinite(top)) {
            return null;
        } else if (Double.isInfinite(posLeft)) {
            return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, negRight));
        } else if (Double.isInfinite(negLeft)) {
            return new GeoBoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, posRight));
        } else if (wrapLongitude) {
            double unwrappedWidth = posRight - negLeft;
            double wrappedWidth = (180 - posLeft) - (-180 - negRight);
            if (unwrappedWidth <= wrappedWidth) {
                return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
            } else {
                return new GeoBoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, negRight));
            }
        } else {
            return new GeoBoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
        }
    }

    @Override
    public GeoPoint topLeft() {
        GeoBoundingBox geoBoundingBox = resolveGeoBoundingBox();
        if (geoBoundingBox == null) {
            return null;
        } else {
            return geoBoundingBox.topLeft();
        }
    }

    @Override
    public GeoPoint bottomRight() {
        GeoBoundingBox geoBoundingBox = resolveGeoBoundingBox();
        if (geoBoundingBox == null) {
            return null;
        } else {
            return geoBoundingBox.bottomRight();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalGeoBounds other = (InternalGeoBounds) obj;
        return top == other.top &&
            bottom == other.bottom &&
            posLeft == other.posLeft &&
            posRight == other.posRight &&
            negLeft == other.negLeft &&
            negRight == other.negRight &&
            wrapLongitude == other.wrapLongitude;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bottom, posLeft, posRight, negLeft, negRight, wrapLongitude);
    }
}
