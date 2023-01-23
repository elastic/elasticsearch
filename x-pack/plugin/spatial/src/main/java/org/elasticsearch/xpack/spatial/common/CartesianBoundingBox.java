/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * A class representing a Cartesian-Bounding-Box for use by Geo queries and aggregations
 * that deal with extents/rectangles representing rectangular areas of interest.
 */
public class CartesianBoundingBox extends BoundingBox<CartesianPoint> {
    public static final ParseField X_FIELD = new ParseField("x");
    public static final ParseField Y_FIELD = new ParseField("y");

    public CartesianBoundingBox(CartesianPoint topLeft, CartesianPoint bottomRight) {
        super(topLeft, bottomRight);
    }

    public CartesianBoundingBox(StreamInput input) throws IOException {
        super(new CartesianPoint(input.readDouble(), input.readDouble()), new CartesianPoint(input.readDouble(), input.readDouble()));
    }

    @Override
    public XContentBuilder toXContentFragment(XContentBuilder builder) throws IOException {
        builder.startObject(TOP_LEFT_FIELD.getPreferredName());
        builder.field(X_FIELD.getPreferredName(), topLeft.getX());
        builder.field(Y_FIELD.getPreferredName(), topLeft.getY());
        builder.endObject();
        builder.startObject(BOTTOM_RIGHT_FIELD.getPreferredName());
        builder.field(X_FIELD.getPreferredName(), bottomRight.getX());
        builder.field(Y_FIELD.getPreferredName(), bottomRight.getY());
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(topLeft.getX());
        out.writeDouble(topLeft.getY());
        out.writeDouble(bottomRight.getX());
        out.writeDouble(bottomRight.getY());
    }

    protected static class CartesianBoundsParser extends BoundsParser<CartesianBoundingBox> {
        CartesianBoundsParser(XContentParser parser) {
            super(parser);
        }

        @Override
        protected CartesianBoundingBox createWithEnvelope() {
            CartesianPoint topLeft = new CartesianPoint(envelope.getMinLon(), envelope.getMaxLat());
            CartesianPoint bottomRight = new CartesianPoint(envelope.getMaxLon(), envelope.getMinLat());
            return new CartesianBoundingBox(topLeft, bottomRight);
        }

        @Override
        protected CartesianBoundingBox createWithBounds() {
            CartesianPoint topLeft = new CartesianPoint(left, top);
            CartesianPoint bottomRight = new CartesianPoint(right, bottom);
            return new CartesianBoundingBox(topLeft, bottomRight);
        }

        @Override
        protected SpatialPoint parsePointWith(XContentParser parser, GeoUtils.EffectivePoint effectivePoint) throws IOException {
            return CartesianPoint.parsePoint(parser, false);
        }
    }

    /**
     * Parses the bounding box and returns bottom, top, left, right coordinates
     */
    public static CartesianBoundingBox parseBoundingBox(XContentParser parser) throws IOException, ElasticsearchParseException {
        CartesianBoundsParser bounds = new CartesianBoundsParser(parser);
        return bounds.parseBoundingBox();
    }
}
