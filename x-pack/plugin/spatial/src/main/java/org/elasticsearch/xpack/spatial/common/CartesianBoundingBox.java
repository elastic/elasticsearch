/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;

/**
 * A class representing a Geo-Bounding-Box for use by Geo queries and aggregations
 * that deal with extents/rectangles representing rectangular areas of interest.
 */
public class CartesianBoundingBox extends BoundingBox<CartesianPoint> {
    static final ParseField TOP_RIGHT_FIELD = new ParseField("top_right");
    static final ParseField BOTTOM_LEFT_FIELD = new ParseField("bottom_left");
    static final ParseField TOP_FIELD = new ParseField("top");
    static final ParseField BOTTOM_FIELD = new ParseField("bottom");
    static final ParseField LEFT_FIELD = new ParseField("left");
    static final ParseField RIGHT_FIELD = new ParseField("right");
    static final ParseField WKT_FIELD = new ParseField("wkt");
    public static final ParseField BOUNDS_FIELD = new ParseField("bounds");
    public static final ParseField LAT_FIELD = new ParseField("y");
    public static final ParseField LON_FIELD = new ParseField("x");
    public static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    public static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");

    public CartesianBoundingBox(CartesianPoint topLeft, CartesianPoint bottomRight) {
        super(topLeft, bottomRight);
    }

    public CartesianBoundingBox(StreamInput input) throws IOException {
        super(new CartesianPoint(input.readDouble(), input.readDouble()), new CartesianPoint(input.readDouble(), input.readDouble()));
    }

    @Override
    public double top() {
        return topLeft.getY();
    }

    @Override
    public double bottom() {
        return bottomRight.getY();
    }

    @Override
    public double left() {
        return topLeft.getX();
    }

    @Override
    public double right() {
        return bottomRight.getX();
    }

    @Override
    public XContentBuilder toXContentFragment(XContentBuilder builder, boolean buildLatLonFields) throws IOException {
        if (buildLatLonFields) {
            builder.startObject(TOP_LEFT_FIELD.getPreferredName());
            builder.field(LAT_FIELD.getPreferredName(), topLeft.getY());
            builder.field(LON_FIELD.getPreferredName(), topLeft.getX());
            builder.endObject();
        } else {
            builder.array(TOP_LEFT_FIELD.getPreferredName(), topLeft.getX(), topLeft.getY());
        }
        if (buildLatLonFields) {
            builder.startObject(BOTTOM_RIGHT_FIELD.getPreferredName());
            builder.field(LAT_FIELD.getPreferredName(), bottomRight.getY());
            builder.field(LON_FIELD.getPreferredName(), bottomRight.getX());
            builder.endObject();
        } else {
            builder.array(BOTTOM_RIGHT_FIELD.getPreferredName(), bottomRight.getX(), bottomRight.getY());
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(topLeft.getX());
        out.writeDouble(topLeft.getY());
        out.writeDouble(bottomRight.getX());
        out.writeDouble(bottomRight.getY());
    }

    /**
     * Parses the bounding box and returns bottom, top, left, right coordinates
     */
    public static CartesianBoundingBox parseBoundingBox(XContentParser parser) throws IOException, ElasticsearchParseException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse bounding box. Expected start object but found [{}]", token);
        }

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;

        String currentFieldName;
        Rectangle envelope = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                parser.nextToken();
                if (WKT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, parser.text());
                        if (ShapeType.ENVELOPE.equals(geometry.type()) == false) {
                            throw new ElasticsearchParseException(
                                "failed to parse WKT bounding box. [" + geometry.type() + "] found. expected [" + ShapeType.ENVELOPE + "]"
                            );
                        }
                        envelope = (Rectangle) geometry;
                    } catch (ParseException | IllegalArgumentException e) {
                        throw new ElasticsearchParseException("failed to parse WKT bounding box", e);
                    }
                } else if (TOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    top = parser.doubleValue();
                } else if (BOTTOM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    bottom = parser.doubleValue();
                } else if (LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    left = parser.doubleValue();
                } else if (RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    right = parser.doubleValue();
                } else {
                    CartesianPoint sparse = CartesianPoint.parsePoint(parser, false);
                    if (TOP_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        top = sparse.getY();
                        left = sparse.getX();
                    } else if (BOTTOM_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        bottom = sparse.getY();
                        right = sparse.getX();
                    } else if (TOP_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        top = sparse.getY();
                        right = sparse.getX();
                    } else if (BOTTOM_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        bottom = sparse.getY();
                        left = sparse.getX();
                    } else {
                        throw new ElasticsearchParseException("failed to parse bounding box. unexpected field [{}]", currentFieldName);
                    }
                }
            } else {
                throw new ElasticsearchParseException("failed to parse bounding box. field name expected but [{}] found", token);
            }
        }
        if (envelope != null) {
            if (Double.isNaN(top) == false
                || Double.isNaN(bottom) == false
                || Double.isNaN(left) == false
                || Double.isNaN(right) == false) {
                throw new ElasticsearchParseException(
                    "failed to parse bounding box. Conflicting definition found " + "using well-known text and explicit corners."
                );
            }
            CartesianPoint topLeft = new CartesianPoint(envelope.getMinLon(), envelope.getMaxLat());
            CartesianPoint bottomRight = new CartesianPoint(envelope.getMaxLon(), envelope.getMinLat());
            return new CartesianBoundingBox(topLeft, bottomRight);
        }
        CartesianPoint topLeft = new CartesianPoint(left, top);
        CartesianPoint bottomRight = new CartesianPoint(right, bottom);
        return new CartesianBoundingBox(topLeft, bottomRight);
    }
}
