/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;

/**
 * A class representing a Bounding-Box for use by Geo and Cartesian queries and aggregations
 * that deal with extents/rectangles representing rectangular areas of interest.
 */
public abstract class BoundingBox<T extends SpatialPoint> implements ToXContentFragment, Writeable {
    static final ParseField TOP_RIGHT_FIELD = new ParseField("top_right");
    static final ParseField BOTTOM_LEFT_FIELD = new ParseField("bottom_left");
    static final ParseField TOP_FIELD = new ParseField("top");
    static final ParseField BOTTOM_FIELD = new ParseField("bottom");
    static final ParseField LEFT_FIELD = new ParseField("left");
    static final ParseField RIGHT_FIELD = new ParseField("right");
    static final ParseField WKT_FIELD = new ParseField("wkt");
    public static final ParseField BOUNDS_FIELD = new ParseField("bounds");
    public static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    public static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");
    protected final T topLeft;
    protected final T bottomRight;

    public BoundingBox(T topLeft, T bottomRight) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    public boolean isUnbounded() {
        return Double.isNaN(left()) || Double.isNaN(top()) || Double.isNaN(right()) || Double.isNaN(bottom());
    }

    public T topLeft() {
        return topLeft;
    }

    public T bottomRight() {
        return bottomRight;
    }

    public final double top() {
        return topLeft.getY();
    }

    public final double bottom() {
        return bottomRight.getY();
    }

    public final double left() {
        return topLeft.getX();
    }

    public final double right() {
        return bottomRight.getX();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder toXContentFragment(XContentBuilder builder) throws IOException;

    protected abstract static class BoundsParser<T extends BoundingBox<? extends SpatialPoint>> {
        protected double top = Double.NaN;
        protected double bottom = Double.NaN;
        protected double left = Double.NaN;
        protected double right = Double.NaN;
        protected Rectangle envelope = null;
        final XContentParser parser;

        protected BoundsParser(XContentParser parser) {
            this.parser = parser;
        }

        public T parseBoundingBox() throws IOException, ElasticsearchParseException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse bounding box. Expected start object but found [{}]", token);
            }
            String currentFieldName;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    parser.nextToken();
                    if (WKT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        try {
                            Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, parser.text());
                            if (ShapeType.ENVELOPE.equals(geometry.type()) == false) {
                                throw new ElasticsearchParseException(
                                    "failed to parse WKT bounding box. ["
                                        + geometry.type()
                                        + "] found. expected ["
                                        + ShapeType.ENVELOPE
                                        + "]"
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
                        if (TOP_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            SpatialPoint point = parsePointWith(parser, GeoUtils.EffectivePoint.TOP_LEFT);
                            this.top = point.getY();
                            this.left = point.getX();
                        } else if (BOTTOM_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            SpatialPoint point = parsePointWith(parser, GeoUtils.EffectivePoint.BOTTOM_RIGHT);
                            this.bottom = point.getY();
                            this.right = point.getX();
                        } else if (TOP_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            SpatialPoint point = parsePointWith(parser, GeoUtils.EffectivePoint.TOP_RIGHT);
                            this.top = point.getY();
                            this.right = point.getX();
                        } else if (BOTTOM_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            SpatialPoint point = parsePointWith(parser, GeoUtils.EffectivePoint.BOTTOM_LEFT);
                            this.bottom = point.getY();
                            this.left = point.getX();
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
                return createWithEnvelope();
            }
            return createWithBounds();
        }

        protected abstract T createWithEnvelope();

        protected abstract T createWithBounds();

        protected abstract SpatialPoint parsePointWith(XContentParser parser, GeoUtils.EffectivePoint effectivePoint) throws IOException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoundingBox<?> that = (BoundingBox<?>) o;
        return topLeft.equals(that.topLeft) && bottomRight.equals(that.bottomRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topLeft, bottomRight);
    }

    @Override
    public String toString() {
        return "BBOX (" + left() + ", " + right() + ", " + top() + ", " + bottom() + ")";
    }
}
