/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.elasticsearch.xcontent.support.MapXContentParser;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a point in the cartesian space.
 */
public class CartesianPoint implements ToXContentFragment {

    private static final ParseField X_FIELD = new ParseField("x");
    private static final ParseField Y_FIELD = new ParseField("y");
    private static final ParseField Z_FIELD = new ParseField("z");

    protected double x;
    protected double y;

    public CartesianPoint() {}

    public CartesianPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public CartesianPoint reset(double x, double y) {
        this.x = x;
        this.y = y;
        return this;
    }

    public CartesianPoint resetFromString(String value, final boolean ignoreZValue) {
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
            return resetFromWKT(value, ignoreZValue);
        } else {
            return resetFromCoordinates(value, ignoreZValue);
        }
    }

    @SuppressWarnings("HiddenField")
    public CartesianPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] vals = value.split(",");
        if (vals.length > 3 || vals.length < 2) {
            throw new ElasticsearchParseException(
                "failed to parse [{}], expected 2 or 3 coordinates " + "but found: [{}]",
                vals,
                vals.length
            );
        }
        final double x;
        final double y;
        try {
            x = Double.parseDouble(vals[0].trim());
            if (Double.isFinite(x) == false) {
                throw new ElasticsearchParseException(
                    "invalid [{}] value [{}]; " + "must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                    X_FIELD.getPreferredName(),
                    x
                );
            }
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("[{}]] must be a number", X_FIELD.getPreferredName());
        }
        try {
            y = Double.parseDouble(vals[1].trim());
            if (Double.isFinite(y) == false) {
                throw new ElasticsearchParseException(
                    "invalid [{}] value [{}]; " + "must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                    Y_FIELD.getPreferredName(),
                    y
                );
            }
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("[{}]] must be a number", Y_FIELD.getPreferredName());
        }
        if (vals.length > 2) {
            try {
                CartesianPoint.assertZValue(ignoreZValue, Double.parseDouble(vals[2].trim()));
            } catch (NumberFormatException ex) {
                throw new ElasticsearchParseException("[{}]] must be a number", Y_FIELD.getPreferredName());
            }
        }
        return reset(x, y);
    }

    private CartesianPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = WellKnownText.fromWKT(StandardValidator.instance(ignoreZValue), false, value);
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid WKT format", e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new ElasticsearchParseException(
                "[{}] supports only POINT among WKT primitives, " + "but found {}",
                PointFieldMapper.CONTENT_TYPE,
                geometry.type()
            );
        }
        org.elasticsearch.geometry.Point point = (org.elasticsearch.geometry.Point) geometry;
        return reset(point.getX(), point.getY());
    }

    public double getX() {
        return this.x;
    }

    public double getY() {
        return this.y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CartesianPoint point = (CartesianPoint) o;

        if (Double.compare(point.x, x) != 0) return false;
        if (Double.compare(point.y, y) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }

    @Override
    public String toString() {
        return x + ", " + y;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(X_FIELD.getPreferredName(), x).field(Y_FIELD.getPreferredName(), y).endObject();
    }

    public static CartesianPoint parsePoint(XContentParser parser, CartesianPoint point, boolean ignoreZvalue) throws IOException,
        ElasticsearchParseException {
        double x = Double.NaN;
        double y = Double.NaN;
        NumberFormatException numberFormatException = null;

        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                while (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (subParser.currentToken() == XContentParser.Token.FIELD_NAME) {
                        String field = subParser.currentName();
                        if (field.equals(X_FIELD.getPreferredName())) {
                            subParser.nextToken();
                            switch (subParser.currentToken()) {
                                case VALUE_NUMBER:
                                case VALUE_STRING:
                                    try {
                                        x = subParser.doubleValue(true);
                                    } catch (NumberFormatException e) {
                                        numberFormatException = e;
                                    }
                                    break;
                                default:
                                    throw new ElasticsearchParseException("[{}] must be a number", X_FIELD.getPreferredName());
                            }
                        } else if (field.equals(Y_FIELD.getPreferredName())) {
                            subParser.nextToken();
                            switch (subParser.currentToken()) {
                                case VALUE_NUMBER:
                                case VALUE_STRING:
                                    try {
                                        y = subParser.doubleValue(true);
                                    } catch (NumberFormatException e) {
                                        numberFormatException = e;
                                    }
                                    break;
                                default:
                                    throw new ElasticsearchParseException("[{}] must be a number", Y_FIELD.getPreferredName());
                            }
                        } else if (field.equals(Z_FIELD.getPreferredName())) {
                            subParser.nextToken();
                            switch (subParser.currentToken()) {
                                case VALUE_NUMBER:
                                case VALUE_STRING:
                                    try {
                                        CartesianPoint.assertZValue(ignoreZvalue, subParser.doubleValue(true));
                                    } catch (NumberFormatException e) {
                                        numberFormatException = e;
                                    }
                                    break;
                                default:
                                    throw new ElasticsearchParseException("[{}] must be a number", Z_FIELD.getPreferredName());
                            }
                        } else {
                            throw new ElasticsearchParseException(
                                "field must be either [{}] or [{}]",
                                X_FIELD.getPreferredName(),
                                Y_FIELD.getPreferredName()
                            );
                        }
                    } else {
                        throw new ElasticsearchParseException("token [{}] not allowed", subParser.currentToken());
                    }
                }
            }
            if (numberFormatException != null) {
                throw new ElasticsearchParseException(
                    "[{}] and [{}] must be valid double values",
                    numberFormatException,
                    X_FIELD.getPreferredName(),
                    Y_FIELD.getPreferredName()
                );
            } else if (Double.isNaN(x)) {
                throw new ElasticsearchParseException("field [{}] missing", X_FIELD.getPreferredName());
            } else if (Double.isNaN(y)) {
                throw new ElasticsearchParseException("field [{}] missing", Y_FIELD.getPreferredName());
            } else {
                return point.reset(x, y);
            }

        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                int element = 0;
                while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (subParser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                        element++;
                        if (element == 1) {
                            x = subParser.doubleValue();
                        } else if (element == 2) {
                            y = subParser.doubleValue();
                        } else {
                            throw new ElasticsearchParseException(
                                "[{}}] field type does not accept > 2 dimensions",
                                PointFieldMapper.CONTENT_TYPE
                            );
                        }
                    } else {
                        throw new ElasticsearchParseException("numeric value expected");
                    }
                }
            }
            return point.reset(x, y);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            String val = parser.text();
            return point.resetFromString(val, ignoreZvalue);
        } else {
            throw new ElasticsearchParseException("point expected");
        }
    }

    public static CartesianPoint parsePoint(Object value, boolean ignoreZValue) throws ElasticsearchParseException {
        return parsePoint(value, new CartesianPoint(), ignoreZValue);
    }

    public static CartesianPoint parsePoint(Object value, CartesianPoint point, boolean ignoreZValue) throws ElasticsearchParseException {
        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("null_value", value),
                null
            )
        ) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parsePoint(parser, point, ignoreZValue);
        } catch (IOException ex) {
            throw new ElasticsearchParseException("error parsing point", ex);
        }
    }

    public static double assertZValue(final boolean ignoreZValue, double zValue) {
        if (ignoreZValue == false) {
            throw new ElasticsearchParseException(
                "Exception parsing coordinates: found Z value [{}] but [ignore_z_value] " + "parameter is [{}]",
                zValue,
                ignoreZValue
            );
        }
        if (Double.isFinite(zValue) == false) {
            throw new ElasticsearchParseException(
                "invalid [{}] value [{}]; " + "must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                Z_FIELD.getPreferredName(),
                zValue
            );
        }
        return zValue;
    }
}
