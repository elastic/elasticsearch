/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.geo.GenericPointParser;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.support.MapXContentParser;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a point in the cartesian space.
 */
public class CartesianPoint implements SpatialPoint, ToXContentFragment {

    private static final String X_FIELD = "x";
    private static final String Y_FIELD = "y";
    private static final String Z_FIELD = "z";

    protected double x;
    protected double y;

    public CartesianPoint() {}

    public CartesianPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public CartesianPoint(SpatialPoint template) {
        this(template.getX(), template.getY());
    }

    public CartesianPoint reset(double x, double y) {
        this.x = x;
        this.y = y;
        return this;
    }

    public CartesianPoint resetX(double x) {
        this.x = x;
        return this;
    }

    public CartesianPoint resetY(double y) {
        this.y = y;
        return this;
    }

    public CartesianPoint resetFromEncoded(long encoded) {
        // TODO add this method to SpatialPoint interface, allowing more code de-duplication
        final double x = XYEncodingUtils.decode((int) (encoded >>> 32));
        final double y = XYEncodingUtils.decode((int) (encoded & 0xFFFFFFFF));
        return reset(x, y);
    }

    public CartesianPoint resetFromString(String value, final boolean ignoreZValue) {
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
            return resetFromWKT(value, ignoreZValue);
        } else if (value.contains(",")) {
            return resetFromCoordinates(value, ignoreZValue);
        } else if (value.contains(".")) {
            // This error mimics the structure of the parser error from 'resetFromCoordinates' below
            throw new ElasticsearchParseException("failed to parse [{}], expected 2 or 3 coordinates but found: [{}]", value, 1);
        } else {
            // This error mimics the structure of the Geohash.mortonEncode() error to simplify testing
            throw new ElasticsearchParseException("unsupported symbol [{}] in point [{}]", value.charAt(0), value);
        }
    }

    @SuppressWarnings("HiddenField")
    public CartesianPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] vals = value.split(",");
        if (vals.length > 3 || vals.length < 2) {
            throw new ElasticsearchParseException("failed to parse [{}], expected 2 or 3 coordinates but found: [{}]", vals, vals.length);
        }
        final double x;
        final double y;
        try {
            x = Double.parseDouble(vals[0].trim());
            if (Double.isFinite(x) == false) {
                throw new ElasticsearchParseException(
                    "invalid [{}] value [{}]; must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                    X_FIELD,
                    x
                );
            }
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("[{}] must be a number", X_FIELD);
        }
        try {
            y = Double.parseDouble(vals[1].trim());
            if (Double.isFinite(y) == false) {
                throw new ElasticsearchParseException(
                    "invalid [{}] value [{}]; must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                    Y_FIELD,
                    y
                );
            }
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("[{}] must be a number", Y_FIELD);
        }
        if (vals.length > 2) {
            try {
                CartesianPoint.assertZValue(ignoreZValue, Double.parseDouble(vals[2].trim()));
            } catch (NumberFormatException ex) {
                throw new ElasticsearchParseException("[{}] must be a number", Y_FIELD);
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
                "[{}] supports only POINT among WKT primitives, but found {}",
                PointFieldMapper.CONTENT_TYPE,
                geometry.type()
            );
        }
        org.elasticsearch.geometry.Point point = (org.elasticsearch.geometry.Point) geometry;
        return reset(point.getX(), point.getY());
    }

    @Override
    public double getX() {
        return this.x;
    }

    @Override
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
        return builder.startObject().field(X_FIELD, x).field(Y_FIELD, y).endObject();
    }

    /**
     * Parse a {@link CartesianPoint} with a {@link XContentParser}. A point has one of the following forms:
     *
     * <ul>
     *     <li>Object: <pre>{&quot;x&quot;: <i>&lt;x-value&gt;</i>, &quot;y&quot;: <i>&lt;y-value&gt;</i>}</pre></li>
     *     <li>Object: <pre>{&quot;type&quot;: <i>Point</i>, &quot;coordinates&quot;: <i>&lt;array of doubles&gt;</i>}</pre></li>
     *     <li>String: <pre>&quot;<i>&lt;latitude&gt;</i>,<i>&lt;longitude&gt;</i>&quot;</pre></li>
     *     <li>Array: <pre>[<i>&lt;x&gt;</i>,<i>&lt;y&gt;</i>]</pre></li>
     * </ul>
     *
     * @param parser {@link XContentParser} to parse the value from
     * @param ignoreZValue {@link XContentParser} to not throw an error if 3 dimensional data is provided
     * @return new {@link CartesianPoint} parsed from the parser
     */
    public static CartesianPoint parsePoint(XContentParser parser, final boolean ignoreZValue) throws IOException,
        ElasticsearchParseException {
        return cartesianPointParser.parsePoint(parser, ignoreZValue, value -> new CartesianPoint().resetFromString(value, ignoreZValue));
    }

    public static CartesianPoint parsePoint(Object value, boolean ignoreZValue) throws ElasticsearchParseException {
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
            return parsePoint(parser, ignoreZValue);
        } catch (IOException ex) {
            throw new ElasticsearchParseException("error parsing point", ex);
        }
    }

    public static void assertZValue(final boolean ignoreZValue, double zValue) {
        if (ignoreZValue == false) {
            throw new ElasticsearchParseException(
                "Exception parsing coordinates: found Z value [{}] but [ignore_z_value] parameter is [{}]",
                zValue,
                ignoreZValue
            );
        }
        if (Double.isFinite(zValue) == false) {
            throw new ElasticsearchParseException(
                "invalid [{}] value [{}]; must be between -3.4028234663852886E38 and 3.4028234663852886E38",
                Z_FIELD,
                zValue
            );
        }
    }

    private static final GenericPointParser<CartesianPoint> cartesianPointParser = new GenericPointParser<>("point", "x", "y") {

        @Override
        public void assertZValue(boolean ignoreZValue, double zValue) {
            CartesianPoint.assertZValue(ignoreZValue, zValue);
        }

        @Override
        public CartesianPoint createPoint(double x, double y) {
            return new CartesianPoint(x, y);
        }

        @Override
        public String fieldError() {
            return "field must be either lat/lon or type/coordinates";
        }
    };

}
