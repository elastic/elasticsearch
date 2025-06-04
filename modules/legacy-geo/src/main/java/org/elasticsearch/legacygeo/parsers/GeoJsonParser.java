/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.legacygeo.parsers;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.builders.CircleBuilder;
import org.elasticsearch.legacygeo.builders.GeometryCollectionBuilder;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses shape geometry represented in geojson
 *
 * complies with geojson specification: https://tools.ietf.org/html/rfc7946
 */
abstract class GeoJsonParser {
    protected static ShapeBuilder<?, ?, ?> parse(XContentParser parser, AbstractShapeGeometryFieldMapper<?> shapeMapper)
        throws IOException {
        GeoShapeType shapeType = null;
        DistanceUnit.Distance radius = null;
        CoordinateNode coordinateNode = null;
        GeometryCollectionBuilder geometryCollections = null;

        Orientation orientation = (shapeMapper == null) ? Orientation.RIGHT : shapeMapper.orientation();
        boolean coerce = shapeMapper != null && shapeMapper.coerce();
        boolean ignoreZValue = shapeMapper == null || shapeMapper.ignoreZValue();

        String malformedException = null;

        XContentParser.Token token;
        try (XContentParser subParser = new XContentSubParser(parser)) {
            while ((token = subParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = subParser.currentName();

                    if (ShapeParser.FIELD_TYPE.match(fieldName, subParser.getDeprecationHandler())) {
                        subParser.nextToken();
                        final GeoShapeType type = GeoShapeType.forName(subParser.text());
                        if (shapeType != null && shapeType.equals(type) == false) {
                            malformedException = ShapeParser.FIELD_TYPE
                                + " already parsed as ["
                                + shapeType
                                + "] cannot redefine as ["
                                + type
                                + "]";
                        } else {
                            shapeType = type;
                        }
                    } else if (ShapeParser.FIELD_COORDINATES.match(fieldName, subParser.getDeprecationHandler())) {
                        subParser.nextToken();
                        CoordinateNode tempNode = parseCoordinates(subParser, ignoreZValue);
                        if (coordinateNode != null && tempNode.numDimensions() != coordinateNode.numDimensions()) {
                            throw new ElasticsearchParseException("Exception parsing coordinates: " + "number of dimensions do not match");
                        }
                        coordinateNode = tempNode;
                    } else if (ShapeParser.FIELD_GEOMETRIES.match(fieldName, subParser.getDeprecationHandler())) {
                        if (shapeType == null) {
                            shapeType = GeoShapeType.GEOMETRYCOLLECTION;
                        } else if (shapeType.equals(GeoShapeType.GEOMETRYCOLLECTION) == false) {
                            malformedException = "cannot have [" + ShapeParser.FIELD_GEOMETRIES + "] with type set to [" + shapeType + "]";
                        }
                        subParser.nextToken();
                        geometryCollections = parseGeometries(subParser, shapeMapper);
                    } else if (CircleBuilder.FIELD_RADIUS.match(fieldName, subParser.getDeprecationHandler())) {
                        if (shapeType == null) {
                            shapeType = GeoShapeType.CIRCLE;
                        } else if (shapeType.equals(GeoShapeType.CIRCLE) == false) {
                            malformedException = "cannot have [" + CircleBuilder.FIELD_RADIUS + "] with type set to [" + shapeType + "]";
                        }
                        subParser.nextToken();
                        radius = DistanceUnit.Distance.parseDistance(subParser.text());
                    } else if (ShapeParser.FIELD_ORIENTATION.match(fieldName, subParser.getDeprecationHandler())) {
                        if (shapeType != null
                            && (shapeType.equals(GeoShapeType.POLYGON) || shapeType.equals(GeoShapeType.MULTIPOLYGON)) == false) {
                            malformedException = "cannot have [" + ShapeParser.FIELD_ORIENTATION + "] with type set to [" + shapeType + "]";
                        }
                        subParser.nextToken();
                        orientation = Orientation.fromString(subParser.text());
                    } else {
                        subParser.nextToken();
                        subParser.skipChildren();
                    }
                }
            }
        }

        if (malformedException != null) {
            throw new ElasticsearchParseException(malformedException);
        } else if (shapeType == null) {
            throw new ElasticsearchParseException("shape type not included");
        } else if (coordinateNode == null && GeoShapeType.GEOMETRYCOLLECTION != shapeType) {
            throw new ElasticsearchParseException("coordinates not included");
        } else if (geometryCollections == null && GeoShapeType.GEOMETRYCOLLECTION == shapeType) {
            throw new ElasticsearchParseException("geometries not included");
        } else if (radius != null && GeoShapeType.CIRCLE != shapeType) {
            throw new ElasticsearchParseException("field [{}] is supported for [{}] only", CircleBuilder.FIELD_RADIUS, CircleBuilder.TYPE);
        }

        if (shapeType.equals(GeoShapeType.GEOMETRYCOLLECTION)) {
            return geometryCollections;
        }

        return shapeType.getBuilder(coordinateNode, radius, orientation, coerce);
    }

    /**
     * Recursive method which parses the arrays of coordinates used to define
     * Shapes
     *
     * @param parser
     *            Parser that will be read from
     * @return CoordinateNode representing the start of the coordinate tree
     * @throws IOException
     *             Thrown if an error occurs while reading from the
     *             XContentParser
     */
    private static CoordinateNode parseCoordinates(XContentParser parser, boolean ignoreZValue) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.skipChildren();
            parser.nextToken();
            throw new ElasticsearchParseException("coordinates cannot be specified as objects");
        }

        XContentParser.Token token = parser.nextToken();
        // Base cases
        if (token != XContentParser.Token.START_ARRAY
            && token != XContentParser.Token.END_ARRAY
            && token != XContentParser.Token.VALUE_NULL) {
            return new CoordinateNode(parseCoordinate(parser, ignoreZValue));
        } else if (token == XContentParser.Token.VALUE_NULL) {
            throw new IllegalArgumentException("coordinates cannot contain NULL values)");
        }

        List<CoordinateNode> nodes = new ArrayList<>();
        while (token != XContentParser.Token.END_ARRAY) {
            CoordinateNode node = parseCoordinates(parser, ignoreZValue);
            if (nodes.isEmpty() == false && nodes.get(0).numDimensions() != node.numDimensions()) {
                throw new ElasticsearchParseException("Exception parsing coordinates: number of dimensions do not match");
            }
            nodes.add(node);
            token = parser.nextToken();
        }

        return new CoordinateNode(nodes);
    }

    private static Coordinate parseCoordinate(XContentParser parser, boolean ignoreZValue) throws IOException {
        if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates must be numbers");
        }
        double lon = parser.doubleValue();
        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates must be numbers");
        }
        double lat = parser.doubleValue();
        XContentParser.Token token = parser.nextToken();
        // alt (for storing purposes only - future use includes 3d shapes)
        double alt = Double.NaN;
        if (token == XContentParser.Token.VALUE_NUMBER) {
            alt = GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
            parser.nextToken();
        }
        // do not support > 3 dimensions
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates greater than 3 dimensions are not supported");
        }
        return new Coordinate(lon, lat, alt);
    }

    /**
     * Parse the geometries array of a GeometryCollection
     *
     * @param parser Parser that will be read from
     * @return Geometry[] geometries of the GeometryCollection
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    static GeometryCollectionBuilder parseGeometries(XContentParser parser, AbstractShapeGeometryFieldMapper<?> mapper) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("geometries must be an array of geojson objects");
        }

        XContentParser.Token token = parser.nextToken();
        GeometryCollectionBuilder geometryCollection = new GeometryCollectionBuilder();
        while (token != XContentParser.Token.END_ARRAY) {
            ShapeBuilder<?, ?, ?> shapeBuilder = ShapeParser.parse(parser);
            geometryCollection.shape(shapeBuilder);
            token = parser.nextToken();
        }

        return geometryCollection;
    }
}
