/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo;

import com.spatial4j.core.shape.MultiShape;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.RectangleImpl;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.*;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Parsers which supports reading {@link Shape}s in GeoJSON format from a given
 * {@link XContentParser}.
 * <p/>
 * An example of the format used for polygons:
 * <p/>
 * {
 * "type": "Polygon",
 * "coordinates": [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
 * [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p/>
 * Note, currently MultiPolygon and GeometryCollections are not supported
 */
public class GeoJSONShapeParser {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private GeoJSONShapeParser() {
    }

    /**
     * Parses the current object from the given {@link XContentParser}, creating
     * the {@link Shape} representation
     *
     * @param parser Parser that will be read from
     * @return Shape representation of the geojson defined Shape
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    public static Shape parse(XContentParser parser) throws IOException {
        ParseResult parseResult = parseObject(parser);
        return buildShape(parseResult);
    }

    /**
     * Parses the current object from the given {@link XContentParser}, returns a {@link ParseResult}
     * to be transformed in a {@link Shape} representation or reused in a geometry collection
     *
     * @param parser Parser that will be read from
     * @return ParseResult non-null type and node or geometries parsed from the geojson
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    public static ParseResult parseObject(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticSearchParseException("Shape must be an object consisting of type and coordinates");
        }

        ParseResult result = new ParseResult();

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();

                if ("type".equals(fieldName)) {
                    parser.nextToken();
                    result.shapeType = parser.text().toLowerCase(Locale.ENGLISH);
                } else if ("coordinates".equals(fieldName)) {
                    parser.nextToken();
                    result.node = parseCoordinates(parser);
                } else if ("geometries".equals(fieldName)) {
                    parser.nextToken();
                    result.geometries = parseGeometries(parser);
                } else {
                    parser.nextToken();
                    parser.skipChildren();
                }
            }
        }

        if (result.shapeType == null) {
            throw new ElasticSearchParseException("Shape type not included");
        }
        return result;
    }

    /**
     * Recursive method which parses the arrays of coordinates used to define Shapes
     *
     * @param parser Parser that will be read from
     * @return CoordinateNode representing the start of the coordinate tree
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    private static CoordinateNode parseCoordinates(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();

        // Base case
        if (token != XContentParser.Token.START_ARRAY) {
            double lon = parser.doubleValue();
            parser.nextToken();
            double lat = parser.doubleValue();
            parser.nextToken();
            return new CoordinateNode(new Coordinate(lon, lat));
        }

        List<CoordinateNode> nodes = new ArrayList<CoordinateNode>();
        while (token != XContentParser.Token.END_ARRAY) {
            nodes.add(parseCoordinates(parser));
            token = parser.nextToken();
        }

        return new CoordinateNode(nodes);
    }

    /**
     * Parse the geometries array of a GeometryCollection
     *
     * @param parser Parser that will be read from
     * @return Geometry[] geometries of the GeometryCollection
     * @throws IOException Thrown if an error occurs while reading from the XContentParser
     */
    private static Geometry[] parseGeometries(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticSearchParseException("Geometries must be an array of geojson objects");
        }

        XContentParser.Token token = parser.nextToken();
        List<Geometry> geometries = new ArrayList<Geometry>();
        while (token != XContentParser.Token.END_ARRAY) {
            ParseResult geometryParseResult = parseObject(parser);
            geometries.add(buildGeometry(geometryParseResult));
            token = parser.nextToken();
        }

        return geometries.toArray(new Geometry[geometries.size()]);
    }

    /**
     * Builds the actual {@link Geometry} with the given shape type from the tree
     * of coordinates
     *
     * @param parseResult parsed data to create the geometry from.
     * @return Geometry built from the coordinates
     */
    private static Geometry buildGeometry(ParseResult parseResult) {
        String shapeType = parseResult.shapeType;
        if ("geometrycollection".equals(shapeType)) {
            if (parseResult.geometries == null) {
                throw new ElasticSearchParseException("Geometries not included");
            }
            return GEOMETRY_FACTORY.createGeometryCollection(parseResult.geometries);
        } else {
            CoordinateNode node = parseResult.node;
            if (node == null) {
                throw new ElasticSearchParseException("Coordinates not included");
            }
            if ("point".equals(shapeType)) {
                return GEOMETRY_FACTORY.createPoint(node.coordinate);
            } else if ("linestring".equals(shapeType)) {
                return GEOMETRY_FACTORY.createLineString(toCoordinates(node));
            } else if ("polygon".equals(shapeType)) {
                return buildPolygon(node);
            } else if ("multipoint".equals(shapeType)) {
                return GEOMETRY_FACTORY.createMultiPoint(toCoordinates(node));
            } else if ("multilinestring".equals(shapeType)) {
                LineString[] linestrings = new LineString[node.children.size()];
                for (int i = 0; i < node.children.size(); i++) {
                    linestrings[i] = GEOMETRY_FACTORY.createLineString(toCoordinates(node.children.get(i)));
                }
                return GEOMETRY_FACTORY.createMultiLineString(linestrings);
            } else if ("multipolygon".equals(shapeType)) {
                Polygon[] polygons = new Polygon[node.children.size()];
                for (int i = 0; i < node.children.size(); i++) {
                    polygons[i] = buildPolygon(node.children.get(i));
                }
                return GEOMETRY_FACTORY.createMultiPolygon(polygons);
            }
        }

        throw new UnsupportedOperationException("ShapeType [" + shapeType + "] not supported");
    }

    /**
     * Builds the actual {@link Shape} with the given shape type from the tree
     * of coordinates
     *
     * @param parseResult parsed data to create the shape from.
     * @return Shape built from the coordinates
     */
    private static Shape buildShape(ParseResult parseResult) {
        if ("envelope".equals(parseResult.shapeType)) {
            Coordinate[] coordinates = toCoordinates(parseResult.node);
            return new RectangleImpl(coordinates[0].x, coordinates[1].x, coordinates[1].y, coordinates[0].y, GeoShapeConstants.SPATIAL_CONTEXT);
        }

        return convertGeometryToShape(buildGeometry(parseResult));
    }

    /**
     * Convert a JTS {@link Geometry} to a {@link Shape}
     *
     * @param geometry The JTS geometry
     * @return Shape built from the geometry
     */
    private static Shape convertGeometryToShape(Geometry geometry) {
        if (geometry instanceof Point) {
            return new JtsPoint((Point) geometry, GeoShapeConstants.SPATIAL_CONTEXT);
        } else if (GeometryCollection.class.equals(geometry.getClass())) {
            //JtsGeometry does not support GeometryCollection but does support its subclasses.
            // we use MultiShape instead
            GeometryCollection geometryCollection = (GeometryCollection) geometry;
            List<Shape> shapes = new ArrayList<Shape>();
            for (int geometryIndex = 0; geometryIndex < geometryCollection.getNumGeometries(); ++geometryIndex) {
                Geometry geometryInCollection = geometryCollection.getGeometryN(geometryIndex);
                shapes.add(convertGeometryToShape(geometryInCollection));
            }
            return new MultiShape(shapes, GeoShapeConstants.SPATIAL_CONTEXT);
        } else {
            return new JtsGeometry(geometry, GeoShapeConstants.SPATIAL_CONTEXT, true);
        }
    }

    /**
     * Builds a {@link Polygon} from the given CoordinateNode
     *
     * @param node CoordinateNode that the Polygon will be built from
     * @return Polygon consisting of the coordinates in the CoordinateNode
     */
    private static Polygon buildPolygon(CoordinateNode node) {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(toCoordinates(node.children.get(0)));
        LinearRing[] holes = null;
        if (node.children.size() > 1) {
            holes = new LinearRing[node.children.size() - 1];
            for (int i = 0; i < node.children.size() - 1; i++) {
                holes[i] = GEOMETRY_FACTORY.createLinearRing(toCoordinates(node.children.get(i + 1)));
            }
        }
        return GEOMETRY_FACTORY.createPolygon(shell, holes);
    }

    /**
     * Converts the children of the given CoordinateNode into an array of
     * {@link Coordinate}.
     *
     * @param node CoordinateNode whose children will be converted
     * @return Coordinate array with the values taken from the children of the Node
     */
    private static Coordinate[] toCoordinates(CoordinateNode node) {
        Coordinate[] coordinates = new Coordinate[node.children.size()];
        for (int i = 0; i < node.children.size(); i++) {
            coordinates[i] = node.children.get(i).coordinate;
        }
        return coordinates;
    }

    /**
     * Node used to represent a tree of coordinates.
     * <p/>
     * Can either be a leaf node consisting of a Coordinate, or a parent with children
     */
    private static class CoordinateNode {

        private Coordinate coordinate;
        private List<CoordinateNode> children;

        /**
         * Creates a new leaf CoordinateNode
         *
         * @param coordinate Coordinate for the Node
         */
        private CoordinateNode(Coordinate coordinate) {
            this.coordinate = coordinate;
        }

        /**
         * Creates a new parent CoordinateNode
         *
         * @param children Children of the Node
         */
        private CoordinateNode(List<CoordinateNode> children) {
            this.children = children;
        }
    }

    /**
     * Result of parsed json object, to be transformed in a geometry or shape
     */
    private static class ParseResult {
        /**
         * shapeType Type of Shape to be built
         */
        private String shapeType;
        /**
         * Root node of the coordinate tree
         */
        private CoordinateNode node;
        /**
         * Geometries of a GeometryCollection object
         */
        private Geometry[] geometries;
    }
}
