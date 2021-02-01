/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.GeometryValidator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Utility class for converting libs/geo shapes to and from GeoJson
 */
public final class GeoJson {

    private static final ParseField FIELD_TYPE = new ParseField("type");
    private static final ParseField FIELD_COORDINATES = new ParseField("coordinates");
    private static final ParseField FIELD_GEOMETRIES = new ParseField("geometries");
    private static final ParseField FIELD_ORIENTATION = new ParseField("orientation");
    private static final ParseField FIELD_RADIUS = new ParseField("radius");

    private final boolean rightOrientation;
    private final boolean coerce;
    private final GeometryValidator validator;

    public GeoJson(boolean rightOrientation, boolean coerce, GeometryValidator validator) {
        this.rightOrientation = rightOrientation;
        this.coerce = coerce;
        this.validator = validator;
    }

    public Geometry fromXContent(XContentParser parser)
        throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            Geometry geometry = PARSER.apply(subParser, this);
            validator.validate(geometry);
            return geometry;
        }
    }

    public static XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE.getPreferredName(), getGeoJsonName(geometry));
        geometry.visit(new GeometryVisitor<XContentBuilder, IOException>() {
            @Override
            public XContentBuilder visit(Circle circle) throws IOException {
                builder.field(FIELD_RADIUS.getPreferredName(), DistanceUnit.METERS.toString(circle.getRadiusMeters()));
                builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
                return coordinatesToXContent(circle.getY(), circle.getX(), circle.getZ());
            }

            @Override
            public XContentBuilder visit(GeometryCollection<?> collection) throws IOException {
                builder.startArray(FIELD_GEOMETRIES.getPreferredName());
                for (Geometry g : collection) {
                    toXContent(g, builder, params);
                }
                return builder.endArray();
            }

            @Override
            public XContentBuilder visit(Line line) throws IOException {
                builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
                return coordinatesToXContent(line);
            }

            @Override
            public XContentBuilder visit(LinearRing ring) {
                throw new UnsupportedOperationException("linearRing cannot be serialized using GeoJson");
            }

            @Override
            public XContentBuilder visit(MultiLine multiLine) throws IOException {
                builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
                builder.startArray();
                for (int i = 0; i < multiLine.size(); i++) {
                    coordinatesToXContent(multiLine.get(i));
                }
                return builder.endArray();
            }

            @Override
            public XContentBuilder visit(MultiPoint multiPoint) throws IOException {
                builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
                for (int i = 0; i < multiPoint.size(); i++) {
                    Point p = multiPoint.get(i);
                    builder.startArray().value(p.getX()).value(p.getY());
                    if (p.hasZ()) {
                        builder.value(p.getZ());
                    }
                    builder.endArray();
                }
                return builder.endArray();
            }

            @Override
            public XContentBuilder visit(MultiPolygon multiPolygon) throws IOException {
                builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
                for (int i = 0; i < multiPolygon.size(); i++) {
                    builder.startArray();
                    coordinatesToXContent(multiPolygon.get(i));
                    builder.endArray();
                }
                return builder.endArray();
            }

            @Override
            public XContentBuilder visit(Point point) throws IOException {
                builder.field(ShapeParser.FIELD_COORDINATES.getPreferredName());
                return coordinatesToXContent(point.getY(), point.getX(), point.getZ());
            }

            @Override
            public XContentBuilder visit(Polygon polygon) throws IOException {
                builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
                coordinatesToXContent(polygon.getPolygon());
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    coordinatesToXContent(polygon.getHole(i));
                }
                return builder.endArray();
            }

            @Override
            public XContentBuilder visit(Rectangle rectangle) throws IOException {
                builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
                coordinatesToXContent(rectangle.getMaxY(), rectangle.getMinX(), rectangle.getMinZ()); // top left
                coordinatesToXContent(rectangle.getMinY(), rectangle.getMaxX(), rectangle.getMaxZ()); // bottom right
                return builder.endArray();
            }

            private XContentBuilder coordinatesToXContent(double lat, double lon, double alt) throws IOException {
                builder.startArray().value(lon).value(lat);
                if (Double.isNaN(alt) == false) {
                    builder.value(alt);
                }
                return builder.endArray();
            }

            private XContentBuilder coordinatesToXContent(Line line) throws IOException {
                builder.startArray();
                for (int i = 0; i < line.length(); i++) {
                    builder.startArray().value(line.getX(i)).value(line.getY(i));
                    if (line.hasZ()) {
                        builder.value(line.getZ(i));
                    }
                    builder.endArray();
                }
                return builder.endArray();
            }

            private XContentBuilder coordinatesToXContent(Polygon polygon) throws IOException {
                coordinatesToXContent(polygon.getPolygon());
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    coordinatesToXContent(polygon.getHole(i));
                }
                return builder;
            }

        });
        return builder.endObject();
    }

    /**
     * Produces that same GeoJSON as toXContent only in parsed map form
     */
    public static Map<String, Object> toMap(Geometry geometry) {
        Map<String, Object> root = new HashMap<>();
        root.put(FIELD_TYPE.getPreferredName(), getGeoJsonName(geometry));

        geometry.visit(new GeometryVisitor<Void, RuntimeException>() {
            @Override
            public Void visit(Circle circle) {
                root.put(FIELD_RADIUS.getPreferredName(), DistanceUnit.METERS.toString(circle.getRadiusMeters()));
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinatesToList(circle.getY(), circle.getX(), circle.getZ()));
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                List<Object> geometries = new ArrayList<>(collection.size());

                for (Geometry g : collection) {
                    geometries.add(toMap(g));
                }
                root.put(FIELD_GEOMETRIES.getPreferredName(),  geometries);
                return null;
            }

            @Override
            public Void visit(Line line) {
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinatesToList(line));
                return null;
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new UnsupportedOperationException("linearRing cannot be serialized using GeoJson");
            }

            @Override
            public Void visit(MultiLine multiLine) {
                List<Object> lines = new ArrayList<>(multiLine.size());
                for (int i = 0; i < multiLine.size(); i++) {
                    lines.add(coordinatesToList(multiLine.get(i)));
                }
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), lines);
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                List<Object> points = new ArrayList<>(multiPoint.size());
                for (int i = 0; i < multiPoint.size(); i++) {
                    Point p = multiPoint.get(i);
                    List<Object> point = new ArrayList<>();
                    point.add(p.getX());
                    point.add(p.getY());
                    if (p.hasZ()) {
                        point.add(p.getZ());
                    }
                    points.add(point);
                }
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), points);
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                List<Object> polygons = new ArrayList<>();
                for (int i = 0; i < multiPolygon.size(); i++) {
                    polygons.add(coordinatesToList(multiPolygon.get(i)));
                }
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), polygons);
                return null;
            }

            @Override
            public Void visit(Point point) {
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinatesToList(point.getY(), point.getX(), point.getZ()));
                return null;
            }

            @Override
            public Void visit(Polygon polygon) {
                List<Object> coords = new ArrayList<>(polygon.getNumberOfHoles() + 1);
                coords.add(coordinatesToList(polygon.getPolygon()));
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    coords.add(coordinatesToList(polygon.getHole(i)));
                }
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coords);
                return null;
            }

            @Override
            public Void visit(Rectangle rectangle) {
                List<Object> coords = new ArrayList<>(2);
                coords.add(coordinatesToList(rectangle.getMaxY(), rectangle.getMinX(), rectangle.getMinZ())); // top left
                coords.add(coordinatesToList(rectangle.getMinY(), rectangle.getMaxX(), rectangle.getMaxZ())); // bottom right
                root.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coords);
                return null;
            }

            private List<Object> coordinatesToList(double lat, double lon, double alt) {
                List<Object> coords = new ArrayList<>(3);
                coords.add(lon);
                coords.add(lat);
                if (Double.isNaN(alt) == false) {
                    coords.add(alt);
                }
                return coords;
            }

            private List<Object> coordinatesToList(Line line) {
                List<Object> lines = new ArrayList<>(line.length());
                for (int i = 0; i < line.length(); i++) {
                    List<Object> coords = new ArrayList<>(3);
                    coords.add(line.getX(i));
                    coords.add(line.getY(i));
                    if (line.hasZ()) {
                        coords.add(line.getZ(i));
                    }
                    lines.add(coords);
                }
                return lines;
            }

            private List<Object> coordinatesToList(Polygon polygon) {
                List<Object> coords = new ArrayList<>(polygon.getNumberOfHoles() + 1);
                coords.add(coordinatesToList(polygon.getPolygon()));
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    coords.add(coordinatesToList(polygon.getHole(i)));
                }
                return coords;
            }

        });
        return root;
    }

    private static final ConstructingObjectParser<Geometry, GeoJson> PARSER =
        new ConstructingObjectParser<>("geojson", true, (a, c) -> {
            String type = (String) a[0];
            CoordinateNode coordinates = (CoordinateNode) a[1];
            @SuppressWarnings("unchecked") List<Geometry> geometries = (List<Geometry>) a[2];
            Boolean orientation = orientationFromString((String) a[3]);
            DistanceUnit.Distance radius = (DistanceUnit.Distance) a[4];
            return createGeometry(type, geometries, coordinates, orientation, c.rightOrientation, c.coerce, radius);
        });

    static {
        PARSER.declareString(constructorArg(), FIELD_TYPE);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> parseCoordinates(p), FIELD_COORDINATES,
            ObjectParser.ValueType.VALUE_ARRAY);
        PARSER.declareObjectArray(optionalConstructorArg(), PARSER, FIELD_GEOMETRIES);
        PARSER.declareString(optionalConstructorArg(), FIELD_ORIENTATION);
        PARSER.declareField(optionalConstructorArg(), p -> DistanceUnit.Distance.parseDistance(p.text()), FIELD_RADIUS,
            ObjectParser.ValueType.STRING);
    }

    private static Geometry createGeometry(String type, List<Geometry> geometries, CoordinateNode coordinates, Boolean orientation,
                                           boolean defaultOrientation, boolean coerce, DistanceUnit.Distance radius) {
        ShapeType shapeType;
        if ("bbox".equalsIgnoreCase(type)) {
            shapeType = ShapeType.ENVELOPE;
        } else {
            shapeType = ShapeType.forName(type);
        }
        if (shapeType == ShapeType.GEOMETRYCOLLECTION) {
            if (geometries == null) {
                throw new ElasticsearchParseException("geometries not included");
            }
            if (coordinates != null) {
                throw new ElasticsearchParseException("parameter coordinates is not supported for type " + type);
            }
            verifyNulls(type, null, orientation, radius);
            return new GeometryCollection<>(geometries);
        }

        // We expect to have coordinates for all the rest
        if (coordinates == null) {
            throw new ElasticsearchParseException("coordinates not included");
        }

        switch (shapeType) {
            case CIRCLE:
                if (radius == null) {
                    throw new ElasticsearchParseException("radius is not specified");
                }
                verifyNulls(type, geometries, orientation, null);
                Point point = coordinates.asPoint();
                return new Circle(point.getX(), point.getY(), point.getZ(), radius.convert(DistanceUnit.METERS).value);
            case POINT:
                verifyNulls(type, geometries, orientation, radius);
                return coordinates.asPoint();
            case MULTIPOINT:
                verifyNulls(type, geometries, orientation, radius);
                return coordinates.asMultiPoint();
            case LINESTRING:
                verifyNulls(type, geometries, orientation, radius);
                return coordinates.asLineString(coerce);
            case MULTILINESTRING:
                verifyNulls(type, geometries, orientation, radius);
                return coordinates.asMultiLineString(coerce);
            case POLYGON:
                verifyNulls(type, geometries, null, radius);
                // handle possible null in orientation
                return coordinates.asPolygon(orientation != null ? orientation : defaultOrientation, coerce);
            case MULTIPOLYGON:
                verifyNulls(type, geometries, null, radius);
                // handle possible null in orientation
                return coordinates.asMultiPolygon(orientation != null ? orientation : defaultOrientation, coerce);
            case ENVELOPE:
                verifyNulls(type, geometries, orientation, radius);
                return coordinates.asRectangle();
            default:
                throw new ElasticsearchParseException("unsupported shape type " + type);
        }
    }

    /**
     * Checks that all passed parameters except type are null, generates corresponding error messages if they are not
     */
    private static void verifyNulls(String type, List<Geometry> geometries, Boolean orientation, DistanceUnit.Distance radius) {
        if (geometries != null) {
            throw new ElasticsearchParseException("parameter geometries is not supported for type " + type);
        }
        if (orientation != null) {
            throw new ElasticsearchParseException("parameter orientation is not supported for type " + type);
        }
        if (radius != null) {
            throw new ElasticsearchParseException("parameter radius is not supported for type " + type);
        }
    }

    /**
     * Recursive method which parses the arrays of coordinates used to define
     * Shapes
     */
    private static CoordinateNode parseCoordinates(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        // Base cases
        if (token != XContentParser.Token.START_ARRAY &&
            token != XContentParser.Token.END_ARRAY &&
            token != XContentParser.Token.VALUE_NULL) {
            return new CoordinateNode(parseCoordinate(parser));
        } else if (token == XContentParser.Token.VALUE_NULL) {
            throw new IllegalArgumentException("coordinates cannot contain NULL values)");
        }

        List<CoordinateNode> nodes = new ArrayList<>();
        while (token != XContentParser.Token.END_ARRAY) {
            CoordinateNode node = parseCoordinates(parser);
            if (nodes.isEmpty() == false && nodes.get(0).numDimensions() != node.numDimensions()) {
                throw new ElasticsearchParseException("Exception parsing coordinates: number of dimensions do not match");
            }
            nodes.add(node);
            token = parser.nextToken();
        }

        return new CoordinateNode(nodes);
    }

    /**
     * Parser a singe set of 2 or 3 coordinates
     */
    private static Point parseCoordinate(XContentParser parser) throws IOException {
        // Add support for coerce here
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
            alt = parser.doubleValue();
            parser.nextToken();
        }
        // do not support > 3 dimensions
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            throw new ElasticsearchParseException("geo coordinates greater than 3 dimensions are not supported");
        }
        return new Point(lon, lat, alt);
    }

    /**
     * Returns true for right orientation and false for left
     */
    private static Boolean orientationFromString(String orientation) {
        if (orientation == null) {
            return null;
        }
        orientation = orientation.toLowerCase(Locale.ROOT);
        switch (orientation) {
            case "right":
            case "counterclockwise":
            case "ccw":
                return true;
            case "left":
            case "clockwise":
            case "cw":
                return false;
            default:
                throw new IllegalArgumentException("Unknown orientation [" + orientation + "]");
        }
    }

    public static String getGeoJsonName(Geometry geometry) {
        return geometry.visit(new GeometryVisitor<>() {
            @Override
            public String visit(Circle circle) {
                return "Circle";
            }

            @Override
            public String visit(GeometryCollection<?> collection) {
                return "GeometryCollection";
            }

            @Override
            public String visit(Line line) {
                return "LineString";
            }

            @Override
            public String visit(LinearRing ring) {
                throw new UnsupportedOperationException("line ring cannot be serialized using GeoJson");
            }

            @Override
            public String visit(MultiLine multiLine) {
                return "MultiLineString";
            }

            @Override
            public String visit(MultiPoint multiPoint) {
                return "MultiPoint";
            }

            @Override
            public String visit(MultiPolygon multiPolygon) {
                return "MultiPolygon";
            }

            @Override
            public String visit(Point point) {
                return "Point";
            }

            @Override
            public String visit(Polygon polygon) {
                return "Polygon";
            }

            @Override
            public String visit(Rectangle rectangle) {
                return "Envelope";
            }
        });
    }

    private static class CoordinateNode implements ToXContentObject {
        public final Point coordinate;
        public final List<CoordinateNode> children;

        /**
         * Creates a new leaf CoordinateNode
         *
         * @param coordinate Coordinate for the Node
         */
        CoordinateNode(Point coordinate) {
            this.coordinate = coordinate;
            this.children = null;
        }

        /**
         * Creates a new parent CoordinateNode
         *
         * @param children Children of the Node
         */
        CoordinateNode(List<CoordinateNode> children) {
            this.children = children;
            this.coordinate = null;
        }

        public boolean isEmpty() {
            return (coordinate == null && (children == null || children.isEmpty()));
        }

        protected int numDimensions() {
            if (isEmpty()) {
                throw new ElasticsearchException("attempting to get number of dimensions on an empty coordinate node");
            }
            if (coordinate != null) {
                return coordinate.hasZ() ? 3 : 2;
            }
            return children.get(0).numDimensions();
        }

        public Point asPoint() {
            if (children != null) {
                throw new ElasticsearchException("expected a single points but got a list");
            }
            return coordinate;
        }

        public MultiPoint asMultiPoint() {
            if (coordinate != null) {
                throw new ElasticsearchException("expected a list of points but got a point");
            }
            List<Point> points = new ArrayList<>();
            for (CoordinateNode node : children) {
                points.add(node.asPoint());
            }
            return new MultiPoint(points);
        }

        private double[][] asLineComponents(boolean orientation, boolean coerce, boolean close) {
            if (coordinate != null) {
                throw new ElasticsearchException("expected a list of points but got a point");
            }

            if (children.size() < 2) {
                throw new ElasticsearchException("not enough points to build a line");
            }

            boolean needsClosing;
            int resultSize;
            if (close && coerce && children.get(0).asPoint().equals(children.get(children.size() - 1).asPoint()) == false) {
                needsClosing = true;
                resultSize = children.size() + 1;
            } else {
                needsClosing = false;
                resultSize = children.size();
            }

            double[] lats = new double[resultSize];
            double[] lons = new double[resultSize];
            double[] alts = numDimensions() == 3 ? new double[resultSize] : null;
            int i = orientation ? 0 : lats.length - 1;
            for (CoordinateNode node : children) {
                Point point = node.asPoint();
                lats[i] = point.getY();
                lons[i] = point.getX();
                if (alts != null) {
                    alts[i] = point.getZ();
                }
                i = orientation ? i + 1 : i - 1;
            }
            if (needsClosing) {
                lats[resultSize - 1] = lats[0];
                lons[resultSize - 1] = lons[0];
                if (alts != null) {
                    alts[resultSize - 1] = alts[0];
                }
            }
            double[][] components = new double[3][];
            components[0] = lats;
            components[1] = lons;
            components[2] = alts;
            return components;
        }

        public Line asLineString(boolean coerce) {
            double[][] components = asLineComponents(true, coerce, false);
            return new Line(components[1], components[0], components[2]);
        }

        public LinearRing asLinearRing(boolean orientation, boolean coerce) {
            double[][] components = asLineComponents(orientation, coerce, true);
            return new LinearRing(components[1], components[0], components[2]);
        }

        public MultiLine asMultiLineString(boolean coerce) {
            if (coordinate != null) {
                throw new ElasticsearchException("expected a list of points but got a point");
            }
            List<Line> lines = new ArrayList<>();
            for (CoordinateNode node : children) {
                lines.add(node.asLineString(coerce));
            }
            return new MultiLine(lines);
        }


        public Polygon asPolygon(boolean orientation, boolean coerce) {
            if (coordinate != null) {
                throw new ElasticsearchException("expected a list of points but got a point");
            }
            List<LinearRing> lines = new ArrayList<>();
            for (CoordinateNode node : children) {
                lines.add(node.asLinearRing(orientation, coerce));
            }
            if (lines.size() == 1) {
                return new Polygon(lines.get(0));
            } else {
                LinearRing shell = lines.remove(0);
                return new Polygon(shell, lines);
            }
        }

        public MultiPolygon asMultiPolygon(boolean orientation, boolean coerce) {
            if (coordinate != null) {
                throw new ElasticsearchException("expected a list of points but got a point");
            }
            List<Polygon> polygons = new ArrayList<>();
            for (CoordinateNode node : children) {
                polygons.add(node.asPolygon(orientation, coerce));
            }
            return new MultiPolygon(polygons);
        }

        public Rectangle asRectangle() {
            if (children.size() != 2) {
                throw new ElasticsearchParseException(
                    "invalid number of points [{}] provided for geo_shape [{}] when expecting an array of 2 coordinates",
                    children.size(), ShapeType.ENVELOPE);
            }
            // verify coordinate bounds, correct if necessary
            Point uL = children.get(0).coordinate;
            Point lR = children.get(1).coordinate;
            return new Rectangle(uL.getX(), lR.getX(), uL.getY(), lR.getY());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (children == null) {
                builder.startArray().value(coordinate.getX()).value(coordinate.getY()).endArray();
            } else {
                builder.startArray();
                for (CoordinateNode child : children) {
                    child.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder;
        }
    }
}
