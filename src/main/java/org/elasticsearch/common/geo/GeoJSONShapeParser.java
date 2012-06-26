package org.elasticsearch.common.geo;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.spatial4j.core.shape.simple.RectangleImpl;
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
 *
 * An example of the format used for polygons:
 *
 * {
 *   "type": "Polygon",
 *   "coordinates": [
 *      [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
 *      [100.0, 1.0], [100.0, 0.0] ]
 *   ]
 * }
 *
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
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticSearchParseException("Shape must be an object consisting of type and coordinates");
        }

        String shapeType = null;
        CoordinateNode node = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();

                if ("type".equals(fieldName)) {
                    token = parser.nextToken();
                    shapeType = parser.text().toLowerCase(Locale.ENGLISH);
                    if (shapeType == null) {
                        throw new ElasticSearchParseException("Unknown Shape type [" + parser.text() + "]");
                    }
                } else if ("coordinates".equals(fieldName)) {
                    token = parser.nextToken();
                    node = parseCoordinates(parser);
                }
            }
        }

        if (shapeType == null) {
            throw new ElasticSearchParseException("Shape type not included");
        } else if (node == null) {
            throw new ElasticSearchParseException("Coordinates not included");
        }

        return buildShape(shapeType, node);
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
            token = parser.nextToken();
            double lat = parser.doubleValue();
            token = parser.nextToken();
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
     * Builds the actual {@link Shape} with the given shape type from the tree
     * of coordinates
     *
     * @param shapeType Type of Shape to be built
     * @param node Root node of the coordinate tree
     * @return Shape built from the coordinates
     */
    private static Shape buildShape(String shapeType, CoordinateNode node) {
        if ("point".equals(shapeType)) {
            return new JtsPoint(GEOMETRY_FACTORY.createPoint(node.coordinate));
        } else if ("linestring".equals(shapeType)) {
            return new JtsGeometry(GEOMETRY_FACTORY.createLineString(toCoordinates(node)));
        } else if ("polygon".equals(shapeType)) {
            LinearRing shell = GEOMETRY_FACTORY.createLinearRing(toCoordinates(node.children.get(0)));
            LinearRing[] holes = null;
            if (node.children.size() > 1) {
                holes = new LinearRing[node.children.size() - 1];
                for (int i = 0; i < node.children.size() - 1; i++) {
                    holes[i] = GEOMETRY_FACTORY.createLinearRing(toCoordinates(node.children.get(i + 1)));
                }
            }
            return new JtsGeometry(GEOMETRY_FACTORY.createPolygon(shell, holes));
        } else if ("multipoint".equals(shapeType)) {
            return new JtsGeometry(GEOMETRY_FACTORY.createMultiPoint(toCoordinates(node)));
        } else if ("envelope".equals(shapeType)) {
            Coordinate[] coordinates = toCoordinates(node);
            return new RectangleImpl(coordinates[0].x, coordinates[1].x, coordinates[1].y, coordinates[0].y);
        }

        throw new UnsupportedOperationException("ShapeType [" + shapeType + "] not supported");
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
     *
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
}
