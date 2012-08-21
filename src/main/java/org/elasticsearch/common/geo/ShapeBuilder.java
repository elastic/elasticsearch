package org.elasticsearch.common.geo;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.spatial4j.core.shape.simple.PointImpl;
import com.spatial4j.core.shape.simple.RectangleImpl;
import com.vividsolutions.jts.geom.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for building {@link Shape} instances like {@link Point},
 * {@link Rectangle} and Polygons.
 */
public class ShapeBuilder {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private ShapeBuilder() {
    }

    /**
     * Creates a new {@link Point}
     *
     * @param lon Longitude of point
     * @param lat Latitude of point
     * @return Point with the latitude and longitude
     */
    public static Point newPoint(double lon, double lat) {
        return new PointImpl(lon, lat);
    }

    /**
     * Creates a new {@link RectangleBuilder} to build a {@link Rectangle}
     *
     * @return RectangleBuilder instance
     */
    public static RectangleBuilder newRectangle() {
        return new RectangleBuilder();
    }

    /**
     * Creates a new {@link PolygonBuilder} to build a Polygon
     *
     * @return PolygonBuilder instance
     */
    public static PolygonBuilder newPolygon() {
        return new PolygonBuilder();
    }

    /**
     * Converts the given Shape into the JTS {@link Geometry} representation.
     * If the Shape already uses a Geometry, that is returned.
     *
     * @param shape Shape to convert
     * @return Geometry representation of the Shape
     */
    public static Geometry toJTSGeometry(Shape shape) {
        if (shape instanceof JtsGeometry) {
            return ((JtsGeometry) shape).geo;
        } else if (shape instanceof JtsPoint) {
            return ((JtsPoint) shape).getJtsPoint();
        } else if (shape instanceof Rectangle) {
            Rectangle rectangle = (Rectangle) shape;

            if (rectangle.getCrossesDateLine()) {
                throw new IllegalArgumentException("Cannot convert Rectangles that cross the dateline into JTS Geometrys");
            }

            return newPolygon().point(rectangle.getMinX(), rectangle.getMaxY())
                    .point(rectangle.getMaxX(), rectangle.getMaxY())
                    .point(rectangle.getMaxX(), rectangle.getMinY())
                    .point(rectangle.getMinX(), rectangle.getMinY())
                    .point(rectangle.getMinX(), rectangle.getMaxY()).toPolygon();
        } else if (shape instanceof Point) {
            Point point = (Point) shape;
            return GEOMETRY_FACTORY.createPoint(new Coordinate(point.getX(), point.getY()));
        }

        throw new IllegalArgumentException("Shape type [" + shape.getClass().getSimpleName() + "] not supported");
    }

    /**
     * Builder for creating a {@link Rectangle} instance
     */
    public static class RectangleBuilder {
        
        private Point topLeft;
        private Point bottomRight;

        /**
         * Sets the top left point of the Rectangle
         *
         * @param lon Longitude of the top left point
         * @param lat Latitude of the top left point
         * @return this
         */
        public RectangleBuilder topLeft(double lon, double lat) {
            this.topLeft = new PointImpl(lon, lat);
            return this;
        }

        /**
         * Sets the bottom right point of the Rectangle
         *
         * @param lon Longitude of the bottom right point
         * @param lat Latitude of the bottom right point
         * @return this
         */
        public RectangleBuilder bottomRight(double lon, double lat) {
            this.bottomRight = new PointImpl(lon, lat);
            return this;
        }

        /**
         * Builds the {@link Rectangle} instance
         *
         * @return Built Rectangle
         */
        public Rectangle build() {
            return new RectangleImpl(topLeft.getX(), bottomRight.getX(), bottomRight.getY(), topLeft.getY());
        }
    }

    /**
     * Builder for creating a {@link Shape} instance of a Polygon
     */
    public static class PolygonBuilder {

        private final List<Point> points = new ArrayList<Point>();

        /**
         * Adds a point to the Polygon
         *
         * @param lon Longitude of the point
         * @param lat Latitude of the point
         * @return this
         */
        public PolygonBuilder point(double lon, double lat) {
            points.add(new PointImpl(lon, lat));
            return this;
        }

        /**
         * Builds a {@link Shape} instance representing the polygon
         *
         * @return Built polygon
         */
        public Shape build() {
            return new JtsGeometry(toPolygon());
        }

        /**
         * Creates the raw {@link Polygon}
         *
         * @return Built polygon
         */
        public Polygon toPolygon() {
            Coordinate[] coordinates = new Coordinate[points.size()];
            for (int i = 0; i < points.size(); i++) {
                coordinates[i] = new Coordinate(points.get(i).getX(), points.get(i).getY());
            }

            LinearRing ring = GEOMETRY_FACTORY.createLinearRing(coordinates);
            return GEOMETRY_FACTORY.createPolygon(ring, null);
        }
    }
}
