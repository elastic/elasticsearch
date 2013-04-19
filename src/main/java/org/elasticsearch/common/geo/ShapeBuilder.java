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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;
import com.spatial4j.core.shape.impl.RectangleImpl;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.spatial4j.core.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;

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
        return new PointImpl(lon, lat, GeoShapeConstants.SPATIAL_CONTEXT);
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
     * Creates a new {@link MultiPolygonBuilder} to build a MultiPolygon
     *
     * @return MultiPolygonBuilder instance
     */
    public static MultiPolygonBuilder newMultiPolygon() {
        return new MultiPolygonBuilder();
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
            return ((JtsGeometry) shape).getGeom();
        } else if (shape instanceof JtsPoint) {
            return ((JtsPoint) shape).getGeom();
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
            this.topLeft = new PointImpl(lon, lat, GeoShapeConstants.SPATIAL_CONTEXT);
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
            this.bottomRight = new PointImpl(lon, lat, GeoShapeConstants.SPATIAL_CONTEXT);
            return this;
        }

        /**
         * Builds the {@link Rectangle} instance
         *
         * @return Built Rectangle
         */
        public Rectangle build() {
            return new RectangleImpl(topLeft.getX(), bottomRight.getX(), bottomRight.getY(), topLeft.getY(), GeoShapeConstants.SPATIAL_CONTEXT);
        }
    }

    /**
     * Builder for creating a {@link Shape} instance of a MultiPolygon
     */
    public static class MultiPolygonBuilder {
        private final ArrayList<EmbededPolygonBuilder<MultiPolygonBuilder>> polygons = new ArrayList<EmbededPolygonBuilder<MultiPolygonBuilder>>();

        /**
         * Add a new polygon to the multipolygon
         * 
         * @return builder for the new polygon
         */
        public EmbededPolygonBuilder<MultiPolygonBuilder> polygon() {
            EmbededPolygonBuilder<MultiPolygonBuilder> builder = new EmbededPolygonBuilder<MultiPolygonBuilder>(this);
            polygons.add(builder);
            return builder;
        }

        public Shape build() {
            return new JtsGeometry(toMultiPolygon(), GeoShapeConstants.SPATIAL_CONTEXT, true);
        }

        public MultiPolygon toMultiPolygon() {
            Polygon[] polygons = new Polygon[this.polygons.size()];
            for (int i = 0; i<polygons.length; i++) {
                polygons[i] = this.polygons.get(i).toPolygon();
            }
            return GEOMETRY_FACTORY.createMultiPolygon(polygons);
        }

        public XContentBuilder toXContent(String name, XContentBuilder xcontent) throws IOException {
            if(name != null) {
                xcontent.startObject(name);
            } else {
                xcontent.startObject();
            }
            xcontent.field("type", "multipolygon");
            emdedXContent("coordinates", xcontent);
            xcontent.endObject();
            return xcontent;
        }

        protected void emdedXContent(String name, XContentBuilder xcontent) throws IOException {
            if(name != null) {
                xcontent.startArray(name);
            } else {
                xcontent.startArray();
            }
            for(EmbededPolygonBuilder<MultiPolygonBuilder> polygon : polygons) {
                polygon.emdedXContent(null, xcontent);
            }
            xcontent.endArray();
        }

    }

    /**
     * Builder for creating a {@link Shape} instance of a single Polygon
     */
    public static class PolygonBuilder extends EmbededPolygonBuilder<PolygonBuilder> {

        private PolygonBuilder() {
            super(null);
        }

        @Override
        public PolygonBuilder close() {
            super.close();
            return this;
        }
    }

    /**
     * Builder for creating a {@link Shape} instance of a Polygon
     */
    public static class EmbededPolygonBuilder<E> {

        private final E parent;
        private final LinearRingBuilder<EmbededPolygonBuilder<E>> ring = new LinearRingBuilder<EmbededPolygonBuilder<E>>(this);
        private final ArrayList<LinearRingBuilder<EmbededPolygonBuilder<E>>> holes = new ArrayList<LinearRingBuilder<EmbededPolygonBuilder<E>>>();

        private EmbededPolygonBuilder(E parent) {
            super();
            this.parent = parent;
        }

        /**
         * Adds a point to the Polygon
         *
         * @param lon Longitude of the point
         * @param lat Latitude of the point
         * @return this
         */
        public EmbededPolygonBuilder<E> point(double lon, double lat) {
            ring.point(lon, lat);
            return this;
        }

        /**
         * Start creating a new hole within the polygon
         * @return a builder for holes
         */
        public LinearRingBuilder<EmbededPolygonBuilder<E>> hole() {
            LinearRingBuilder<EmbededPolygonBuilder<E>> builder = new LinearRingBuilder<EmbededPolygonBuilder<E>>(this);
            this.holes.add(builder);
            return builder;
        }

        /**
         * Builds a {@link Shape} instance representing the {@link Polygon}
         *
         * @return Built LinearRing
         */
        public Shape build() {
            return new JtsGeometry(toPolygon(), GeoShapeConstants.SPATIAL_CONTEXT, true);
        }

        /**
         * Creates the raw {@link Polygon}
         *
         * @return Built polygon
         */
        public Polygon toPolygon() {
            this.ring.close();
            LinearRing ring = this.ring.toLinearRing();
            LinearRing[] rings = new LinearRing[holes.size()];
            for (int i = 0; i < rings.length; i++) {
                rings[i] = this.holes.get(i).toLinearRing();
            }
            return GEOMETRY_FACTORY.createPolygon(ring, rings);
        }

        /**
         * Close the linestring by copying the first point if necessary
         * @return parent object
         */
        public E close() {
            this.ring.close();
            return parent;
        }

        public XContentBuilder toXContent(String name, XContentBuilder xcontent) throws IOException {
            if(name != null) {
                xcontent.startObject(name);
            } else {
                xcontent.startObject();
            }
            xcontent.field("type", "polygon");
            emdedXContent("coordinates", xcontent);
            xcontent.endObject();
            return xcontent;
        }

        protected void emdedXContent(String name, XContentBuilder xcontent) throws IOException {
            if(name != null) {
                xcontent.startArray(name);
            } else {
                xcontent.startArray();
            }
            ring.emdedXContent(null, xcontent);
            for (LinearRingBuilder<?> ring : holes) {
                ring.emdedXContent(null, xcontent);
            }
            xcontent.endArray();
        }

    }

    /**
     * Builder for creating a {@link Shape} instance of a Polygon
     */
    public static class LinearRingBuilder<E> {

        private final E parent;
        private final List<Point> points = new ArrayList<Point>();

        private LinearRingBuilder(E parent) {
            super();
            this.parent = parent;
        }

        /**
         * Adds a point to the Ring
         *
         * @param lon Longitude of the point
         * @param lat Latitude of the point
         * @return this
         */
        public LinearRingBuilder<E> point(double lon, double lat) {
            points.add(new PointImpl(lon, lat, GeoShapeConstants.SPATIAL_CONTEXT));
            return this;
        }

        /**
         * Builds a {@link Shape} instance representing the ring
         *
         * @return Built LinearRing
         */
        protected Shape build() {
            return new JtsGeometry(toLinearRing(), GeoShapeConstants.SPATIAL_CONTEXT, true);
        }

        /**
         * Creates the raw {@link Polygon}
         *
         * @return Built LinearRing
         */
        protected LinearRing toLinearRing() {
            this.close();
            Coordinate[] coordinates = new Coordinate[points.size()];
            for (int i = 0; i < coordinates.length; i++) {
                coordinates[i] = new Coordinate(points.get(i).getX(), points.get(i).getY());
            }

            return GEOMETRY_FACTORY.createLinearRing(coordinates);
        }

        /**
         * Close the linestring by copying the first point if necessary
         * @return parent object
         */
        public E close() {
            Point first = points.get(0);
            Point last = points.get(points.size()-1);
            if(first.getX() != last.getX() || first.getY() != last.getY()) {
                points.add(first);
            }

            if(points.size()<4) {
                throw new ElasticSearchIllegalArgumentException("A linear ring is defined by a least four points");
            }

            return parent;
        }

        public XContentBuilder toXContent(String name, XContentBuilder xcontent) throws IOException {
            if(name != null) {
                xcontent.startObject(name);
            } else {
                xcontent.startObject();
            }
            xcontent.field("type", "linestring");
            emdedXContent("coordinates", xcontent);
            xcontent.endObject();
            return xcontent;
        }

        protected void emdedXContent(String name, XContentBuilder xcontent) throws IOException {
            if(name != null) {
                xcontent.startArray(name);
            } else {
                xcontent.startArray();
            }
            for(Point point : points) {
                xcontent.startArray().value(point.getY()).value(point.getX()).endArray();
            }
            xcontent.endArray();
        }
    }
}
