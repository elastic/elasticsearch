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
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
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
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that transforms Elasticsearch geometry objects to other representations
 */
public class GeoShapeUtils {

   static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
       org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
       for(int i = 0; i<holes.length; i++) {
           holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
       }
       return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    static org.apache.lucene.geo.Polygon toLucenePolygon(Rectangle r) {
       return new org.apache.lucene.geo.Polygon(
           new double[]{r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat()},
           new double[]{r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon()});
    }

    static org.apache.lucene.geo.Rectangle toLuceneRectangle(Rectangle r) {
        return new org.apache.lucene.geo.Rectangle(r.getMaxLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
    }

    static org.apache.lucene.geo.Point toLucenePoint(Point point) {
        return new org.apache.lucene.geo.Point(point.getLat(), point.getLon());
    }

    static org.apache.lucene.geo.Line toLuceneLine(Line line) {
        return new org.apache.lucene.geo.Line(line.getLats(), line.getLons());
    }

    // public for testing
    public static ShapeBuilder<?,?,?> geometryToShapeBuilder(Geometry shape) {
        return  shape.visit(new GeometryVisitor<ShapeBuilder<?, ?, ?>, RuntimeException>() {

            @Override
            public ShapeBuilder<?, ?, ?> visit(Circle circle) {
                return new CircleBuilder().center(circle.getLon(), circle.getLat()).radius(circle.getRadiusMeters(), DistanceUnit.METERS);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(GeometryCollection<?> collection) {
                GeometryCollectionBuilder shapes = new GeometryCollectionBuilder();
                for (Geometry geometry : collection) {
                    shapes.shape(geometry.visit(this));
                }
                return shapes;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Line line) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < line.length(); i++) {
                    coordinates.add(new Coordinate(line.getX(i), line.getY(i), line.getZ(i)));
                }
                return new LineStringBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(LinearRing ring) {
                throw new UnsupportedOperationException("circle is not supported");
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiLine multiLine) {
                MultiLineStringBuilder lines = new MultiLineStringBuilder();
                for (int i = 0; i < multiLine.size(); i++) {
                    lines.linestring((LineStringBuilder) visit(multiLine.get(i)));
                }
                return lines;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPoint multiPoint) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < multiPoint.size(); i++) {
                    Point p = multiPoint.get(i);
                    coordinates.add(new Coordinate(p.getX(), p.getY(), p.getZ()));
                }
                return new MultiPointBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPolygon multiPolygon) {
                MultiPolygonBuilder polygons = new MultiPolygonBuilder();
                for (int i = 0; i < multiPolygon.size(); i++) {
                    polygons.polygon((PolygonBuilder) visit(multiPolygon.get(i)));
                }
                return polygons;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Point point) {
                return new PointBuilder(point.getX(), point.getY());
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Polygon polygon) {
                PolygonBuilder polygonBuilder =
                    new PolygonBuilder((LineStringBuilder) visit((Line) polygon.getPolygon()),
                        ShapeBuilder.Orientation.RIGHT, false);
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygonBuilder.hole((LineStringBuilder) visit((Line) polygon.getHole(i)));
                }
                return polygonBuilder;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Rectangle rectangle) {
                return new EnvelopeBuilder(new Coordinate(rectangle.getMinX(), rectangle.getMaxY()),
                    new Coordinate(rectangle.getMaxX(), rectangle.getMinY()));
            }
        });
    }

    private GeoShapeUtils() {
    }

}
