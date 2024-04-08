/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geo;

import org.apache.lucene.tests.geo.GeoTestUtil;
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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

public class GeometryTestUtils {

    public static double randomLat() {
        return GeoTestUtil.nextLatitude();
    }

    public static double randomLon() {
        return GeoTestUtil.nextLongitude();
    }

    public static double randomAlt() {
        return ESTestCase.randomDouble();
    }

    public static Circle randomCircle(boolean hasAlt) {
        org.apache.lucene.geo.Circle luceneCircle = GeoTestUtil.nextCircle();
        if (hasAlt) {
            return new Circle(luceneCircle.getLon(), luceneCircle.getLat(), ESTestCase.randomDouble(), luceneCircle.getRadius());
        } else {
            return new Circle(luceneCircle.getLon(), luceneCircle.getLat(), luceneCircle.getRadius());
        }
    }

    public static Line randomLine(boolean hasAlts) {
        // we use nextPolygon because it guarantees no duplicate points
        org.apache.lucene.geo.Polygon lucenePolygon = GeoTestUtil.nextPolygon();
        int size = lucenePolygon.numPoints() - 1;
        double[] lats = new double[size];
        double[] lons = new double[size];
        double[] alts = hasAlts ? new double[size] : null;
        for (int i = 0; i < size; i++) {
            lats[i] = lucenePolygon.getPolyLat(i);
            lons[i] = lucenePolygon.getPolyLon(i);
            if (hasAlts) {
                alts[i] = randomAlt();
            }
        }
        if (hasAlts) {
            return new Line(lons, lats, alts);
        }
        return new Line(lons, lats);
    }

    public static Point randomPoint() {
        return randomPoint(ESTestCase.randomBoolean());
    }

    public static Point randomPoint(boolean hasAlt) {
        if (hasAlt) {
            return new Point(randomLon(), randomLat(), randomAlt());
        } else {
            return new Point(randomLon(), randomLat());
        }
    }

    public static Polygon randomPolygon(boolean hasAlt) {
        org.apache.lucene.geo.Polygon lucenePolygon = randomValueOtherThanMany(p -> area(p) == 0, GeoTestUtil::nextPolygon);
        if (lucenePolygon.numHoles() > 0) {
            org.apache.lucene.geo.Polygon[] luceneHoles = lucenePolygon.getHoles();
            List<LinearRing> holes = new ArrayList<>();
            for (int i = 0; i < lucenePolygon.numHoles(); i++) {
                org.apache.lucene.geo.Polygon poly = luceneHoles[i];
                holes.add(linearRing(poly.getPolyLons(), poly.getPolyLats(), hasAlt));
            }
            return new Polygon(linearRing(lucenePolygon.getPolyLons(), lucenePolygon.getPolyLats(), hasAlt), holes);
        }
        return new Polygon(linearRing(lucenePolygon.getPolyLons(), lucenePolygon.getPolyLats(), hasAlt));
    }

    private static double area(org.apache.lucene.geo.Polygon lucenePolygon) {
        double windingSum = 0;
        final int numPts = lucenePolygon.numPoints() - 1;
        for (int i = 0; i < numPts; i++) {
            // compute signed area
            windingSum += lucenePolygon.getPolyLon(i) * lucenePolygon.getPolyLat(i + 1) - lucenePolygon.getPolyLat(i) * lucenePolygon
                .getPolyLon(i + 1);
        }
        return Math.abs(windingSum / 2);
    }

    private static double[] randomAltRing(int size) {
        double[] alts = new double[size];
        for (int i = 0; i < size - 1; i++) {
            alts[i] = randomAlt();
        }
        alts[size - 1] = alts[0];
        return alts;
    }

    public static LinearRing linearRing(double[] lons, double[] lats, boolean generateAlts) {
        if (generateAlts) {
            return new LinearRing(lons, lats, randomAltRing(lats.length));
        }
        return new LinearRing(lons, lats);
    }

    public static Rectangle randomRectangle() {
        org.apache.lucene.geo.Rectangle rectangle = GeoTestUtil.nextBox();
        return new Rectangle(rectangle.minLon, rectangle.maxLon, rectangle.maxLat, rectangle.minLat);
    }

    public static MultiPoint randomMultiPoint(boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(3, 10);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            points.add(randomPoint(hasAlt));
        }
        return new MultiPoint(points);
    }

    public static MultiLine randomMultiLine(boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(3, 10);
        List<Line> lines = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            lines.add(randomLine(hasAlt));
        }
        return new MultiLine(lines);
    }

    public static MultiPolygon randomMultiPolygon(boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(3, 10);
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            polygons.add(randomPolygon(hasAlt));
        }
        return new MultiPolygon(polygons);
    }

    public static GeometryCollection<Geometry> randomGeometryCollection(boolean hasAlt) {
        return randomGeometryCollection(0, hasAlt);
    }

    public static GeometryCollection<Geometry> randomGeometryCollectionWithoutCircle(boolean hasAlt) {
        return randomGeometryCollectionWithoutCircle(0, hasAlt);
    }

    private static GeometryCollection<Geometry> randomGeometryCollection(int level, boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometry(level, hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }

    private static GeometryCollection<Geometry> randomGeometryCollectionWithoutCircle(int level, boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometryWithoutCircle(level, hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }

    public static Geometry randomGeometry(ShapeType type, boolean hasAlt) {
        return switch (type) {
            case GEOMETRYCOLLECTION -> randomGeometryCollection(0, hasAlt);
            case MULTILINESTRING -> randomMultiLine(hasAlt);
            case ENVELOPE -> randomRectangle();
            case LINESTRING -> randomLine(hasAlt);
            case POLYGON -> randomPolygon(hasAlt);
            case MULTIPOLYGON -> randomMultiPolygon(hasAlt);
            case CIRCLE -> randomCircle(hasAlt);
            case MULTIPOINT -> randomMultiPoint(hasAlt);
            case POINT -> randomPoint(hasAlt);
            default -> throw new IllegalArgumentException("Unsupported shape type [" + type + "]");
        };
    }

    public static Geometry randomGeometry(boolean hasAlt) {
        return randomGeometry(0, hasAlt);
    }

    protected static Geometry randomGeometry(int level, boolean hasAlt) {
        @SuppressWarnings("unchecked")
        Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            GeometryTestUtils::randomCircle,
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint,
            GeometryTestUtils::randomMultiPolygon,
            hasAlt ? GeometryTestUtils::randomPoint : (b) -> randomRectangle(),
            level < 3 ? (b) -> randomGeometryCollection(level + 1, b) : GeometryTestUtils::randomPoint // don't build too deep
        );
        return geometry.apply(hasAlt);
    }

    public static Geometry randomGeometryWithoutCircle(int level, boolean hasAlt) {
        @SuppressWarnings("unchecked")
        Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomMultiPoint,
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiPolygon,
            hasAlt ? GeometryTestUtils::randomPoint : (b) -> randomRectangle(),
            level < 3 ? (b) -> randomGeometryWithoutCircleCollection(level + 1, hasAlt) : GeometryTestUtils::randomPoint // don't build too
                                                                                                                         // deep
        );
        return geometry.apply(hasAlt);
    }

    private static Geometry randomGeometryWithoutCircleCollection(int level, boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometryWithoutCircle(level, hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }

    /**
     * Extracts all vertices of the supplied geometry
     */
    public static MultiPoint toMultiPoint(Geometry geometry) {
        return geometry.visit(new GeometryVisitor<>() {
            @Override
            public MultiPoint visit(Circle circle) throws RuntimeException {
                throw new UnsupportedOperationException("not supporting circles yet");
            }

            @Override
            public MultiPoint visit(GeometryCollection<?> collection) throws RuntimeException {
                List<Point> points = new ArrayList<>();
                collection.forEach(geometry -> toMultiPoint(geometry).forEach(points::add));
                return new MultiPoint(points);
            }

            @Override
            public MultiPoint visit(Line line) throws RuntimeException {
                List<Point> points = new ArrayList<>();
                for (int i = 0; i < line.length(); i++) {
                    points.add(new Point(line.getX(i), line.getY(i), line.getZ(i)));
                }
                return new MultiPoint(points);
            }

            @Override
            public MultiPoint visit(LinearRing ring) throws RuntimeException {
                return visit((Line) ring);
            }

            @Override
            public MultiPoint visit(MultiLine multiLine) throws RuntimeException {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public MultiPoint visit(MultiPoint multiPoint) throws RuntimeException {
                return multiPoint;
            }

            @Override
            public MultiPoint visit(MultiPolygon multiPolygon) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public MultiPoint visit(Point point) throws RuntimeException {
                return new MultiPoint(Collections.singletonList(point));
            }

            @Override
            public MultiPoint visit(Polygon polygon) throws RuntimeException {
                List<Geometry> multiPoints = new ArrayList<>();
                multiPoints.add(toMultiPoint(polygon.getPolygon()));
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    multiPoints.add(toMultiPoint(polygon.getHole(i)));
                }
                return toMultiPoint(new GeometryCollection<>(multiPoints));
            }

            @Override
            public MultiPoint visit(Rectangle rectangle) throws RuntimeException {
                return new MultiPoint(
                    Arrays.asList(
                        new Point(rectangle.getMinX(), rectangle.getMinY(), rectangle.getMinZ()),
                        new Point(rectangle.getMaxX(), rectangle.getMaxY(), rectangle.getMaxZ())
                    )
                );
            }
        });
    }
}
