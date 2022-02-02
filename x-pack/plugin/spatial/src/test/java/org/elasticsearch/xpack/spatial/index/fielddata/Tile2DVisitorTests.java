/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.geo.GeometryTestUtils.randomMultiLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPolygon;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.hamcrest.Matchers.equalTo;

public class Tile2DVisitorTests extends ESTestCase {

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = { 0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0 };
        double[] py = { 0, -5, -9, -10, -9, 0, 9, 10, 9, 5, 0 };

        // test cell crossing poly
        Polygon pacMan = new Polygon(new LinearRing(py, px), Collections.emptyList());
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(pacMan, TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(-5, -6, 2, -2));
    }

    // adapted from org.apache.lucene.geo.TestPolygon2D#testMultiPolygon
    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(
            new LinearRing(new double[] { -50, 50, 50, -50, -50 }, new double[] { -50, -50, 50, 50, -50 }),
            Collections.singletonList(new LinearRing(new double[] { -10, 10, 10, -10, -10 }, new double[] { -10, -10, 10, 10, -10 }))
        );

        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(polyWithHole, CoordinateEncoder.GEO);

        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(6, -6, 6, -6)); // in the hole
        assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(25, -25, 25, -25)); // on the mainland
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(51, 51, 52, 52)); // outside of mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-60, -60, 60, 60)); // enclosing us completely
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(49, 49, 51, 51)); // overlapping the mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(9, 9, 11, 11)); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = { 0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 0, 0 };
        double[] py = { 0, 0, 20, 20, 0, 0, 20, 20, 0, 0, 30, 30, 0 };

        double[] hx = { 21, 21, 29, 29, 21 };
        double[] hy = { 1, 20, 20, 1, 1 };

        Polygon polyWithHole = new Polygon(new LinearRing(px, py), Collections.singletonList(new LinearRing(hx, hy)));
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(polyWithHole, CoordinateEncoder.GEO);
        // test cell crossing poly
        assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(5, 10, 5, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(15, 10, 15, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(25, 10, 25, 10));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = { 0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0 };
        double[] py = { 0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0 };

        // test cell crossing poly
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(new Line(px, py), CoordinateEncoder.GEO);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(-5, -6, 2, -2));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = { 0, 10, 10, 0, -8, -10, -8, 0, 10, 10 };
        double[] py = { 0, 5, 9, 10, 9, 0, -9, -10, -9, -5 };

        // test cell crossing poly
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(new Line(px, py), CoordinateEncoder.GEO);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(-5, -6, 2, -2));
    }

    public void testPacManPoints() throws Exception {
        // pacman
        List<Point> points = Arrays.asList(
            new Point(0, 0),
            new Point(5, 10),
            new Point(9, 10),
            new Point(10, 0),
            new Point(9, -8),
            new Point(0, -10),
            new Point(-9, -8),
            new Point(-10, 0),
            new Point(-9, 10),
            new Point(-5, 10)
        );

        // candidate intersects cell
        int xMin = 0;
        int xMax = 11;
        int yMin = -10;
        int yMax = 9;

        // test cell crossing poly
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(new MultiPoint(points), CoordinateEncoder.GEO);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(xMin, yMin, xMax, yMax));
    }

    public void testRandomMultiLineIntersections() throws IOException {
        MultiLine geometry = randomMultiLine(false);
        geometry = (MultiLine) GeometryNormalizer.apply(Orientation.CCW, geometry);
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
        Extent readerExtent = reader.getExtent();

        for (Line line : geometry) {
            Extent lineExtent = GeoTestUtils.geometryDocValueReader(line, CoordinateEncoder.GEO).getExtent();
            if (lineExtent.minX() != Integer.MIN_VALUE
                && lineExtent.maxX() != Integer.MAX_VALUE
                && lineExtent.minY() != Integer.MIN_VALUE
                && lineExtent.maxY() != Integer.MAX_VALUE) {
                assertRelation(
                    GeoRelation.QUERY_CROSSES,
                    reader,
                    Extent.fromPoints(lineExtent.minX() - 1, lineExtent.minY() - 1, lineExtent.maxX() + 1, lineExtent.maxY() + 1)
                );
            }
        }

        // extent that fully encloses the MultiLine
        assertRelation(GeoRelation.QUERY_CROSSES, reader, reader.getExtent());
        if (readerExtent.minX() != Integer.MIN_VALUE
            && readerExtent.maxX() != Integer.MAX_VALUE
            && readerExtent.minY() != Integer.MIN_VALUE
            && readerExtent.maxY() != Integer.MAX_VALUE) {
            assertRelation(
                GeoRelation.QUERY_CROSSES,
                reader,
                Extent.fromPoints(readerExtent.minX() - 1, readerExtent.minY() - 1, readerExtent.maxX() + 1, readerExtent.maxY() + 1)
            );
        }

    }

    public void testRandomPolygonIntersection() throws IOException {
        int testPointCount = randomIntBetween(50, 100);
        Point[] testPoints = new Point[testPointCount];
        double extentSize = randomDoubleBetween(1, 10, true);
        boolean[] intersects = new boolean[testPointCount];
        for (int i = 0; i < testPoints.length; i++) {
            testPoints[i] = randomPoint(false);
        }

        Geometry geometry = randomMultiPolygon(false);
        Geometry preparedGeometry = GeometryNormalizer.apply(Orientation.CCW, geometry);

        for (int i = 0; i < testPointCount; i++) {
            int cur = i;
            intersects[cur] = fold(preparedGeometry, false, (g, s) -> s || intersects(g, testPoints[cur], extentSize));
        }

        for (int i = 0; i < testPointCount; i++) {
            assertEquals(intersects[i], intersects(preparedGeometry, testPoints[i], extentSize));
        }
    }

    private Extent bufferedExtentFromGeoPoint(double x, double y, double extentSize) {
        int xMin = CoordinateEncoder.GEO.encodeX(Math.max(x - extentSize, -180.0));
        int xMax = CoordinateEncoder.GEO.encodeX(Math.min(x + extentSize, 180.0));
        int yMin = CoordinateEncoder.GEO.encodeY(Math.max(y - extentSize, -90));
        int yMax = CoordinateEncoder.GEO.encodeY(Math.min(y + extentSize, 90));
        return Extent.fromPoints(xMin, yMin, xMax, yMax);
    }

    private static Extent getExtentFromBox(double bottomLeftX, double bottomLeftY, double topRightX, double topRightY) {
        return Extent.fromPoints(
            CoordinateEncoder.GEO.encodeX(bottomLeftX),
            CoordinateEncoder.GEO.encodeY(bottomLeftY),
            CoordinateEncoder.GEO.encodeX(topRightX),
            CoordinateEncoder.GEO.encodeY(topRightY)
        );

    }

    private boolean intersects(Geometry g, Point p, double extentSize) throws IOException {

        Extent bufferBounds = bufferedExtentFromGeoPoint(p.getX(), p.getY(), extentSize);
        Tile2DVisitor tile2DVisitor = new Tile2DVisitor();
        tile2DVisitor.reset(bufferBounds.minX(), bufferBounds.minY(), bufferBounds.maxX(), bufferBounds.maxY());
        GeoTestUtils.geometryDocValueReader(g, CoordinateEncoder.GEO).visit(tile2DVisitor);
        return tile2DVisitor.relation() == GeoRelation.QUERY_CROSSES || tile2DVisitor.relation() == GeoRelation.QUERY_INSIDE;
    }

    /**
     * Preforms left fold operation on all primitive geometries (points, lines polygons, circles and rectangles).
     * All collection geometries are iterated depth first.
     */
    public static <R, E extends Exception> R fold(Geometry geometry, R state, CheckedBiFunction<Geometry, R, R, E> operation) throws E {
        return geometry.visit(new GeometryVisitor<R, E>() {
            @Override
            public R visit(Circle circle) throws E {
                return operation.apply(geometry, state);
            }

            @Override
            public R visit(GeometryCollection<?> collection) throws E {
                R ret = state;
                for (Geometry g : collection) {
                    ret = fold(g, ret, operation);
                }
                return ret;
            }

            @Override
            public R visit(Line line) throws E {
                return operation.apply(line, state);
            }

            @Override
            public R visit(LinearRing ring) throws E {
                return operation.apply(ring, state);
            }

            @Override
            public R visit(MultiLine multiLine) throws E {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public R visit(MultiPoint multiPoint) throws E {
                return visit((GeometryCollection<?>) multiPoint);
            }

            @Override
            public R visit(MultiPolygon multiPolygon) throws E {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public R visit(Point point) throws E {
                return operation.apply(point, state);
            }

            @Override
            public R visit(Polygon polygon) throws E {
                return operation.apply(polygon, state);
            }

            @Override
            public R visit(Rectangle rectangle) throws E {
                return operation.apply(rectangle, state);
            }
        });
    }

    static void assertRelation(GeoRelation expectedRelation, GeometryDocValueReader reader, Extent extent) throws IOException {
        Tile2DVisitor tile2DVisitor = new Tile2DVisitor();
        tile2DVisitor.reset(extent.minX(), extent.minY(), extent.maxX(), extent.maxY());
        reader.visit(tile2DVisitor);
        assertThat(expectedRelation, equalTo(tile2DVisitor.relation()));
    }
}
