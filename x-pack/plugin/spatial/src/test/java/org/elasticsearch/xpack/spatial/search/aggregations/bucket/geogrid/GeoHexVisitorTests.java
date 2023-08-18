/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeometryDocValueReader;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

public class GeoHexVisitorTests extends ESTestCase {

    public void testPoint() throws IOException {
        doTestGeometry(GeoHexVisitorTests::getGeometryAsPoints, false);
    }

    public void testLine() throws IOException {
        doTestGeometry(GeoHexVisitorTests::getGeometryAsLine, false);
    }

    public void testTriangle() throws IOException {
        doTestGeometry(GeoHexVisitorTests::getGeometryAsPolygon, true);
    }

    private void doTestGeometry(LongFunction<Geometry> h3ToGeometry, boolean hasArea) throws IOException {
        // we ignore polar cells are they are problematic and do not keep the relationships
        long h3 = randomValueOtherThanMany(
            l -> l == H3.geoToH3(90, 0, H3.getResolution(l)) || l == H3.geoToH3(-90, 0, H3.getResolution(l)),
            () -> H3.geoToH3(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude(), randomIntBetween(2, 14))
        );
        long centerChild = H3.childPosToH3(h3, 0);
        // children position 3 is chosen so we never use a polar polygon
        long noChildIntersecting = H3.noChildIntersectingPosToH3(h3, 3);
        GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        final String failMsg = "failing h3: " + h3;
        boolean h3CrossesDateline = visitor.getLeftX() > visitor.getRightX();
        {
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(h3ToGeometry.apply(h3), CoordinateEncoder.GEO);
            visitor.reset(h3);
            reader.visit(visitor);
            assertEquals(failMsg, GeoRelation.QUERY_CROSSES, visitor.relation());

            Rectangle rectangle = getGeometryAsRectangle(h3);
            assertTrue(failMsg, visitor.intersectsBbox(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMinY(), rectangle.getMaxY()));
        }
        {
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(h3ToGeometry.apply(centerChild), CoordinateEncoder.GEO);
            visitor.reset(h3);
            reader.visit(visitor);
            assertEquals("failing h3: " + h3, GeoRelation.QUERY_CONTAINS, visitor.relation());

            Rectangle rectangle = getGeometryAsRectangle(centerChild);
            assertTrue(failMsg, visitor.intersectsBbox(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMinY(), rectangle.getMaxY()));
        }
        {
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(h3ToGeometry.apply(h3), CoordinateEncoder.GEO);
            visitor.reset(centerChild);
            reader.visit(visitor);
            if (hasArea) {
                if (h3CrossesDateline && visitor.getLeftX() > visitor.getRightX()) {
                    // if both polygons crosses the dateline it cannot be inside due to the polygon splitting technique
                    assertEquals("failing h3: " + h3, GeoRelation.QUERY_CROSSES, visitor.relation());
                } else {
                    assertEquals("failing h3: " + h3, GeoRelation.QUERY_INSIDE, visitor.relation());
                }
            } else {
                assertEquals("failing h3: " + h3, GeoRelation.QUERY_DISJOINT, visitor.relation());
            }
        }
        {
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(
                h3ToGeometry.apply(noChildIntersecting),
                CoordinateEncoder.GEO
            );
            visitor.reset(centerChild);
            reader.visit(visitor);
            assertEquals("failing h3: " + h3, GeoRelation.QUERY_DISJOINT, visitor.relation());

            Rectangle rectangle = getGeometryAsRectangle(noChildIntersecting);
            assertFalse(
                failMsg,
                visitor.intersectsBbox(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMinY(), rectangle.getMaxY())
            );
        }
        {
            GeometryCollection<Geometry> collection = new GeometryCollection<>(
                List.of(h3ToGeometry.apply(centerChild), h3ToGeometry.apply(noChildIntersecting))
            );
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(collection, CoordinateEncoder.GEO);
            visitor.reset(h3);
            reader.visit(visitor);
            assertEquals("failing h3: " + h3, GeoRelation.QUERY_CROSSES, visitor.relation());
        }
        {
            LatLng latLng1 = H3.h3ToLatLng(centerChild);
            LatLng latLng2 = H3.h3ToLatLng(noChildIntersecting);
            MultiPoint multiPoint = new MultiPoint(
                List.of(new Point(latLng1.getLonDeg(), latLng1.getLatDeg()), new Point(latLng2.getLonDeg(), latLng2.getLatDeg()))
            );
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(multiPoint, CoordinateEncoder.GEO);
            visitor.reset(h3);
            reader.visit(visitor);
            assertEquals("failing h3: " + h3, GeoRelation.QUERY_CROSSES, visitor.relation());
        }
    }

    private static Geometry getGeometryAsPolygon(long h3) {
        final GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        final Polygon polygon = new Polygon(new LinearRing(visitor.getXs(), visitor.getYs()));
        if (visitor.getLeftX() > visitor.getRightX()) {
            final Geometry geometry = GeometryNormalizer.apply(Orientation.CCW, polygon);
            if (geometry instanceof Polygon) {
                // there is a bug on the code that breaks polygons across the dateline
                // when polygon is close to the pole (I think) so we need to try again
                return GeometryNormalizer.apply(Orientation.CW, polygon);
            }
            return geometry;
        } else {
            return polygon;
        }
    }

    private static Geometry getGeometryAsLine(long h3) {
        final GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        if (visitor.getLeftX() > visitor.getRightX()) {
            double[] translatedXs = visitor.getXs();
            for (int i = 0; i < translatedXs.length; i++) {
                translatedXs[i] = translatedXs[i] < 0 ? translatedXs[i] + 360 : translatedXs[i];
            }
            final Geometry geometry = GeometryNormalizer.apply(Orientation.CCW, new Line(translatedXs, visitor.getYs()));
            return GeometryNormalizer.apply(Orientation.CW, geometry);
        } else {
            return new Line(visitor.getXs(), visitor.getYs());
        }
    }

    private static Geometry getGeometryAsPoints(long h3) {
        final GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        List<Point> points = new ArrayList<>();
        double[] xs = visitor.getXs();
        double[] ys = visitor.getYs();
        for (int i = 0; i < xs.length; i++) {
            points.add(new Point(xs[i], ys[i]));
        }
        return new MultiPoint(points);
    }

    private static Rectangle getGeometryAsRectangle(long h3) {
        final GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        return new Rectangle(visitor.getLeftX(), visitor.getRightX(), visitor.getMaxY(), visitor.getMinY());
    }

    public void testLongGeometriesWithDateline() throws IOException {
        long h3 = H3.geoToH3(0, 180, randomIntBetween(0, 4));
        GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        {
            Line line = new Line(new double[] { -180, 180 }, new double[] { 0, 0 });
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(line, CoordinateEncoder.GEO);
            reader.visit(visitor);
            assertEquals(GeoRelation.QUERY_CROSSES, visitor.relation());
        }
        {
            Polygon polygon = new Polygon(new LinearRing(new double[] { -180, 180, 180, -180 }, new double[] { -1, -1, 1, -1 }));
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(polygon, CoordinateEncoder.GEO);
            reader.visit(visitor);
            assertEquals(GeoRelation.QUERY_CROSSES, visitor.relation());
        }
    }

    // Testing issue with a specific cell left of the dateline that touches the dateline with one point
    // Intersecting a polygon on the other side of the dateline.
    public void testSpecificCellTouchesDateline() throws IOException {
        long h3 = 646728346019944298L;
        GeoBoundingBox box = new GeoBoundingBox(new GeoPoint(-11.29550, -180), new GeoPoint(-11.29552, -179.99999));
        GeoHexVisitor visitor = new GeoHexVisitor();
        visitor.reset(h3);
        {
            // Polygon on the same side of the dateline (overlaps)
            Polygon polygon = makePolygonFromBox(179.99999, 180, -11.29550, -11.29552);
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(polygon, CoordinateEncoder.GEO);
            reader.visit(visitor);
            assertEquals("Polygon on the same side of the dateline", GeoRelation.QUERY_CROSSES, visitor.relation());
        }
        {
            // Polygon on the other side of the dateline (just touches)
            Polygon polygon = makePolygonFromBox(-180, -179.99999, -11.29550, -11.29552);
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(polygon, CoordinateEncoder.GEO);
            reader.visit(visitor);
            assertEquals(
                "Polygon on the other side of the dateline (touches at single point)",
                GeoRelation.QUERY_CROSSES,
                visitor.relation()
            );
        }
    }

    private Polygon makePolygonFromBox(double left, double right, double top, double bottom) {
        double[] x = new double[] { left, right, right, left, left };
        double[] y = new double[] { bottom, bottom, top, top, bottom };
        LinearRing ring = new LinearRing(x, y);
        return new Polygon(ring);
    }
}
