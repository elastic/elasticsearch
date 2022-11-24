/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.geom.TestGeometryCollector;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * The behaviour of H3Polygon2D is mostly done in NormalizedH3Polygon2DTests, but we have a few specific tests her to focus on edge cases.
 */
public class H3Polygon2DTests extends ESTestCase {
    // TestGeometryCollector testGeometryCollector = TestGeometryCollector.createGeometryCollector();
    TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter("H3Polygon2DTests");

    public void testPolygon() {
        testGeometryCollector.start("testPolygon", 0, 10);
        TestInspector inspector = new TestInspector(testGeometryCollector.normal());
        Point point = new Point(0, -76);
        String h3Address = H3.geoToH3Address(point.getLat(), point.getLon(), 0);
        H3Polygon2D.Planar.Unscaled unscaled = new H3Polygon2D.Planar.Unscaled(h3Address);
        H3Polygon2D.Planar.Scaled scaledA = new H3Polygon2D.Planar.Scaled(h3Address, 1.1);
        H3Polygon2D.Planar.Scaled scaledB = new H3Polygon2D.Planar.Scaled(h3Address, 1.2);
        H3Polygon2D.Planar.Scaled scaledC = new H3Polygon2D.Planar.Scaled(h3Address, 1.3);
        H3Polygon2D.Planar.Scaled scaledD = new H3Polygon2D.Planar.Scaled(h3Address, 1.4);
        unscaled.inspect(inspector);
        scaledA.inspect(inspector);
        scaledB.inspect(inspector);
        scaledC.inspect(inspector);
        scaledD.inspect(inspector);
        testGeometryCollector.stop((normal, special) -> {
            assertThat("All should be normal", normal.size(), is(5));
            assertThat("All should be normal", special.size(), is(0));
        });
        assertAllPointsWithinPolygon(scaledA, unscaled);
        assertAllPointsWithinPolygon(scaledB, unscaled);
        assertAllPointsWithinPolygon(scaledC, unscaled);
        assertAllPointsWithinPolygon(scaledD, unscaled);
        assertAllPointsWithinPolygon(scaledB, scaledA);
        assertAllPointsWithinPolygon(scaledC, scaledB);
        assertAllPointsWithinPolygon(scaledD, scaledC);
    }

    public void testHighLatitudePointInPolygon() {
        testGeometryCollector.start("testHighLatitudePointInPolygon");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        TestInspector inspector = new TestInspector(collector);
        String parentAddress = "8001fffffffffff";
        String childAddress = "81007ffffffffff";
        Point point = new Point(0, 81.59665378276259);
        collector.addPoint(point);
        // System.out.println("Precision\t1.0 \t1.17");
        for (int precision = 0; precision < H3.MAX_H3_RES; precision++) {
            long h3 = H3.geoToH3(point.getLat(), point.getLon(), precision);
            String address = H3.h3ToString(h3);
            collector.addH3Cell(address);
            H3Polygon2D.Planar cell = new H3Polygon2D.Planar.Unscaled(address);
            H3Polygon2D.Planar inflated = new H3Polygon2D.Planar.Scaled(address, 1.17);
            cell.inspect(inspector);
            inflated.inspect(inspector);
            boolean cellContainsPoint = cell.contains(point.getX(), point.getY());
            boolean inflatedContainsPoint = inflated.contains(point.getX(), point.getY());
            // System.out.printf("%d \t%b \t%b\n", precision, cellContainsPoint, inflatedContainsPoint);
            assertThat("Inflation should not change result", inflatedContainsPoint, is(cellContainsPoint));
        }
        testGeometryCollector.stop();
    }

    public void testHighLatitudePointInPolygonLevel0() {
        // TODO this test was used to find and fix an issue with inflated polar cells that also crossed the dateline.
        // However, it now also shows that the point-in-polygon function gives slightly different results than expected
        // There is no assertion for that, but it can be seen in the printed output
        testGeometryCollector.start("testHighLatitudePointInPolygonLevel0");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        TestInspector inspector = new TestInspector(collector);
        String address = "8001fffffffffff";
        collector.addH3Cell(address);
        H3Polygon2D.Planar cell = new H3Polygon2D.Planar.Unscaled(address);
        H3Polygon2D.Planar inflated = new H3Polygon2D.Planar.Scaled(address, 1.17);
        cell.inspect(inspector);
        inflated.inspect(inspector);
        int width = 200;
        int height = 100;
        double minX = -170;
        double maxX = 70;
        double minY = 65.00;
        double maxY = 85.00;
        collector.addBox(minX, maxX, minY, maxY);
        for (int yi = 0; yi < height; yi++) {
            for (int xi = 0; xi < width; xi++) {
                Point point = new Point(minX + (maxX - minX) * xi / width, minY + (maxY - minY) * yi / height);
                boolean cellContainsPoint = cell.contains(point.getX(), point.getY());
                boolean inflatedContainsPoint = inflated.contains(point.getX(), point.getY());
                if (inflatedContainsPoint != cellContainsPoint) {
                    collector.addPoint(point);
                }
                // assertThat("Inflation should not change result", inflatedContainsPoint, is(cellContainsPoint));
            }
        }
        testGeometryCollector.stop();
    }

    public void testInflatedContainsAllChildren() throws Exception {
        String cell = H3.geoToH3Address(0, 0, 0);
        H3LatLonGeometry hexagon = new H3LatLonGeometry.Planar.Scaled(cell, 1.174);
        Component2D component2D = LatLonGeometry.create(hexagon);
        testInflatedContainsAllChildren(component2D, cell);
    }

    private void testInflatedContainsAllChildren(Component2D component2D, String cell) throws Exception {
        if (H3.getResolution(cell) == 5) {
            return;
        }
        String[] children = H3.h3ToChildren(cell);
        for (String child : children) {
            assertThat(cell, GeoTestUtils.geoShapeValue(getGeoPolygon(child)).relate(component2D), not(is(GeoRelation.QUERY_DISJOINT)));
            testInflatedContainsAllChildren(component2D, child);
        }
    }

    private Polygon getGeoPolygon(String h3Address) {
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
        double[] x = new double[cellBoundary.numPoints() + 1];
        double[] y = new double[cellBoundary.numPoints() + 1];
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng latLng = cellBoundary.getLatLon(i);
            x[i] = latLng.getLonDeg();
            y[i] = latLng.getLatDeg();
        }
        x[x.length - 1] = x[0];
        y[y.length - 1] = y[0];
        LinearRing ring = new LinearRing(x, y);
        return new Polygon(ring);
    }

    private void assertAllPointsWithinPolygon(H3Polygon2D bigger, H3Polygon2D smaller) {
        bigger.inspect(new TestComparisonInspector(smaller, false));
        smaller.inspect(new TestComparisonInspector(bigger, true));
    }

    private static class TestInspector implements H3Polygon2D.CellInspector {
        private final TestGeometryCollector.Collector collector;

        private TestInspector(TestGeometryCollector.Collector collector) {
            this.collector = collector;
        }

        @Override
        public void inspect(long h3, int res, double minX, double maxX, double minY, double maxY, List<Point> boundary) {
            collector.addPolygon(boundary);
        }
    }

    private static class TestComparisonInspector implements H3Polygon2D.CellInspector {
        private H3Polygon2D polygon;
        private boolean polygonIsBigger;

        private TestComparisonInspector(H3Polygon2D polygon, boolean polygonIsBigger) {
            this.polygon = polygon;
            this.polygonIsBigger = polygonIsBigger;
        }

        @Override
        public void inspect(long h3, int res, double minX, double maxX, double minY, double maxY, List<Point> boundary) {
            for (Point point : boundary) {
                if (point.getY() < GeoTileUtils.LATITUDE_MASK && point.getY() > -GeoTileUtils.LATITUDE_MASK) {
                    assertThat("Polygon.contains(" + point + ")", polygon.contains(point.getX(), point.getY()), is(polygonIsBigger));
                }
            }
        }
    }
}
