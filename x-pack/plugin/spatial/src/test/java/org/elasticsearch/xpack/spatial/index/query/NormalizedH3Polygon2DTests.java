/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.geom.TestGeometryCollector;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.signum;
import static java.lang.Math.toDegrees;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.LATITUDE_MASK;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid3d;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.pointInterpolation;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.NEITHER;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.NORTH;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.SOUTH;
import static org.elasticsearch.xpack.spatial.index.query.NormalizedH3Polygon2D.fromH3Boundary;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class NormalizedH3Polygon2DTests extends ESTestCase {

    // private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createGeometryCollector();
    // Uncomment the following to enable export of WKT output for specific tests when debugging
    private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter(getTestClass().getSimpleName());

    public void testNorthPoleQuad() {
        for (int startLon = -180; startLon < 180; startLon += 45) {
            NormalizedH3Polygon2D polygon = polarPolygon(4, startLon, 80, 90);
            assertThat("Polygon contains north pole", polygon.pole(), is(NORTH));
        }
    }

    public void testNorthPoleHex() {
        for (int startLon = -180; startLon < 180; startLon += 45) {
            NormalizedH3Polygon2D polygon = polarPolygon(6, startLon, 80, 90);
            assertThat("Polygon contains north pole", polygon.pole(), is(NORTH));
        }
    }

    public void testSouthPoleQuad() {
        for (int startLon = -180; startLon < 180; startLon += 45) {
            NormalizedH3Polygon2D polygon = polarPolygon(4, startLon, -90, -80);
            assertThat("Polygon contains south pole", polygon.pole(), is(SOUTH));
        }
    }

    public void testSouthPoleHex() {
        for (int startLon = -180; startLon < 180; startLon += 45) {
            NormalizedH3Polygon2D polygon = polarPolygon(6, startLon, -90, -80);
            assertThat("Polygon contains south pole", polygon.pole(), is(SOUTH));
        }
    }

    public void testSafePolygon() {
        double ox = 0;
        double oy = 0;
        NormalizedH3Polygon2D polygon = rectangularPolygon(ox, oy);
        assertThat(
            "Polygon contains triangle, so triangle is NOTWITHIN polygon",
            polygon.withinTriangle(ox - 5, oy - 3, true, ox + 5, oy - 3, true, ox, oy, true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with no intersections",
            polygon.withinTriangle(ox - 50, oy - 10, true, ox + 50, oy - 10, true, ox, oy + 10, true),
            is(Component2D.WithinRelation.CANDIDATE)
        );
        assertThat(
            "Triangle contains polygon with external intersections",
            polygon.withinTriangle(ox - 20, oy - 20, true, ox + 20, oy - 20, true, ox, oy + 20, true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with internal intersections",
            polygon.withinTriangle(ox - 20, oy - 20, true, ox + 20, oy - 20, false, ox, oy + 20, false),
            is(Component2D.WithinRelation.CANDIDATE)
        );
    }

    public void testCrossesNorthernThreshold() {
        double ox = 0;
        double oy = 85;
        NormalizedH3Polygon2D polygon = rectangularPolygon(ox, oy);
        assertThat(
            "Polygon contains triangle, so triangle is NOTWITHIN polygon",
            polygon.withinTriangle(nx(ox - 5), ny(oy - 5), true, nx(ox + 5), ny(oy - 5), true, nx(ox + 0), ny(oy + 5), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with no intersections",
            polygon.withinTriangle(nx(ox - 50), ny(oy - 10), true, nx(ox + 50), ny(oy - 10), true, nx(ox + 0), ny(oy + 30), true),
            is(Component2D.WithinRelation.CANDIDATE)
        );
        assertThat(
            "Triangle contains polygon with external intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), true, nx(ox + 0), ny(oy + 10), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with internal intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), false, nx(ox + 0), ny(oy + 10), false),
            is(Component2D.WithinRelation.CANDIDATE)
        );
    }

    public void testCrossesSouthernThreshold() {
        double ox = 0;
        double oy = -85;
        NormalizedH3Polygon2D polygon = rectangularPolygon(ox, oy);
        assertThat(
            "Polygon contains triangle, so triangle is NOTWITHIN polygon",
            polygon.withinTriangle(nx(ox - 5), ny(oy - 5), true, nx(ox + 5), ny(oy - 5), true, nx(ox + 0), ny(oy + 5), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with no intersections",
            polygon.withinTriangle(nx(ox - 50), ny(oy - 10), true, nx(ox + 50), ny(oy - 10), true, nx(ox + 0), ny(oy + 30), true),
            is(Component2D.WithinRelation.CANDIDATE)
        );
        assertThat(
            "Triangle contains polygon with external intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), true, nx(ox + 0), ny(oy + 10), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with internal intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), false, nx(ox + 0), ny(oy + 10), false),
            is(Component2D.WithinRelation.CANDIDATE)
        );
    }

    public void testCrossesDateline() {
        double ox = 180;
        double oy = 0;
        NormalizedH3Polygon2D polygon = rectangularPolygon(ox, oy);

        assertThat(
            "Polygon contains triangle, so triangle is NOTWITHIN polygon",
            polygon.withinTriangle(nx(ox - 5), ny(oy - 5), true, nx(ox + 5), ny(oy - 5), true, nx(ox + 0), ny(oy + 5), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with no intersections",
            polygon.withinTriangle(nx(ox - 50), ny(oy - 10), true, nx(ox + 50), ny(oy - 10), true, nx(ox + 0), ny(oy + 30), true),
            is(Component2D.WithinRelation.CANDIDATE)
        );
        assertThat(
            "Triangle contains polygon with external intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), true, nx(ox + 0), ny(oy + 10), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with internal intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), false, nx(ox + 0), ny(oy + 10), false),
            is(Component2D.WithinRelation.CANDIDATE)
        );
    }

    public void testCrossesDatelineAndNorthernThreshold() {
        double ox = 180;
        double oy = 85;
        NormalizedH3Polygon2D polygon = rectangularPolygon(ox, oy);

        assertThat(
            "Polygon contains triangle, so triangle is NOTWITHIN polygon",
            polygon.withinTriangle(nx(ox - 5), ny(oy - 5), true, nx(ox + 5), ny(oy - 5), true, nx(ox + 0), ny(oy + 5), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with no intersections",
            polygon.withinTriangle(nx(ox - 50), ny(oy - 10), true, nx(ox + 50), ny(oy - 10), true, nx(ox + 0), ny(oy + 30), true),
            is(Component2D.WithinRelation.CANDIDATE)
        );
        assertThat(
            "Triangle contains polygon with external intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), true, nx(ox + 0), ny(oy + 10), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with internal intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), false, nx(ox + 0), ny(oy + 10), false),
            is(Component2D.WithinRelation.CANDIDATE)
        );
    }

    public void testCrossesDatelineAndSouthernThreshold() {
        double ox = 180;
        double oy = -85;
        NormalizedH3Polygon2D polygon = rectangularPolygon(ox, oy);

        assertThat(
            "Polygon contains triangle, so triangle is NOTWITHIN polygon",
            polygon.withinTriangle(nx(ox - 5), ny(oy - 5), true, nx(ox + 5), ny(oy - 5), true, nx(ox + 0), ny(oy + 5), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with no intersections",
            polygon.withinTriangle(nx(ox - 50), ny(oy - 10), true, nx(ox + 50), ny(oy - 10), true, nx(ox + 0), ny(oy + 30), true),
            is(Component2D.WithinRelation.CANDIDATE)
        );
        assertThat(
            "Triangle contains polygon with external intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), true, nx(ox + 0), ny(oy + 10), true),
            is(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Triangle contains polygon with internal intersections",
            polygon.withinTriangle(nx(ox - 20), ny(oy - 15), true, nx(ox + 20), ny(oy - 15), false, nx(ox + 0), ny(oy + 10), false),
            is(Component2D.WithinRelation.CANDIDATE)
        );
    }

    public void testTroublesomeCell_NorthPole() {
        testGeometryCollector.start("testTroublesomeCell_NorthPole");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        // This cell failed with latitudeThreshold at 70
        TestCaptureListener listener = new TestCaptureListener();
        CellBoundary cell = H3.h3ToGeoBoundary("8001fffffffffff");
        NormalizedH3Polygon2D polygon = fromH3Boundary(cell, NORTH, LATITUDE_MASK, listener);
        assertCapturedResults("North pole", listener, LATITUDE_MASK, -31.8312804990, 31.8312804990, -146.2180383587649, 134.93902584878086);
        collector.addPolygon(listener.lons, listener.lats);

        listener = new TestCaptureListener();
        polygon = fromH3Boundary(cell, NORTH, 70, listener);
        assertCapturedResults("North pole at 70", listener, 70, -31.8312804990, 31.8312804990, 24.134853951596615, 65.19377055877843);
        collector.addPolygon(listener.lons, listener.lats);

        listener = new TestCaptureListener();
        polygon = fromScaledH3Boundary(cell, NORTH, LATITUDE_MASK, listener, 1.01);
        assertCapturedResults(
            "North pole scaled 1%",
            listener,
            LATITUDE_MASK,
            -31.79342991289,
            31.79342991289,
            -146.7903467237,
            136.9517255672
        );
        collector.addPolygon(listener.lons, listener.lats);

        listener = new TestCaptureListener();
        polygon = fromScaledH3Boundary(cell, NORTH, 70, listener, 1.01);
        assertCapturedResults("North pole at 70 scaled 1%", listener, 70, -31.79342991289, 31.79342991289, 23.33944706667, 65.8140838526);
        collector.addPolygon(listener.lons, listener.lats);

        listener = new TestCaptureListener();
        polygon = fromScaledH3Boundary(cell, NORTH, LATITUDE_MASK, listener, 1.17);
        assertCapturedResults(
            "North pole scaled 17%",
            listener,
            LATITUDE_MASK,
            -31.23601631730,
            31.23601631730,
            -163.8646413706,
            164.0966561844
        );
        collector.addPolygon(listener.lons, listener.lats);

        listener = new TestCaptureListener();
        polygon = fromScaledH3Boundary(cell, NORTH, 70, listener, 1.17);
        assertCapturedResults("North pole at 70 scaled 17%", listener, 70, -31.23601631730, 31.23601631730, 10.55560802383, 75.8793947031);
        collector.addPolygon(listener.lons, listener.lats);

        testGeometryCollector.stop();
    }

    private NormalizedH3Polygon2D fromScaledH3Boundary(
        CellBoundary boundary,
        int pole,
        double latitudeThreshold,
        NormalizedH3Polygon2D.Listener listener,
        double scaleFactor
    ) {
        double[] lats = new double[boundary.numPoints() + 1];
        double[] lons = new double[boundary.numPoints() + 1];
        GeoPoint centroid = calculateCentroid3d(boundary);
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertex = boundary.getLatLon(i);
            GeoPoint geoPoint = new GeoPoint(PlanetModel.SPHERE, vertex.getLatRad(), vertex.getLonRad());
            GeoPoint scaled = pointInterpolation(centroid, geoPoint, scaleFactor);
            lats[i] = toDegrees(scaled.getLatitude());
            lons[i] = toDegrees(scaled.getLongitude());
        }
        lats[lats.length - 1] = lats[0];
        lons[lons.length - 1] = lons[0];
        return new NormalizedH3Polygon2D(lats, lons, pole, latitudeThreshold, listener);
    }

    private void assertCapturedResults(
        String description,
        TestCaptureListener listener,
        double maxLat,
        double offset,
        double lonAtMinLat,
        double minLon,
        double maxLon
    ) {
        assertThat(description + ": truncated at max latitude", listener.maxLat, is(maxLat));
        // assertThat(description + ": offset", listener.offset, closeTo(offset, 1e-10));
        // assertThat(description + ": longitude at minimum Latitude", listener.lonAtMinLat, closeTo(lonAtMinLat, 1e-10));
        assertThat(description + ": minimum longitude", listener.minLon, closeTo(minLon, 1e-10));
        assertThat(description + ": maximum longitude", listener.maxLon, closeTo(maxLon, 1e-10));
    }

    public void testTroublesomeCell_NotNorthPole() {
        // This cell failed with latitudeThreshold at 70
        NormalizedH3Polygon2D polygon = fromH3Boundary(H3.h3ToGeoBoundary("8003fffffffffff"), NEITHER, 70, null);
        assertThat("Near north pole H3 cell", polygon.pole(), is(NEITHER));
    }

    public void testTroublesomeCell_SouthPole() {
        // This cell failed with latitudeThreshold at 70
        NormalizedH3Polygon2D polygon = fromH3Boundary(H3.h3ToGeoBoundary("80f3fffffffffff"), SOUTH, 70, null);
        assertThat("South pole H3 cell", polygon.pole(), is(SOUTH));
    }

    public void testDatelineCellLevel0() {
        testGeometryCollector.start("testDatelineCellLevel0");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        double x = 146.58268694992358;
        double y = 68.64804543269979;
        long h3 = H3.geoToH3(y, x, 0);
        String special = H3.h3ToString(h3);
        CellBoundary boundary = H3.h3ToGeoBoundary(special);
        TestGeometryListener listener = new TestGeometryListener(testGeometryCollector, h3, null);
        NormalizedH3Polygon2D polygon = fromH3Boundary(boundary, NEITHER, LATITUDE_MASK, listener);
        Point centroid = calculateCentroid(boundary);
        collector.addPoint(centroid.getX(), centroid.getY());
        Point[] inside = new Point[boundary.numPoints()];
        Point[] outside = new Point[boundary.numPoints()];
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertex = boundary.getLatLon(i);
            inside[i] = pointInterpolation(centroid, new Point(vertex.getLonDeg(), vertex.getLatDeg()), 0.9);
            outside[i] = pointInterpolation(centroid, new Point(vertex.getLonDeg(), vertex.getLatDeg()), 1.1);
            collector.addPoint(inside[i].getLon(), inside[i].getLat());
            collector.addPoint(outside[i].getLon(), outside[i].getLat());
        }
        testGeometryCollector.stop();
        for (Point in : inside) {
            boolean expectedContains = true;
            PointValues.Relation expectedRelated = PointValues.Relation.CELL_INSIDE_QUERY;
            if (in.getLat() > LATITUDE_MASK) {
                expectedContains = false;
                expectedRelated = PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            assertThat("Cell contains inside point " + in, polygon.contains(in.getX(), in.getY()), is(expectedContains));
            PointValues.Relation related = polygon.relate(in.getX(), in.getX(), in.getY(), in.getY());
            assertThat("Cell related to inside point " + in, related, is(expectedRelated));
        }
        for (Point out : outside) {
            boolean expectedContains = false;
            PointValues.Relation expectedRelated = PointValues.Relation.CELL_OUTSIDE_QUERY;
            assertThat("Cell does not contain outside point " + out, polygon.contains(out.getX(), out.getY()), is(expectedContains));
            PointValues.Relation related = polygon.relate(out.getX(), out.getX(), out.getY(), out.getY());
            assertThat("Cell related to outside point " + out, related, is(expectedRelated));
        }
    }

    public void testAllH3CellsLevel0() {
        testGeometryCollector.start("testAllH3CellsLevel0");
        assertH3CellsTruncateToBounds(H3.getLongRes0Cells(), LATITUDE_MASK, 0);
        testGeometryCollector.stop((normal, special) -> {
            assertThat(normal.size(), is(120));
            assertThat(special.size(), is(2));
        });
    }

    public void testAllH3CellsLevel1() {
        testGeometryCollector.start("testAllH3CellsLevel1");
        for (long h30 : H3.getLongRes0Cells()) {
            assertH3CellsTruncateToBounds(H3.h3ToChildren(h30), LATITUDE_MASK, 1);
        }
        testGeometryCollector.stop((normal, special) -> {
            assertThat(normal.size(), is(840));
            assertThat(special.size(), is(2));
        });
    }

    public void testAllH3CellsLevel2() {
        testGeometryCollector.start("testAllH3CellsLevel2");
        for (long h30 : H3.getLongRes0Cells()) {
            for (long h31 : H3.h3ToChildren(h30)) {
                assertH3CellsTruncateToBounds(H3.h3ToChildren(h31), LATITUDE_MASK, 2);
            }
        }
        testGeometryCollector.stop((normal, special) -> {
            assertThat(normal.size(), is(5872));
            assertThat(special.size(), is(0)); // at this precision the polar cells are outside the latitude range
        });
    }

    public void testAllH3CellsLevel0_70() {
        testGeometryCollector.start("testAllH3CellsLevel0_70");
        assertH3CellsTruncateToBounds(H3.getLongRes0Cells(), 70, 0);
        testGeometryCollector.stop((normal, special) -> {
            // Even truncating to 70, we still see all 122 H3 cells
            assertThat(normal.size(), is(120));
            assertThat(special.size(), is(2));
        });
    }

    public void testAllH3CellsLevel1_70() {
        testGeometryCollector.start("testAllH3CellsLevel1_70");
        for (long h30 : H3.getLongRes0Cells()) {
            assertH3CellsTruncateToBounds(H3.h3ToChildren(h30), 70, 1);
        }
        testGeometryCollector.stop((normal, special) -> {
            assertThat(normal.size(), is(812));
            assertThat(special.size(), is(0)); // at this threshold and precision the polar cells are outside the latitude range
        });
    }

    public void testAllH3CellsLevel2_70() {
        testGeometryCollector.start("testAllH3CellsLevel2_70");
        for (long h30 : H3.getLongRes0Cells()) {
            for (long h31 : H3.h3ToChildren(h30)) {
                assertH3CellsTruncateToBounds(H3.h3ToChildren(h31), 70, 2);
            }
        }
        testGeometryCollector.stop((normal, special) -> {
            assertThat(normal.size(), is(5592));
            assertThat(special.size(), is(0)); // at this threshold and precision the polar cells are outside the latitude range
        });
    }

    private void assertH3CellsTruncateToBounds(long[] cells, double latitudeThreshold, int precision) {
        long northPole = H3.geoToH3(90, 0, precision);
        long southPole = H3.geoToH3(-90, 0, precision);
        String[] poles = new String[] { H3.h3ToString(northPole), H3.h3ToString(southPole) };
        for (long h3 : cells) {
            NormalizedH3Polygon2D.Listener listener = new TestGeometryListener(testGeometryCollector, h3, poles);
            try {
                int pole = h3 == northPole ? NORTH : h3 == southPole ? SOUTH : NEITHER;
                NormalizedH3Polygon2D polygon = fromH3Boundary(H3.h3ToGeoBoundary(h3), pole, latitudeThreshold, listener);
                assertThat("Is cell polar: " + H3.h3ToString(h3), polygon.pole(), is(pole));
            } catch (Exception e) {
                assertThat("Cell outside latitude bounds", h3, isOutsideBounds(-latitudeThreshold, latitudeThreshold));
            }
        }
    }

    private NormalizedH3Polygon2D rectangularPolygon(double ox, double oy) {
        double[] lats = new double[] { -5, -5, 5, 5, -5 };
        double[] lons = new double[] { -10, 10, 10, -10, -10 };
        shift(lats, oy);
        shift(lons, ox);
        normalizeLats(lats);
        normalizeLons(lons);
        TestCaptureListener listener = new TestCaptureListener();
        NormalizedH3Polygon2D polygon = new NormalizedH3Polygon2D(lats, lons, NEITHER, LATITUDE_MASK, listener);
        assertThat("Offset", listener.offset, equalTo(ox));
        assertThat("MinX", polygon.getMinX(), equalTo(nx(ox - 10d)));
        assertThat("MaxX", polygon.getMaxX(), equalTo(nx(ox + 10d)));
        assertThat("MinY", polygon.getMinY(), equalTo(max(ny(oy - 5d), -LATITUDE_MASK)));
        assertThat("MaxY", polygon.getMaxY(), equalTo(min(ny(oy + 5d), LATITUDE_MASK)));
        for (int ix = -20; ix <= 20; ix += 10) {
            for (int iy = -10; iy <= 10; iy += 5) {
                double x = nx(ox + ix);
                double y = ny(oy + iy);
                assertContainsPoint(polygon, x, y);
                assertContainsLine(polygon, ox, oy, x, y);
                assertContainsTriangle(polygon, nx(ox - 2), ny(oy - 1), nx(ox + 2), ny(oy + 1), x, y);
                assertContainsTriangle(polygon, nx(ox - 30), ny(oy - 20), nx(ox + 2), ny(oy + 1), x, y);
            }
        }
        return polygon;
    }

    private NormalizedH3Polygon2D polarPolygon(int segments, double startLon, double minLat, double maxLat) {
        assertThat("Max>min", maxLat, greaterThan(minLat));
        double height = maxLat - minLat;
        double[] lats = new double[segments + 1];
        double[] lons = new double[segments + 1];
        int pole = (int) signum(minLat);
        for (int i = 0; i < segments; i++) {
            lons[i] = startLon + i * 360d / segments;
            double factor = 2d * Math.abs(segments / 2d - i) / segments;
            if (pole == NORTH) {
                lats[i] = minLat + height * factor;
            } else {
                lats[i] = maxLat - height * factor;
            }
        }
        lats[lats.length - 1] = lats[0];
        lons[lons.length - 1] = lons[0];
        normalizeLats(lats);
        normalizeLons(lons);
        TestCaptureListener listener = new TestCaptureListener();
        NormalizedH3Polygon2D polygon = new NormalizedH3Polygon2D(lats, lons, pole, LATITUDE_MASK, listener);
        assertThat("Polygon contains a pole", polygon.pole(), is(not(NEITHER)));
        String prefix = "Polygon of " + segments + " edges with max latitude at " + startLon;
        double expectedOffset = nx(180 - startLon);
        double expectedPeakLon = nx(startLon - 180);
        assertThat(prefix + ": Offset", listener.offset, closeTo(expectedOffset, 1e-10));
        if (pole == NORTH) {
            assertThat(prefix + ": Lon at minLat", listener.lonAtMinLat, equalTo(expectedPeakLon));
        } else {
            assertThat(prefix + ": Lon at maxLat", listener.lonAtMaxLat, equalTo(expectedPeakLon));
        }
        double latToThreshold = pole == NORTH ? LATITUDE_MASK - minLat : maxLat + LATITUDE_MASK;
        double latFactor = latToThreshold / height;
        double diffX = latFactor * 180d;
        double minX = nx(expectedPeakLon - diffX);
        double maxX = nx(expectedPeakLon + diffX);
        assertThat(prefix + ": MinX", polygon.getMinX(), closeTo(minX, 1e-10));
        assertThat(prefix + ": MaxX", polygon.getMaxX(), closeTo(maxX, 1e-10));
        assertThat(prefix + ": MinY", polygon.getMinY(), equalTo(max(ny(minLat), -LATITUDE_MASK)));
        assertThat(prefix + ": MaxY", polygon.getMaxY(), equalTo(min(ny(maxLat), LATITUDE_MASK)));
        for (double x = -180; x <= 180; x += 10) {
            for (double y = minLat; y <= maxLat; y += 5) {
                boolean expected = polygon.contains(x, y);
                assertThat(prefix + " contains point: (" + x + "," + y + ")", polygon.contains(x, y), is(expected));
                if (minLat < 0) {
                    // South pole
                    assertContainsLine(polygon, x, polygon.getMinY(), x, y, expected);
                    assertContainsLine(polygon, x, minLat, x, y, false);
                } else {
                    // North pole
                    assertContainsLine(polygon, x, polygon.getMaxY(), x, y, expected);
                    assertContainsLine(polygon, x, maxLat, x, y, false);
                }
            }
        }
        return polygon;
    }

    /**
     * Capture the internal polygons for testing expected behaviour.
     */
    private static class TestCaptureListener implements NormalizedH3Polygon2D.Listener {
        double minLon = Float.MAX_VALUE;
        double maxLon = -Float.MAX_VALUE;
        double minLat = Float.MAX_VALUE;
        double maxLat = -Float.MAX_VALUE;
        double lonAtMinLat = Float.NaN;
        double lonAtMaxLat = Float.NaN;
        double offset = Float.NaN;
        double[] lats;
        double[] lons;

        @Override
        public void polygon(double[] lats, double[] lons, double offset) {
            this.lats = lats;
            this.lons = lons;
            this.offset = offset;
            for (int i = 0; i < lons.length; i++) {
                double lon = nx(lons[i] - offset);
                minLon = min(minLon, lon);
                maxLon = max(maxLon, lon);
                if (lats[i] < minLat) {
                    minLat = lats[i];
                    lonAtMinLat = lon;
                }
                if (lats[i] > maxLat) {
                    maxLat = lats[i];
                    lonAtMaxLat = lon;
                }
            }
        }
    }

    /**
     * Capture the internal polygons and draw them to WKT, correcting for offsets.
     */
    private static class TestGeometryListener implements NormalizedH3Polygon2D.Listener {
        private final TestGeometryCollector collector;
        private final long h3;
        private final String[] highlightCells;

        TestGeometryListener(TestGeometryCollector collector, long h3, String[] highlightCells) {
            this.collector = collector;
            this.h3 = h3;
            this.highlightCells = highlightCells == null ? new String[0] : highlightCells;
        }

        @Override
        public void polygon(double[] lats, double[] lons, double offset) {
            boolean highlight = false;
            for (String cell : highlightCells) {
                if (H3.h3ToString(h3).equals(cell)) {
                    highlight = true;
                    break;
                }
            }
            addPolygon(lats, lons, offset, highlight);
        }

        private void addPolygon(double[] lats, double[] lons, double offset, boolean highlight) {
            double[] olons = lons.clone();
            for (int i = 0; i < lons.length; i++) {
                olons[i] = lons[i] - offset;
            }
            collector.highlighted(highlight).addPolygon(olons, lats);
        }
    }

    // Shift a set of coordinates by a fixed amount
    private static void shift(double[] data, double offset) {
        for (int i = 0; i < data.length; i++) {
            data[i] += offset;
        }
    }

    // Truncate latitudes into the -90:90 range
    private static void normalizeLats(double[] lats) {
        for (int i = 0; i < lats.length; i++) {
            lats[i] = ny(lats[i]);
        }
    }

    private static double ny(double y) {
        return max(-90, min(90, y));
    }

    // Wrap longitudes around the dateline
    private static void normalizeLons(double[] lons) {
        for (int i = 0; i < lons.length; i++) {
            lons[i] = nx(lons[i]);
        }
    }

    private static double nx(double x) {
        while (x > 180) {
            x -= 360;
        }
        while (x <= -180) {
            x += 360;
        }
        return x;
    }

    private void assertContainsPoint(NormalizedH3Polygon2D p, double x, double y) {
        assertContainsPoint(p, x, y, p.getMinX(), p.getMaxX());
    }

    /** when the polygon is split, we need explicit x-ranges */
    private void assertContainsPoint(NormalizedH3Polygon2D p, double x, double y, double minX, double maxX) {
        boolean expected = xInRange(x, minX, maxX) && yInRange(y, p.getMinY(), p.getMaxY());
        assertThat("Contains point: (" + x + "," + y + ")", p.contains(x, y), is(expected));
    }

    private boolean xInRange(double x, double minX, double maxX) {
        if (minX > maxX) {
            return xInRange(nx(180 + x), nx(180 + minX), nx(180 + maxX));
        } else {
            return x >= minX && x <= maxX;
        }
    }

    private boolean yInRange(double y, double minY, double maxY) {
        return y >= minY && y <= maxY;
    }

    private void assertContainsLine(NormalizedH3Polygon2D p, double ax, double ay, double bx, double by) {
        assertContainsLine(p, ax, ay, bx, by, p.getMinX(), p.getMaxX());
    }

    private void assertContainsLine(NormalizedH3Polygon2D p, double ax, double ay, double bx, double by, double minX, double maxX) {
        boolean expectedA = xInRange(ax, minX, maxX) && yInRange(ay, p.getMinY(), p.getMaxY());
        boolean expectedB = xInRange(bx, minX, maxX) && yInRange(by, p.getMinY(), p.getMaxY());
        assertContainsLine(p, ax, ay, bx, by, expectedA && expectedB);
    }

    private void assertContainsLine(NormalizedH3Polygon2D p, double ax, double ay, double bx, double by, boolean expected) {
        assertThat("Contains line: (" + ax + "," + ay + ")->(" + bx + "," + by + ")", p.containsLine(ax, ay, bx, by), is(expected));
    }

    private void assertContainsTriangle(NormalizedH3Polygon2D p, double ax, double ay, double bx, double by, double cx, double cy) {
        assertContainsTriangle(p, ax, ay, bx, by, cx, cy, p.getMinX(), p.getMaxX());
    }

    private void assertContainsTriangle(
        NormalizedH3Polygon2D p,
        double ax,
        double ay,
        double bx,
        double by,
        double cx,
        double cy,
        double minX,
        double maxX
    ) {
        boolean expectedA = xInRange(ax, minX, maxX) && yInRange(ay, p.getMinY(), p.getMaxY());
        boolean expectedB = xInRange(bx, minX, maxX) && yInRange(by, p.getMinY(), p.getMaxY());
        boolean expectedC = xInRange(cx, minX, maxX) && yInRange(cy, p.getMinY(), p.getMaxY());
        assertContainsTriangle(p, ax, ay, bx, by, cx, cy, expectedA && expectedB && expectedC);
    }

    private void assertContainsTriangle(
        NormalizedH3Polygon2D p,
        double ax,
        double ay,
        double bx,
        double by,
        double cx,
        double cy,
        boolean expected
    ) {
        assertThat(
            "Contains triangle: (" + ax + "," + ay + ")->(" + bx + "," + by + ")->(" + cx + "," + cy + ")",
            p.containsTriangle(ax, ay, bx, by, cx, cy),
            is(expected)
        );
    }

    private Matcher<Long> isOutsideBounds(double min, double max) {
        return new TestH3OutsideBoundsMatcher(min, max);
    }

    private static class TestH3OutsideBoundsMatcher extends BaseMatcher<Long> {
        private final double min;
        private final double max;

        private TestH3OutsideBoundsMatcher(double min, double max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public boolean matches(Object actual) {
            if (actual instanceof Long h3) {
                CellBoundary boundary = H3.h3ToGeoBoundary(h3);
                for (int i = 0; i < boundary.numPoints(); i++) {
                    LatLng vertext = boundary.getLatLon(i);
                    double lat = vertext.getLatDeg();
                    if (lat <= max && lat >= min) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("H3 cell outside latitude bounds (" + min + ".." + max + ")");
        }
    }
}
