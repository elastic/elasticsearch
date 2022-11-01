/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.geom;

import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.Vector;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;

import static org.apache.lucene.spatial3d.geom.Spatial3DTestUtil.containsHorizontalLine;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid3d;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.pointInterpolation;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeThat;

/**
 * TODO: Consider moving these tests and GeoRegularConvexPolygon into Lucene spatial3d library itself.
 */
public class GeoRegularConvexPolygonTests extends ESTestCase {

    private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createGeometryCollector();
    // Uncomment the following to enable export of WKT output for specific tests when debugging
    // private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter(getTestClass().getSimpleName());

    public void testSimpleTriangle() {
        GeoPoint left = new GeoPoint(PlanetModel.SPHERE, 0, -0.1);
        GeoPoint right = new GeoPoint(PlanetModel.SPHERE, 0, 0.1);
        GeoPoint top = new GeoPoint(PlanetModel.SPHERE, 0.2, 0);
        simpleShapeShouldBeValid(left, right, top);
    }

    public void testSimpleSquare() {
        GeoPoint left = new GeoPoint(PlanetModel.SPHERE, 0, -0.1);
        GeoPoint right = new GeoPoint(PlanetModel.SPHERE, 0, 0.1);
        GeoPoint top = new GeoPoint(PlanetModel.SPHERE, 0.1, 0);
        GeoPoint bottom = new GeoPoint(PlanetModel.SPHERE, -0.1, 0);
        simpleShapeShouldBeValid(left, bottom, right, top);
    }

    public void testSimpleHexagon() {
        simpleShapeShouldBeValid(
            new GeoPoint(PlanetModel.SPHERE, 0, -0.1),
            new GeoPoint(PlanetModel.SPHERE, -0.1, -0.02),
            new GeoPoint(PlanetModel.SPHERE, -0.1, 0.02),
            new GeoPoint(PlanetModel.SPHERE, 0, 0.1),
            new GeoPoint(PlanetModel.SPHERE, 0.1, 0.02),
            new GeoPoint(PlanetModel.SPHERE, 0.1, -0.02)
        );
    }

    public void testH3Hexagon() {
        testGeometryCollector.start("testH3Hexagon");
        double latitude = randomDoubleBetween(-85, 85, false);
        double longitude = randomDoubleBetween(-180, 180, true);
        int resolution = randomIntBetween(0, H3.MAX_H3_RES);
        String h3Address = H3.geoToH3Address(latitude, longitude, resolution);
        CellBoundary boundary = H3.h3ToGeoBoundary(h3Address);
        assumeThat("This test only works with true convex hexagons", boundary.numPoints(), equalTo(6));
        GeoPoint centroid = calculateCentroid3d(boundary);
        GeoPoint[] points = new GeoPoint[boundary.numPoints()];
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng latlng = boundary.getLatLon(i);
            points[i] = new GeoPoint(PlanetModel.SPHERE, latlng.getLatRad(), latlng.getLonRad());
        }
        GeoPoint[] inner = new GeoPoint[boundary.numPoints() / 2];
        GeoPoint[] outer = new GeoPoint[boundary.numPoints() / 2];
        for (int i = 0; i < boundary.numPoints() / 2; i++) {
            GeoPoint point = pointInterpolation(points[i * 2], points[i * 2 + 1], 0.5);
            inner[i] = pointInterpolation(centroid, point, 0.9);
            outer[i] = pointInterpolation(centroid, point, 3.0);
        }
        // debug geometries
        testGeometryCollector.normal(c -> {
            c.addPoint(centroid);
            c.addPolygon(points);
            c.addPolygon(inner);
            c.addPolygon(outer);
        });

        // Prepare test
        GeoPolygon hexagon = GeoRegularConvexPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
        GeoPolygon innerTriangle = GeoRegularConvexPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, inner);
        GeoPolygon outerTriangle = GeoRegularConvexPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, outer);
        GeoPolygon hexGeom = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, Arrays.asList(points));
        GeoPolygon innGeom = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, Arrays.asList(inner));
        GeoPolygon outGeom = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, Arrays.asList(outer));
        HashMap<GeoPolygon, String> names = new HashMap<>();
        names.put(hexagon, "Hexagon");
        names.put(innerTriangle, "Inner Triangle");
        names.put(outerTriangle, "Outer Triangle");
        names.put(hexGeom, "HexGeom");
        names.put(innGeom, "InnGeom");
        names.put(outGeom, "OutGeom");
        GeoPolygon[] allPolygons = new GeoPolygon[] { innerTriangle, outerTriangle, hexagon, innGeom, outGeom, hexGeom };
        for (GeoPolygon polygon : allPolygons) {
            String name = names.get(polygon);
            for (GeoPolygon other : allPolygons) {
                String otherName = names.get(other);
                boolean samePolygon = name.substring(0, 3).equals(otherName.substring(0, 3));
                int relates = samePolygon ? GeoArea.OVERLAPS : expectedRelationship(name, otherName);
                // System.out.println(name + " relation to " + otherName + ":\t" + polygon.getRelationship(other));
                assertThat(name + " intersects " + otherName, polygon.intersects(other), equalTo(samePolygon));
                assertThat(name + " relates to " + otherName, polygon.getRelationship(other), equalTo(relates));
            }
        }
        testGeometryCollector.stop();
    }

    private int expectedRelationship(String name, String otherName) {
        if (name.equals(otherName)) {
            return GeoArea.OVERLAPS;
        }
        if (name.contains("Inn")) {
            return GeoArea.CONTAINS;
        }
        if (name.contains("Out")) {
            return GeoArea.WITHIN;
        }
        if (otherName.contains("Inn")) {
            return GeoArea.WITHIN;
        }
        if (otherName.contains("Out")) {
            return GeoArea.CONTAINS;
        }
        throw new IllegalStateException("Must match one of the above cases, but we got '" + name + "' and '" + otherName + "'");
    }

    private void simpleShapeShouldBeValid(GeoPoint... points) {
        GeoPolygon triangle = GeoRegularConvexPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
        // test 3D axes (all samples above surround the x-axis)
        assertTrue(triangle.isWithin(new Vector(1, 0, 0)));
        assertFalse(triangle.isWithin(new Vector(0, 1, 0)));
        assertFalse(triangle.isWithin(new Vector(0, 0, 1)));
        assertFalse(triangle.isWithin(new Vector(-1, 0, 0)));
        assertFalse(triangle.isWithin(new Vector(0, -1, 0)));
        assertFalse(triangle.isWithin(new Vector(0, 0, -1)));
        LatLonBounds expected = new LatLonBounds();
        for (GeoPoint point : points) {
            // This does not take into account great circles for horizontal lines (see thresholds below)
            expected.addPoint(point);
        }
        LatLonBounds bounds = new LatLonBounds();
        triangle.getBounds(bounds);
        double latThreshold = 1e-10, lonThreshold = 1e-10;
        if (containsHorizontalLine(points)) {
            // Horizontal lines are not great circles, so the real bounds will differ due to the great circles
            latThreshold = 1e-3;
        }
        assertThat("Expected bounds max latitude", bounds.getMaxLatitude(), closeTo(expected.getMaxLatitude(), latThreshold));
        assertThat("Expected bounds min latitude", bounds.getMinLatitude(), closeTo(expected.getMinLatitude(), latThreshold));
        assertThat("Expected bounds left longitude", bounds.getLeftLongitude(), closeTo(expected.getLeftLongitude(), lonThreshold));
        assertThat("Expected bounds right longitude", bounds.getRightLongitude(), closeTo(expected.getRightLongitude(), lonThreshold));
    }
}
