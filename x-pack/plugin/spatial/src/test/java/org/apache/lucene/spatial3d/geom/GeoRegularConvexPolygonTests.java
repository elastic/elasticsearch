/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.lucene.spatial3d.geom;

import org.junit.Test;

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GeoRegularConvexPolygonTests {

    @Test
    public void testSimpleTriangle() {
        GeoPoint left = new GeoPoint(PlanetModel.SPHERE, 0, -0.1);
        GeoPoint right = new GeoPoint(PlanetModel.SPHERE, 0, 0.1);
        GeoPoint top = new GeoPoint(PlanetModel.SPHERE, 0.2, 0);
        simpleShapeShouldBeValid(left, right, top);
    }

    @Test
    public void testSimpleSquare() {
        GeoPoint left = new GeoPoint(PlanetModel.SPHERE, 0, -0.1);
        GeoPoint right = new GeoPoint(PlanetModel.SPHERE, 0, 0.1);
        GeoPoint top = new GeoPoint(PlanetModel.SPHERE, 0.1, 0);
        GeoPoint bottom = new GeoPoint(PlanetModel.SPHERE, -0.1, 0);
        simpleShapeShouldBeValid(left, bottom, right, top);
    }

    @Test
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

    private void simpleShapeShouldBeValid(GeoPoint... points) {
        GeoPolygon triangle = GeoRegularConvexPolygonFactory.makePolygon(PlanetModel.SPHERE, points);
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
        if (containsHorizonalLine(points)) {
            // Horizontal lines are not great circles, so the real bounds will differ due to the great circles
            latThreshold = 1e-3;
        }
        assertThat("Expected bounds max latitude", bounds.getMaxLatitude(), closeTo(expected.getMaxLatitude(), latThreshold));
        assertThat("Expected bounds min latitude", bounds.getMinLatitude(), closeTo(expected.getMinLatitude(), latThreshold));
        assertThat("Expected bounds left longitude", bounds.getLeftLongitude(), closeTo(expected.getLeftLongitude(), lonThreshold));
        assertThat("Expected bounds right longitude", bounds.getRightLongitude(), closeTo(expected.getRightLongitude(), lonThreshold));
    }

    private boolean containsHorizonalLine(GeoPoint[] points) {
        boolean ans = false;
        for (int i = 0; i < points.length; i++) {
            int prev = (i == 0) ? points.length - 1 : i - 1;
            double latDiff = Math.abs(points[i].latitude - points[prev].latitude);
            double lonDiff = Math.abs(points[i].longitude - points[prev].longitude);
            ans |= latDiff < 0.001 && lonDiff > 0.001;
        }
        return ans;
    }
}
