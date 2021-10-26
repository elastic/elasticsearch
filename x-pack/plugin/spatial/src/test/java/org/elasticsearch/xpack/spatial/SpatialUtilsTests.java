/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SpatialUtilsTests extends ESTestCase {

    public void testCreateRegularGeoShapePolygon() {
        final Circle circle = randomValueOtherThanMany(
            c -> SloppyMath.haversinMeters(c.getLat(), c.getLon(), 90, 0) < c.getRadiusMeters()
                || SloppyMath.haversinMeters(c.getLat(), c.getLon(), -90, 0) < c.getRadiusMeters(),
            () -> GeometryTestUtils.randomCircle(true));
        doRegularGeoShapePolygon(circle);
    }

    public void testCircleContainsNorthPole() {
        final Circle circle = randomValueOtherThanMany(
            c -> SloppyMath.haversinMeters(c.getLat(), c.getLon(), 90, 0) >= c.getRadiusMeters(),
            () -> GeometryTestUtils.randomCircle(true));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> doRegularGeoShapePolygon(circle));
        assertThat(ex.getMessage(), containsString("contains the north pole"));
    }

    public void testCircleContainsSouthPole() {
        final Circle circle = randomValueOtherThanMany(
            c -> SloppyMath.haversinMeters(c.getLat(), c.getLon(), -90, 0) >= c.getRadiusMeters(),
            () -> GeometryTestUtils.randomCircle(true));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> doRegularGeoShapePolygon(circle));
        assertThat(ex.getMessage(), containsString("contains the south pole"));
    }

    private void doRegularGeoShapePolygon(Circle circle) {
        int numSides = randomIntBetween(4, 1000);
        Polygon polygon = SpatialUtils.createRegularGeoShapePolygon(circle, numSides);
        LinearRing outerShell = polygon.getPolygon();
        int numPoints = outerShell.length();

        // check no holes created
        assertThat(polygon.getNumberOfHoles(), equalTo(0));
        // check there are numSides edges
        assertThat(numPoints, equalTo(numSides + 1));
        // check that all the points are about a radius away from the center
        for (int i = 0; i < numPoints ; i++) {
            double actualDistance = SloppyMath
                .haversinMeters(circle.getY(), circle.getX(), outerShell.getY(i), outerShell.getX(i));
            assertThat(actualDistance, closeTo(circle.getRadiusMeters(), 0.1));
        }
    }

    public void testCreateRegularShapePolygon() {
        double x = randomDoubleBetween(-20, 20, true);
        double y = randomDoubleBetween(-20, 20, true);
        double radius = randomDoubleBetween(10, 10000, true);
        Circle circle = new Circle(x, y, radius);
        int numSides = randomIntBetween(4, 1000);
        Polygon polygon = SpatialUtils.createRegularShapePolygon(circle, numSides);
        LinearRing outerShell = polygon.getPolygon();
        int numPoints = outerShell.length();

        // check no holes created
        assertThat(polygon.getNumberOfHoles(), equalTo(0));
        // check there are numSides edges
        assertThat(numPoints, equalTo(numSides + 1));
        // check that all the points are about a radius away from the center
        for (int i = 0; i < numPoints ; i++) {
            double deltaX = circle.getX() - outerShell.getX(i);
            double deltaY = circle.getY() - outerShell.getY(i);
            double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
            assertThat(distance, closeTo(radius, 0.0001));
        }
    }
}
