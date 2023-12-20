/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.geometry.utils;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CircleUtilsTests extends ESTestCase {

    public void testCreateRegularGeoShapePolygon() {
        final Circle circle = randomValueOtherThanMany(
            c -> SloppyMath.haversinMeters(c.getLat(), c.getLon(), 90, 0) < c.getRadiusMeters()
                || SloppyMath.haversinMeters(c.getLat(), c.getLon(), -90, 0) < c.getRadiusMeters(),
            () -> GeometryTestUtils.randomCircle(true)
        );
        doRegularGeoShapePolygon(circle);
    }

    public void testCircleContainsNorthPole() {
        final Circle circle = randomValueOtherThanMany(
            c -> SloppyMath.haversinMeters(c.getLat(), c.getLon(), 90, 0) >= c.getRadiusMeters(),
            () -> GeometryTestUtils.randomCircle(true)
        );
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> doRegularGeoShapePolygon(circle));
        assertThat(ex.getMessage(), containsString("contains the north pole"));
    }

    public void testCircleContainsSouthPole() {
        final Circle circle = randomValueOtherThanMany(
            c -> SloppyMath.haversinMeters(c.getLat(), c.getLon(), -90, 0) >= c.getRadiusMeters(),
            () -> GeometryTestUtils.randomCircle(true)
        );
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> doRegularGeoShapePolygon(circle));
        assertThat(ex.getMessage(), containsString("contains the south pole"));
    }

    private void doRegularGeoShapePolygon(Circle circle) {
        int numSides = randomIntBetween(4, 1000);
        Polygon polygon = CircleUtils.createRegularGeoShapePolygon(circle, numSides);
        LinearRing outerShell = polygon.getPolygon();
        int numPoints = outerShell.length();

        // check no holes created
        assertThat(polygon.getNumberOfHoles(), equalTo(0));
        // check there are numSides edges
        assertThat(numPoints, equalTo(numSides + 1));
        // check that all the points are about a radius away from the center
        for (int i = 0; i < numPoints; i++) {
            double actualDistance = SloppyMath.haversinMeters(circle.getY(), circle.getX(), outerShell.getY(i), outerShell.getX(i));
            assertThat(actualDistance, closeTo(circle.getRadiusMeters(), 0.1));
        }
    }

    public void testCreateRegularShapePolygon() {
        double x = randomDoubleBetween(-20, 20, true);
        double y = randomDoubleBetween(-20, 20, true);
        double radius = randomDoubleBetween(10, 10000, true);
        Circle circle = new Circle(x, y, radius);
        int numSides = randomIntBetween(4, 1000);
        Polygon polygon = CircleUtils.createRegularShapePolygon(circle, numSides);
        LinearRing outerShell = polygon.getPolygon();
        int numPoints = outerShell.length();

        // check no holes created
        assertThat(polygon.getNumberOfHoles(), equalTo(0));
        // check there are numSides edges
        assertThat(numPoints, equalTo(numSides + 1));
        // check that all the points are about a radius away from the center
        for (int i = 0; i < numPoints; i++) {
            double deltaX = circle.getX() - outerShell.getX(i);
            double deltaY = circle.getY() - outerShell.getY(i);
            double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
            assertThat(distance, closeTo(radius, 0.0001));
        }
    }

    public void testCircleToPolygonNumSides() {
        // 0.01, 6371000
        double randomErrorDistanceMeters = randomDoubleBetween(0.01, 6371000, true);
        double[] errorDistancesMeters = new double[] {
            0.01,
            0.02,
            0.03,
            0.1,
            0.2,
            1,
            1.1,
            1.2,
            2,
            2.5,
            3,
            3.7,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            5000000.12345,
            6370999.999,
            6371000,
            randomErrorDistanceMeters };

        for (double errorDistanceMeters : errorDistancesMeters) {
            // radius is same as error distance
            assertThat(CircleUtils.circleToPolygonNumSides(errorDistanceMeters, errorDistanceMeters), equalTo(4));
            // radius is much smaller than error distance
            assertThat(CircleUtils.circleToPolygonNumSides(0, errorDistanceMeters), equalTo(4));
            // radius is much larger than error distance
            double errorDistanceForPow = errorDistanceMeters;
            if (errorDistanceForPow < 1.12d) {
                errorDistanceForPow = 1.12;
            }
            assertThat(CircleUtils.circleToPolygonNumSides(Math.pow(errorDistanceForPow, 100), errorDistanceForPow), equalTo(1000));
            // radius is 5 times longer than error distance
            assertThat(CircleUtils.circleToPolygonNumSides(5 * errorDistanceMeters, errorDistanceMeters), equalTo(10));
        }
    }

}
