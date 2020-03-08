/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SpatialUtilsTests extends ESTestCase {

    public void testCreateRegularGeoShapePolygon() {
        double lon = randomDoubleBetween(-20, 20, true);
        double lat = randomDoubleBetween(-20, 20, true);
        double radiusMeters = randomDoubleBetween(10, 10000, true);
        Circle circle = new Circle(lon, lat, radiusMeters);
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
            assertThat(actualDistance, closeTo(radiusMeters, 0.1));
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
