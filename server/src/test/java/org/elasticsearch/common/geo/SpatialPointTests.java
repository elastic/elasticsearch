/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class SpatialPointTests extends ESTestCase {

    public void testEqualsAndHashcode() {
        for (int i = 0; i < 100; i++) {
            SpatialPoint point = randomGeoPoint();
            GeoPoint geoPoint = new GeoPoint(point);
            TestPoint testPoint = new TestPoint(point);
            TestPoint testPoint2 = new TestPoint(point);
            assertEqualsAndHashcode("Same point", point, point);
            assertEqualsAndHashcode("Same geo-point", point, geoPoint);
            assertNotEqualsAndHashcode("Same location, but different class", point, testPoint);
            assertEqualsAndHashcode("Same location, same class", testPoint, testPoint2);
        }
    }

    public void testCompareTo() {
        for (int i = 0; i < 100; i++) {
            SpatialPoint point = randomValueOtherThanMany(p -> p.getX() < -170 || p.getX() > 170, SpatialPointTests::randomGeoPoint);
            GeoPoint smaller = new GeoPoint(point.getY(), point.getX() - 1);
            GeoPoint bigger = new GeoPoint(point.getY(), point.getX() + 1);
            TestPoint testSmaller = new TestPoint(smaller);
            TestPoint testBigger = new TestPoint(bigger);
            assertThat(smaller + " smaller than " + point, smaller.compareTo(point), lessThan(0));
            assertThat(bigger + " bigger than " + point, bigger.compareTo(point), greaterThan(0));
            assertThat(testSmaller + " smaller than " + testBigger, testSmaller.compareTo(testBigger), lessThan(0));
            // TestPoint always greater than GeoPoint
            assertThat(testSmaller + " bigger than " + point, testSmaller.compareTo(point), greaterThan(0));
            assertThat(testBigger + " bigger than " + point, testBigger.compareTo(point), greaterThan(0));
        }
    }

    private void assertEqualsAndHashcode(String message, SpatialPoint a, SpatialPoint b) {
        assertThat("Equals: " + message, a, equalTo(b));
        assertThat("Hashcode: " + message, a.hashCode(), equalTo(b.hashCode()));
        assertThat("Compare: " + message, a.compareTo(b), equalTo(0));
    }

    private void assertNotEqualsAndHashcode(String message, SpatialPoint a, SpatialPoint b) {
        assertThat("Equals: " + message, a, not(equalTo(b)));
        assertThat("Hashcode: " + message, a.hashCode(), not(equalTo(b.hashCode())));
        assertThat("Compare: " + message, a.compareTo(b), not(equalTo(0)));
    }

    private static GeoPoint randomGeoPoint() {
        return new GeoPoint(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
    }

    /**
     * This test class used to be trivial, when SpatialPoint was a concrete class.
     * If we ever revert back to a concrete class, we can simplify this test class.
     * The only requirement is that it extends SpatialPoint, but have a different class name.
     */
    private static class TestPoint implements SpatialPoint {
        double x;
        double y;

        private TestPoint(SpatialPoint template) {
            this.x = template.getX();
            this.y = template.getY();
        }

        @Override
        public double getX() {
            return x;
        }

        @Override
        public double getY() {
            return y;
        }

        @Override
        public int hashCode() {
            return 31 * 31 * getClass().getSimpleName().hashCode() + 31 * Double.hashCode(x) + Double.hashCode(y);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            SpatialPoint point = (SpatialPoint) obj;
            return (Double.compare(point.getX(), x) == 0) && Double.compare(point.getY(), y) == 0;
        }

        @Override
        public String toString() {
            return toWKT();
        }
    }
}
