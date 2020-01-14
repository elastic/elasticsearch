/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class CentroidCalculatorTests extends ESTestCase {
    private static final double DELTA = 0.000001;

    public void testLine() {
        double[] y = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] x = new double[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
        double[] yRunningAvg = new double[] { 1, 1.5, 2.0, 2.5, 3, 3.5, 4, 4.5, 5, 5.5 };
        double[] xRunningAvg = new double[] { 10, 15, 20, 25, 30, 35, 40, 45, 50, 55 };

        Point point = new Point(x[0], y[0]);
        CentroidCalculator calculator = new CentroidCalculator(point);
        assertThat(calculator.getX(), equalTo(xRunningAvg[0]));
        assertThat(calculator.getY(), equalTo(yRunningAvg[0]));
        for (int i = 1; i < 10; i++) {
            double[] subX = new double[i + 1];
            double[] subY = new double[i + 1];
            System.arraycopy(x, 0, subX, 0, i + 1);
            System.arraycopy(y, 0, subY, 0, i + 1);
            Geometry geometry  = new Line(subX, subY);
            calculator = new CentroidCalculator(geometry);
            assertEquals(xRunningAvg[i], calculator.getX(), DELTA);
            assertEquals(yRunningAvg[i], calculator.getY(), DELTA);
        }
        CentroidCalculator otherCalculator = new CentroidCalculator(new Point(0, 0));
        calculator.addFrom(otherCalculator);
        assertEquals(55.0, calculator.getX(), DELTA);
        assertEquals(5.5, calculator.getY(), DELTA);
    }

    public void testRoundingErrorAndNormalization() {
        double lonA = GeometryTestUtils.randomLon();
        double latA = GeometryTestUtils.randomLat();
        double lonB = randomValueOtherThanMany((l) -> Math.abs(l - lonA) <= GeoUtils.TOLERANCE, GeometryTestUtils::randomLon);
        double latB = randomValueOtherThanMany((l) -> Math.abs(l - latA) <= GeoUtils.TOLERANCE, GeometryTestUtils::randomLat);
        {
            Line line = new Line(new double[]{180.0, 180.0}, new double[]{latA, latB});
            assertThat(new CentroidCalculator(line).getX(), anyOf(equalTo(179.99999999999997),
                equalTo(180.0), equalTo(-179.99999999999997)));
        }

        {
            Line line = new Line(new double[]{-180.0, -180.0}, new double[]{latA, latB});
            assertThat(new CentroidCalculator(line).getX(), anyOf(equalTo(179.99999999999997),
                equalTo(180.0), equalTo(-179.99999999999997)));
        }

        {
            Line line = new Line(new double[]{lonA, lonB}, new double[] { 90.0, 90.0 });
            assertThat(new CentroidCalculator(line).getY(), anyOf(equalTo(90.0), equalTo(89.99999999999999)));
        }

        {
            Line line = new Line(new double[]{lonA, lonB}, new double[] { -90.0, -90.0 });
            assertThat(new CentroidCalculator(line).getY(), anyOf(equalTo(-90.0), equalTo(-89.99999999999999)));
        }
    }

    // test that the centroid calculation is agnostic to orientation
    public void testPolyonWithHole() {
        for (boolean ccwOuter : List.of(true, false)) {
            for (boolean ccwInner : List.of(true, false)) {
                final LinearRing outer, inner;
                if (ccwOuter) {
                    outer = new LinearRing(new double[]{-50, 50, 50, -50, -50}, new double[]{-50, -50, 50, 50, -50});
                } else {
                    outer = new LinearRing(new double[]{-50, -50, 50, 50, -50}, new double[]{-50, 50, 50, -50, -50});
                }
                if (ccwInner) {
                    inner = new LinearRing(new double[]{-40, 30, 30, -40, -40}, new double[]{-40, -40, 30, 30, -40});
                } else {
                    inner = new LinearRing(new double[]{-40, -40, 30, 30, -40}, new double[]{-40, 30, 30, -40, -40});
                }
                final double POLY_CENTROID = 4.803921568627451;
                CentroidCalculator calculator = new CentroidCalculator(new Polygon(outer, Collections.singletonList(inner)));
                assertEquals(POLY_CENTROID, calculator.getX(), DELTA);
                assertEquals(POLY_CENTROID, calculator.getY(), DELTA);
                assertThat(calculator.sumWeight(), equalTo(5100.0));
            }
        }
    }

    public void testPolygonWithEqualSizedHole() {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[]{-50, 50, 50, -50, -50}, new double[]{-50, -50, 50, 50, -50}),
            Collections.singletonList(new LinearRing(new double[]{-50, -50, 50, 50, -50}, new double[]{-50, 50, 50, -50, -50})));
        CentroidCalculator calculator = new CentroidCalculator(polyWithHole);
        assertThat(calculator.getX(), equalTo(Double.NaN));
        assertThat(calculator.getY(), equalTo(Double.NaN));
        assertThat(calculator.sumWeight(), equalTo(0.0));
    }

    public void testLineAsClosedPoint() {
        double lon = GeometryTestUtils.randomLon();
        double lat = GeometryTestUtils.randomLat();
        CentroidCalculator calculator = new CentroidCalculator(new Line(new double[] {lon, lon}, new double[] { lat, lat}));
        assertThat(calculator.getX(), equalTo(Double.NaN));
        assertThat(calculator.getY(), equalTo(Double.NaN));
        assertThat(calculator.sumWeight(), equalTo(0.0));
    }
}
