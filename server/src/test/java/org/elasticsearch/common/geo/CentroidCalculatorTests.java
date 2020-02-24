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
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.geo.DimensionalShapeType.LINE;
import static org.elasticsearch.common.geo.DimensionalShapeType.POINT;
import static org.elasticsearch.common.geo.DimensionalShapeType.POLYGON;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class CentroidCalculatorTests extends ESTestCase {
    private static final double DELTA = 0.000000001;

    public void testPoint() {
        Point point = GeometryTestUtils.randomPoint(false);
        CentroidCalculator calculator = new CentroidCalculator(point);
        assertThat(calculator.getX(), equalTo(GeoUtils.normalizeLon(point.getX())));
        assertThat(calculator.getY(), equalTo(GeoUtils.normalizeLat(point.getY())));
        assertThat(calculator.sumWeight(), equalTo(1.0));
        assertThat(calculator.getDimensionalShapeType(), equalTo(POINT));
    }

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

    public void testMultiLine() {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(false);
        double sumLineX = 0;
        double sumLineY = 0;
        double sumLineWeight = 0;
        for (Line line : multiLine) {
            CentroidCalculator calculator = new CentroidCalculator(line);
            sumLineX += calculator.compSumX.value();
            sumLineY += calculator.compSumY.value();
            sumLineWeight += calculator.compSumWeight.value();
        }
        CentroidCalculator calculator = new CentroidCalculator(multiLine);

        assertEquals(sumLineX / sumLineWeight, calculator.getX(), DELTA);
        assertEquals(sumLineY / sumLineWeight, calculator.getY(), DELTA);
        assertEquals(sumLineWeight, calculator.sumWeight(), DELTA);
        assertThat(calculator.getDimensionalShapeType(), equalTo(LINE));
    }

    public void testMultiPoint() {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        double sumPointX = 0;
        double sumPointY = 0;
        double sumPointWeight = 0;
        for (Point point : multiPoint) {
            sumPointX += point.getX();
            sumPointY += point.getY();
            sumPointWeight += 1;
        }

        CentroidCalculator calculator = new CentroidCalculator(multiPoint);
        assertEquals(sumPointX / sumPointWeight, calculator.getX(), DELTA);
        assertEquals(sumPointY / sumPointWeight, calculator.getY(), DELTA);
        assertEquals(sumPointWeight, calculator.sumWeight(), DELTA);
        assertThat(calculator.getDimensionalShapeType(), equalTo(POINT));

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

    public void testLineAsClosedPoint() {
        double lon = GeometryTestUtils.randomLon();
        double lat = GeometryTestUtils.randomLat();
        CentroidCalculator calculator = new CentroidCalculator(new Line(new double[] {lon, lon}, new double[] { lat, lat}));
        assertThat(calculator.getX(), equalTo(GeoUtils.normalizeLon(lon)));
        assertThat(calculator.getY(), equalTo(GeoUtils.normalizeLat(lat)));
        assertThat(calculator.sumWeight(), equalTo(1.0));
    }

    public void testPolygonAsLine() {
        // create a line that traces itself as a polygon
        Line sourceLine = GeometryTestUtils.randomLine(false);
        double[] x = new double[2 * sourceLine.length() - 1];
        double[] y = new double[2 * sourceLine.length() - 1];
        int idx = 0;
        for (int i = 0; i < sourceLine.length(); i++) {
            x[idx] = sourceLine.getX(i);
            y[idx] = sourceLine.getY(i);
            idx += 1;
        }
        for (int i = sourceLine.length() - 2; i >= 0; i--) {
            x[idx] = sourceLine.getX(i);
            y[idx] = sourceLine.getY(i);
            idx += 1;
        }

        Line line = new Line(x, y);
        CentroidCalculator lineCalculator = new CentroidCalculator(line);

        Polygon polygon = new Polygon(new LinearRing(x, y));
        CentroidCalculator calculator = new CentroidCalculator(polygon);

        // sometimes precision issues yield non-zero areas. must verify that area is close to zero
        if (calculator.getDimensionalShapeType() == POLYGON) {
            assertEquals(0.0, calculator.sumWeight(), 1e-10);
        } else {
            assertThat(calculator.getDimensionalShapeType(), equalTo(LINE));
            assertThat(calculator.getX(), equalTo(lineCalculator.getX()));
            assertThat(calculator.getY(), equalTo(lineCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(lineCalculator.compSumWeight.value()));
        }
    }

    public void testPolygonWithEqualSizedHole() {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[]{-50, 50, 50, -50, -50}, new double[]{-50, -50, 50, 50, -50}),
            Collections.singletonList(new LinearRing(new double[]{-50, -50, 50, 50, -50}, new double[]{-50, 50, 50, -50, -50})));
        CentroidCalculator calculator = new CentroidCalculator(polyWithHole);
        assertThat(calculator.getX(), equalTo(0.0));
        assertThat(calculator.getY(), equalTo(0.0));
        assertThat(calculator.sumWeight(), equalTo(400.0));
        assertThat(calculator.getDimensionalShapeType(), equalTo(LINE));
    }

    public void testPolygonAsPoint() {
        Point point = GeometryTestUtils.randomPoint(false);
        Polygon polygon = new Polygon(new LinearRing(new double[] { point.getX(), point.getX(), point.getX(), point.getX() },
            new double[] { point.getY(), point.getY(), point.getY(), point.getY() }));
        CentroidCalculator calculator = new CentroidCalculator(polygon);
        assertThat(calculator.getX(), equalTo(GeoUtils.normalizeLon(point.getX())));
        assertThat(calculator.getY(), equalTo(GeoUtils.normalizeLat(point.getY())));
        assertThat(calculator.sumWeight(), equalTo(1.0));
        assertThat(calculator.getDimensionalShapeType(), equalTo(POINT));
    }

    public void testGeometryCollection() {
        int numPoints = randomIntBetween(0, 3);
        int numLines = randomIntBetween(0, 3);
        int numPolygons = randomIntBetween(0, 3);

        if (numPoints == 0 && numLines == 0 && numPolygons == 0) {
            numPoints = 1;
            numLines = 1;
            numPolygons = 1;
        }
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            if (randomBoolean()) {
                shapes.add(GeometryTestUtils.randomPoint(false));
            } else {
                shapes.add(GeometryTestUtils.randomMultiPoint(false));
            }
        }
        for (int i = 0; i < numLines; i++) {
            if (randomBoolean()) {
                shapes.add(GeometryTestUtils.randomLine(false));
            } else {
                shapes.add(GeometryTestUtils.randomMultiLine(false));
            }
        }
        for (int i = 0; i < numPolygons; i++) {
            if (randomBoolean()) {
                shapes.add(GeometryTestUtils.randomPolygon(false));
            } else {
                shapes.add(GeometryTestUtils.randomMultiPolygon(false));
            }
        }

        DimensionalShapeType dimensionalShapeType = numPolygons > 0 ? POLYGON : numLines > 0 ? LINE : POINT;

        // addFromCalculator is only adding from shapes with the highest dimensionalShapeType
        CentroidCalculator addFromCalculator = null;
        for (Geometry shape : shapes) {
            if ((shape.type() == ShapeType.MULTIPOLYGON || shape.type() == ShapeType.POLYGON) ||
                    (dimensionalShapeType == LINE && (shape.type() == ShapeType.LINESTRING || shape.type() == ShapeType.MULTILINESTRING)) ||
                    (dimensionalShapeType == POINT && (shape.type() == ShapeType.POINT || shape.type() == ShapeType.MULTIPOINT))) {
                if (addFromCalculator == null) {
                    addFromCalculator = new CentroidCalculator(shape);
                } else {
                    addFromCalculator.addFrom(new CentroidCalculator(shape));
                }
            }
        }

        // shuffle
        if (randomBoolean()) {
            Collections.shuffle(shapes, random());
        } else if (randomBoolean()) {
            Collections.reverse(shapes);
        }

        GeometryCollection collection = new GeometryCollection<>(shapes);
        CentroidCalculator calculator = new CentroidCalculator(collection);

        assertThat(addFromCalculator.getDimensionalShapeType(), equalTo(dimensionalShapeType));
        assertThat(calculator.getDimensionalShapeType(), equalTo(dimensionalShapeType));
        assertEquals(calculator.getX(), addFromCalculator.getX(), DELTA);
        assertEquals(calculator.getY(), addFromCalculator.getY(), DELTA);
        assertEquals(calculator.sumWeight(), addFromCalculator.sumWeight(), DELTA);
    }

    public void testAddFrom() {
        Point point = GeometryTestUtils.randomPoint(false);
        Line line = GeometryTestUtils.randomLine(false);
        Polygon polygon = GeometryTestUtils.randomPolygon(false);

        // point add point
        {
            CentroidCalculator calculator = new CentroidCalculator(point);
            calculator.addFrom(new CentroidCalculator(point));
            assertThat(calculator.compSumX.value(), equalTo(2 * point.getX()));
            assertThat(calculator.compSumY.value(), equalTo(2 * point.getY()));
            assertThat(calculator.sumWeight(), equalTo(2.0));
        }

        // point add line/polygon
        {
            CentroidCalculator lineCalculator = new CentroidCalculator(line);
            CentroidCalculator calculator = new CentroidCalculator(point);
            calculator.addFrom(lineCalculator);
            assertThat(calculator.getX(), equalTo(lineCalculator.getX()));
            assertThat(calculator.getY(), equalTo(lineCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(lineCalculator.sumWeight()));
        }

        // line add point
        {
            CentroidCalculator lineCalculator = new CentroidCalculator(line);
            CentroidCalculator calculator = new CentroidCalculator(line);
            calculator.addFrom(new CentroidCalculator(point));
            assertThat(calculator.getX(), equalTo(lineCalculator.getX()));
            assertThat(calculator.getY(), equalTo(lineCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(lineCalculator.sumWeight()));
        }

        // line add line
        {
            CentroidCalculator lineCalculator = new CentroidCalculator(line);
            CentroidCalculator calculator = new CentroidCalculator(line);
            calculator.addFrom(lineCalculator);
            assertEquals(2 * lineCalculator.compSumX.value(), calculator.compSumX.value(), DELTA);
            assertEquals(2 * lineCalculator.compSumY.value(), calculator.compSumY.value(), DELTA);
            assertEquals(2 * lineCalculator.sumWeight(), calculator.sumWeight(), DELTA);
        }

        // line add polygon
        {
            CentroidCalculator polygonCalculator = new CentroidCalculator(polygon);
            CentroidCalculator calculator = new CentroidCalculator(line);
            calculator.addFrom(polygonCalculator);
            assertThat(calculator.getX(), equalTo(polygonCalculator.getX()));
            assertThat(calculator.getY(), equalTo(polygonCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(calculator.sumWeight()));
        }

        // polygon add point/line
        {
            CentroidCalculator polygonCalculator = new CentroidCalculator(polygon);
            CentroidCalculator calculator = new CentroidCalculator(polygon);
            calculator.addFrom(new CentroidCalculator(randomBoolean() ? point : line));
            assertThat(calculator.getX(), equalTo(polygonCalculator.getX()));
            assertThat(calculator.getY(), equalTo(polygonCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(calculator.sumWeight()));
        }

        // polygon add polygon
        {
            CentroidCalculator polygonCalculator = new CentroidCalculator(polygon);
            CentroidCalculator calculator = new CentroidCalculator(polygon);
            calculator.addFrom(polygonCalculator);
            assertThat(calculator.compSumX.value(), equalTo(2 * polygonCalculator.compSumX.value()));
            assertThat(calculator.compSumY.value(), equalTo(2 * polygonCalculator.compSumY.value()));
            assertThat(calculator.sumWeight(), equalTo(2 * polygonCalculator.sumWeight()));
        }
    }
}
