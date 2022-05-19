/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.GeoUtils;
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
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType.LINE;
import static org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType.POINT;
import static org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType.POLYGON;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CentroidCalculatorTests extends ESTestCase {
    private static final double DELTA = 0.000000001;

    public void testPoint() {
        Point point = GeometryTestUtils.randomPoint(false);
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(point);
        assertThat(calculator.getX(), equalTo(point.getX()));
        assertThat(calculator.getY(), equalTo(point.getY()));
        assertThat(calculator.sumWeight(), equalTo(1.0));
        assertThat(calculator.getDimensionalShapeType(), equalTo(POINT));
    }

    public void testPolygonWithSmallTrianglesOfZeroWeight() throws Exception {
        Geometry geometry = WellKnownText.fromWKT(
            GeographyValidator.instance(true),
            false,
            "POLYGON((-4.385064 55.2259599,-4.385056 55.2259224,-4.3850466 55.2258994,-4.3849755 55.2258574,"
                + "-4.3849339 55.2258589,-4.3847033 55.2258742,-4.3846805 55.2258818,-4.3846282 55.2259132,-4.3846215 55.2259247,"
                + "-4.3846121 55.2259683,-4.3846147 55.2259798,-4.3846369 55.2260157,-4.3846472 55.2260241,"
                + "-4.3846697 55.2260409,-4.3846952 55.2260562,-4.384765 55.22608,-4.3848199 55.2260861,-4.3848481 55.2260845,"
                + "-4.3849245 55.2260761,-4.3849393 55.22607,-4.3849996 55.2260432,-4.3850131 55.2260364,-4.3850426 55.2259989,"
                + "-4.385064 55.2259599),(-4.3850104 55.2259583,-4.385005 55.2259752,-4.384997 55.2259892,-4.3849339 55.2259981,"
                + "-4.3849272 55.2259308,-4.3850016 55.2259262,-4.385005 55.2259377,-4.3850104 55.2259583),"
                + "(-4.3849996 55.2259193,-4.3847502 55.2259331,-4.3847548 55.2258921,-4.3848012 55.2258895,"
                + "-4.3849219 55.2258811,-4.3849514 55.2258818,-4.3849728 55.2258933,-4.3849996 55.2259193),"
                + "(-4.3849917 55.2259984,-4.3849849 55.2260103,-4.3849771 55.2260192,-4.3849701 55.2260019,-4.3849917 55.2259984),"
                + "(-4.3846608 55.2259374,-4.384663 55.2259316,-4.3846711 55.2259201,-4.3846992 55.225904,"
                + "-4.384718 55.2258941,-4.3847434 55.2258927,-4.3847314 55.2259407,-4.3849098 55.2259316,-4.3849098 55.2259492,"
                + "-4.3848843 55.2259515,-4.3849017 55.2260119,-4.3849567 55.226005,-4.3849701 55.2260272,-4.3849299 55.2260486,"
                + "-4.3849192 55.2260295,-4.384883 55.2260188,-4.3848776 55.2260119,-4.3848441 55.2260149,-4.3848441 55.2260226,"
                + "-4.3847864 55.2260241,-4.384722 55.2259652,-4.3847053 55.2259706,-4.384683 55.225954,-4.3846608 55.2259374),"
                + "(-4.3846541 55.2259549,-4.384698 55.2259883,-4.3847173 55.2259828,-4.3847743 55.2260333,-4.3847891 55.2260356,"
                + "-4.3848146 55.226031,-4.3848199 55.2260409,-4.3848387 55.2260417,-4.3848494 55.2260593,-4.3848092 55.2260616,"
                + "-4.3847623 55.2260539,-4.3847341 55.2260432,-4.3847046 55.2260279,-4.3846738 55.2260062,-4.3846496 55.2259844,"
                + "-4.3846429 55.2259737,-4.3846523 55.2259714,-4.384651 55.2259629,-4.3846541 55.2259549),"
                + "(-4.3846608 55.2259374,-4.3846559 55.2259502,-4.3846541 55.2259549,-4.3846608 55.2259374))"
        );
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(geometry);
        assertThat(calculator.getX(), closeTo(-4.3848, 1e-4));
        assertThat(calculator.getY(), closeTo(55.22595, 1e-4));
        assertThat(calculator.sumWeight(), closeTo(0, 1e-5));
        assertThat(calculator.getDimensionalShapeType(), equalTo(POLYGON));
    }

    public void testLine() {
        double[] y = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] x = new double[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
        double[] yRunningAvg = new double[] { 1, 1.5, 2.0, 2.5, 3, 3.5, 4, 4.5, 5, 5.5 };
        double[] xRunningAvg = new double[] { 10, 15, 20, 25, 30, 35, 40, 45, 50, 55 };

        Point point = new Point(x[0], y[0]);
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(point);
        assertThat(calculator.getX(), equalTo(xRunningAvg[0]));
        assertThat(calculator.getY(), equalTo(yRunningAvg[0]));
        for (int i = 1; i < 10; i++) {
            double[] subX = new double[i + 1];
            double[] subY = new double[i + 1];
            System.arraycopy(x, 0, subX, 0, i + 1);
            System.arraycopy(y, 0, subY, 0, i + 1);
            Geometry geometry = new Line(subX, subY);
            calculator = new CentroidCalculator();
            calculator.add(geometry);
            assertEquals(xRunningAvg[i], calculator.getX(), DELTA);
            assertEquals(yRunningAvg[i], calculator.getY(), DELTA);
        }
        calculator.add(new Point(0, 0));
        assertEquals(55.0, calculator.getX(), DELTA);
        assertEquals(5.5, calculator.getY(), DELTA);
    }

    public void testMultiLine() {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(false);
        CentroidCalculator lineCalculator = new CentroidCalculator();
        for (Line line : multiLine) {
            lineCalculator.add(line);
        }
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(multiLine);

        assertEquals(lineCalculator.getX(), calculator.getX(), DELTA);
        assertEquals(lineCalculator.getY(), calculator.getY(), DELTA);
        assertEquals(lineCalculator.sumWeight(), calculator.sumWeight(), DELTA);
        assertThat(lineCalculator.getDimensionalShapeType(), equalTo(calculator.getDimensionalShapeType()));
        assertThat(calculator.getDimensionalShapeType(), equalTo(LINE));
    }

    public void testMultiPoint() {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        CentroidCalculator pointCalculator = new CentroidCalculator();
        for (Point point : multiPoint) {
            pointCalculator.add(point);
        }
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(multiPoint);
        assertEquals(pointCalculator.getX(), calculator.getX(), DELTA);
        assertEquals(pointCalculator.getY(), calculator.getY(), DELTA);
        assertEquals(pointCalculator.sumWeight(), calculator.sumWeight(), DELTA);
        assertThat(calculator.getDimensionalShapeType(), equalTo(POINT));

    }

    public void testRoundingErrorAndNormalization() throws IOException {
        double lonA = GeometryTestUtils.randomLon();
        double latA = GeometryTestUtils.randomLat();
        double lonB = randomValueOtherThanMany((l) -> Math.abs(l - lonA) <= GeoUtils.TOLERANCE, GeometryTestUtils::randomLon);
        double latB = randomValueOtherThanMany((l) -> Math.abs(l - latA) <= GeoUtils.TOLERANCE, GeometryTestUtils::randomLat);
        {
            Line line = new Line(new double[] { 180.0, 180.0 }, new double[] { latA, latB });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.lon(), anyOf(equalTo(179.99999991618097), equalTo(-180.0)));
        }

        {
            Line line = new Line(new double[] { -180.0, -180.0 }, new double[] { latA, latB });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.lon(), anyOf(equalTo(179.99999991618097), equalTo(-180.0)));
        }

        {
            Line line = new Line(new double[] { lonA, lonB }, new double[] { 90.0, 90.0 });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.lat(), equalTo(89.99999995809048));
        }

        {
            Line line = new Line(new double[] { lonA, lonB }, new double[] { -90.0, -90.0 });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.lat(), equalTo(-90.0));
        }
    }

    // test that the centroid calculation is agnostic to orientation
    public void testPolyonWithHole() {
        for (boolean ccwOuter : List.of(true, false)) {
            for (boolean ccwInner : List.of(true, false)) {
                final LinearRing outer, inner;
                if (ccwOuter) {
                    outer = new LinearRing(new double[] { -50, 50, 50, -50, -50 }, new double[] { -50, -50, 50, 50, -50 });
                } else {
                    outer = new LinearRing(new double[] { -50, -50, 50, 50, -50 }, new double[] { -50, 50, 50, -50, -50 });
                }
                if (ccwInner) {
                    inner = new LinearRing(new double[] { -40, 30, 30, -40, -40 }, new double[] { -40, -40, 30, 30, -40 });
                } else {
                    inner = new LinearRing(new double[] { -40, -40, 30, 30, -40 }, new double[] { -40, 30, 30, -40, -40 });
                }
                final double POLY_CENTROID = 4.803921568627451;
                CentroidCalculator calculator = new CentroidCalculator();
                calculator.add(new Polygon(outer, Collections.singletonList(inner)));
                assertEquals(POLY_CENTROID, calculator.getX(), DELTA);
                assertEquals(POLY_CENTROID, calculator.getY(), DELTA);
                assertThat(calculator.sumWeight(), equalTo(5100.0));
            }
        }
    }

    public void testRectangle() {
        for (int i = 0; i < 100; i++) {
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(GeometryTestUtils.randomRectangle());
            assertThat(calculator.sumWeight(), greaterThan(0.0));
        }
    }

    public void testLineAsClosedPoint() {
        double lon = GeometryTestUtils.randomLon();
        double lat = GeometryTestUtils.randomLat();
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(new Line(new double[] { lon, lon }, new double[] { lat, lat }));
        assertThat(calculator.getX(), equalTo(lon));
        assertThat(calculator.getY(), equalTo(lat));
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
        CentroidCalculator lineCalculator = new CentroidCalculator();
        lineCalculator.add(line);

        Polygon polygon = new Polygon(new LinearRing(x, y));
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(polygon);

        // sometimes precision issues yield non-zero areas. must verify that area is close to zero
        if (calculator.getDimensionalShapeType() == POLYGON) {
            assertEquals(0.0, calculator.sumWeight(), 1e-10);
        } else {
            assertThat(calculator.getDimensionalShapeType(), equalTo(LINE));
            assertThat(calculator.getX(), equalTo(lineCalculator.getX()));
            assertThat(calculator.getY(), equalTo(lineCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(lineCalculator.sumWeight()));
        }
    }

    public void testPolygonWithEqualSizedHole() {
        Polygon polyWithHole = new Polygon(
            new LinearRing(new double[] { -50, 50, 50, -50, -50 }, new double[] { -50, -50, 50, 50, -50 }),
            Collections.singletonList(new LinearRing(new double[] { -50, -50, 50, 50, -50 }, new double[] { -50, 50, 50, -50, -50 }))
        );
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(polyWithHole);
        assertThat(calculator.getX(), equalTo(0.0));
        assertThat(calculator.getY(), equalTo(0.0));
        assertThat(calculator.sumWeight(), equalTo(400.0));
        assertThat(calculator.getDimensionalShapeType(), equalTo(LINE));
    }

    public void testPolygonAsPoint() {
        Point point = GeometryTestUtils.randomPoint(false);
        Polygon polygon = new Polygon(
            new LinearRing(
                new double[] { point.getX(), point.getX(), point.getX(), point.getX() },
                new double[] { point.getY(), point.getY(), point.getY(), point.getY() }
            )
        );
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(polygon);
        // make calculation to account for floating-point arithmetic
        assertThat(calculator.getX(), equalTo((3 * point.getX()) / 3));
        assertThat(calculator.getY(), equalTo((3 * point.getY()) / 3));
        assertThat(calculator.sumWeight(), equalTo(3.0));
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
        CentroidCalculator addFromCalculator = new CentroidCalculator();
        for (Geometry shape : shapes) {
            if ((shape.type() == ShapeType.MULTIPOLYGON || shape.type() == ShapeType.POLYGON)
                || (dimensionalShapeType == LINE && (shape.type() == ShapeType.LINESTRING || shape.type() == ShapeType.MULTILINESTRING))
                || (dimensionalShapeType == POINT && (shape.type() == ShapeType.POINT || shape.type() == ShapeType.MULTIPOINT))) {
                addFromCalculator.add(shape);
            }
        }

        // shuffle
        if (randomBoolean()) {
            Collections.shuffle(shapes, random());
        } else if (randomBoolean()) {
            Collections.reverse(shapes);
        }

        GeometryCollection<Geometry> collection = new GeometryCollection<>(shapes);
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.add(collection);

        assertNotNull(addFromCalculator.getDimensionalShapeType());
        assertThat(addFromCalculator.getDimensionalShapeType(), equalTo(dimensionalShapeType));
        assertThat(calculator.getDimensionalShapeType(), equalTo(dimensionalShapeType));
        assertEquals(calculator.getX(), addFromCalculator.getX(), DELTA);
        assertEquals(calculator.getY(), addFromCalculator.getY(), DELTA);
        assertEquals(calculator.sumWeight(), addFromCalculator.sumWeight(), DELTA);
    }

    public void testAddDifferentDimensionalType() {
        Point point = GeometryTestUtils.randomPoint(false);
        Line line = GeometryTestUtils.randomLine(false);
        Polygon polygon = GeometryTestUtils.randomPolygon(false);

        // point add point
        {
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(point);
            calculator.add(point);
            assertThat(calculator.getX(), equalTo(point.getX()));
            assertThat(calculator.getY(), equalTo(point.getY()));
            assertThat(calculator.sumWeight(), equalTo(2.0));
        }

        // point add line/polygon
        {
            CentroidCalculator lineCalculator = new CentroidCalculator();
            lineCalculator.add(line);
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(point);
            calculator.add(line);
            if (lineCalculator.getDimensionalShapeType() == LINE) {  // skip degenerated line
                assertThat(calculator.getX(), equalTo(lineCalculator.getX()));
                assertThat(calculator.getY(), equalTo(lineCalculator.getY()));
                assertThat(calculator.sumWeight(), equalTo(lineCalculator.sumWeight()));
            }
        }

        // line add point
        {
            CentroidCalculator lineCalculator = new CentroidCalculator();
            lineCalculator.add(line);
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(line);
            calculator.add(point);
            if (lineCalculator.getDimensionalShapeType() == LINE) { // skip degenerated line
                assertThat(calculator.getX(), equalTo(lineCalculator.getX()));
                assertThat(calculator.getY(), equalTo(lineCalculator.getY()));
                assertThat(calculator.sumWeight(), equalTo(lineCalculator.sumWeight()));
            }
        }

        // line add line
        {
            CentroidCalculator lineCalculator = new CentroidCalculator();
            lineCalculator.add(line);
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(line);
            calculator.add(line);
            assertEquals(lineCalculator.getX(), calculator.getX(), DELTA);
            assertEquals(lineCalculator.getY(), calculator.getY(), DELTA);
            assertEquals(2 * lineCalculator.sumWeight(), calculator.sumWeight(), DELTA);
        }

        // line add polygon
        {
            CentroidCalculator polygonCalculator = new CentroidCalculator();
            polygonCalculator.add(polygon);
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(line);
            calculator.add(polygon);
            assertThat(calculator.getX(), equalTo(polygonCalculator.getX()));
            assertThat(calculator.getY(), equalTo(polygonCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(calculator.sumWeight()));
        }

        // polygon add point/line
        {
            CentroidCalculator polygonCalculator = new CentroidCalculator();
            polygonCalculator.add(polygon);
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(polygon);
            calculator.add(randomBoolean() ? point : line);
            assertThat(calculator.getX(), equalTo(polygonCalculator.getX()));
            assertThat(calculator.getY(), equalTo(polygonCalculator.getY()));
            assertThat(calculator.sumWeight(), equalTo(calculator.sumWeight()));
        }

        // polygon add polygon
        {
            CentroidCalculator polygonCalculator = new CentroidCalculator();
            polygonCalculator.add(polygon);
            CentroidCalculator calculator = new CentroidCalculator();
            calculator.add(polygon);
            calculator.add(polygon);
            assertEquals(polygonCalculator.getX(), calculator.getX(), DELTA);
            assertEquals(polygonCalculator.getY(), calculator.getY(), DELTA);
            assertThat(calculator.sumWeight(), equalTo(2 * polygonCalculator.sumWeight()));
        }
    }
}
