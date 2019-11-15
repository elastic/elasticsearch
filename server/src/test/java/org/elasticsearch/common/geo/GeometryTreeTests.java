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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.geo.GeometryTestUtils.fold;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.hamcrest.Matchers.equalTo;

public class GeometryTreeTests extends ESTestCase {

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-80, 70);
            int maxX = randomIntBetween(minX + 10, 80);
            int minY = randomIntBetween(-80, 70);
            int maxY = randomIntBetween(minY + 10, 80);
            double[] x = new double[]{minX, maxX, maxX, minX, minX};
            double[] y = new double[]{minY, minY, maxY, maxY, minY};
            Geometry rectangle = randomBoolean() ?
                new Polygon(new LinearRing(x, y), Collections.emptyList()) : new Rectangle(minX, maxX, maxY, minY);
            GeometryTreeReader reader = geometryTreeReader(rectangle, TestCoordinateEncoder.INSTANCE);

            assertThat(Extent.fromPoints(minX, minY, maxX, maxY), equalTo(reader.getExtent()));
            // encoder loses precision when casting to integer, so centroid is calculated using integer division here
            assertThat(reader.getCentroidX(), equalTo((double) ((minX + maxX) / 2)));
            assertThat(reader.getCentroidY(), equalTo((double) ((minY + maxY) / 2)));

            // box-query touches bottom-left corner
            assertTrue(reader.intersects(Extent.fromPoints(minX - randomIntBetween(1, 10), minY - randomIntBetween(1, 10), minX, minY)));
            // box-query touches bottom-right corner
            assertTrue(reader.intersects(Extent.fromPoints(maxX, minY - randomIntBetween(1, 10), maxX + randomIntBetween(1, 10), minY)));
            // box-query touches top-right corner
            assertTrue(reader.intersects(Extent.fromPoints(maxX, maxY, maxX + randomIntBetween(1, 10), maxY + randomIntBetween(1, 10))));
            // box-query touches top-left corner
            assertTrue(reader.intersects(Extent.fromPoints(minX - randomIntBetween(1, 10), maxY, minX, maxY + randomIntBetween(1, 10))));
            // box-query fully-enclosed inside rectangle
            assertTrue(reader.intersects(Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4)));
            // box-query fully-contains poly
            assertTrue(reader.intersects(Extent.fromPoints(minX - randomIntBetween(1, 10), minY - randomIntBetween(1, 10),
                maxX + randomIntBetween(1, 10), maxY + randomIntBetween(1, 10))));
            // box-query half-in-half-out-right
            assertTrue(reader.intersects(Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 10),
                (3 * maxY + minY) / 4)));
            // box-query half-in-half-out-left
            assertTrue(reader.intersects(Extent.fromPoints(minX - randomIntBetween(1, 10), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4)));
            // box-query half-in-half-out-top
            assertTrue(reader.intersects(Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 10),
                maxY + randomIntBetween(1, 10))));
            // box-query half-in-half-out-bottom
            assertTrue(reader.intersects(Extent.fromPoints((3 * minX + maxX) / 4, minY - randomIntBetween(1, 10),
                maxX + randomIntBetween(1, 10), (3 * maxY + minY) / 4)));

            // box-query outside to the right
            assertFalse(reader.intersects(Extent.fromPoints(maxX + randomIntBetween(1, 4), minY, maxX + randomIntBetween(5, 10), maxY)));
            // box-query outside to the left
            assertFalse(reader.intersects(Extent.fromPoints(maxX - randomIntBetween(5, 10), minY, minX - randomIntBetween(1, 4), maxY)));
            // box-query outside to the top
            assertFalse(reader.intersects(Extent.fromPoints(minX, maxY + randomIntBetween(1, 4), maxX, maxY + randomIntBetween(5, 10))));
            // box-query outside to the bottom
            assertFalse(reader.intersects(Extent.fromPoints(minX, minY - randomIntBetween(5, 10), maxX, minY - randomIntBetween(1, 4))));
        }
    }

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeReader reader = geometryTreeReader(new Polygon(new LinearRing(py, px), Collections.emptyList()),
            TestCoordinateEncoder.INSTANCE);
        assertTrue(reader.intersects(Extent.fromPoints(2, -1, 11, 1)));
        assertTrue(reader.intersects(Extent.fromPoints(-12, -12, 12, 12)));
        assertTrue(reader.intersects(Extent.fromPoints(-2, -1, 2, 0)));
        assertTrue(reader.intersects(Extent.fromPoints(-5, -6, 2, -2)));
    }

    // adapted from org.apache.lucene.geo.TestPolygon2D#testMultiPolygon
    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[]{-50, 50, 50, -50, -50}, new double[]{-50, -50, 50, 50, -50}),
            Collections.singletonList(new LinearRing(new double[]{-10, 10, 10, -10, -10}, new double[]{-10, -10, 10, 10, -10})));

        GeometryTreeReader reader = geometryTreeReader(polyWithHole, TestCoordinateEncoder.INSTANCE);

        assertFalse(reader.intersects(Extent.fromPoints(6, -6, 6, -6))); // in the hole
        assertTrue(reader.intersects(Extent.fromPoints(25, -25, 25, -25))); // on the mainland
        assertFalse(reader.intersects(Extent.fromPoints(51, 51, 52, 52))); // outside of mainland
        assertTrue(reader.intersects(Extent.fromPoints(-60, -60, 60, 60))); // enclosing us completely
        assertTrue(reader.intersects(Extent.fromPoints(49, 49, 51, 51))); // overlapping the mainland
        assertTrue(reader.intersects(Extent.fromPoints(9, 9, 11, 11))); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 0, 0};
        double[] py = {0, 0, 20, 20, 0, 0, 20, 20, 0, 0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        Polygon polyWithHole = new Polygon(new LinearRing(px, py), Collections.singletonList(new LinearRing(hx, hy)));
        GeometryTreeReader reader = geometryTreeReader(polyWithHole, TestCoordinateEncoder.INSTANCE);
        // test cell crossing poly
        assertTrue(reader.intersects(Extent.fromPoints(5, 10, 5, 10)));
        assertFalse(reader.intersects(Extent.fromPoints(15, 10, 15, 10)));
        assertFalse(reader.intersects(Extent.fromPoints(25, 10, 25, 10)));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeReader reader = geometryTreeReader(new Line(px, py), TestCoordinateEncoder.INSTANCE);
        assertTrue(reader.intersects(Extent.fromPoints(2, -1, 11, 1)));
        assertTrue(reader.intersects(Extent.fromPoints(-12, -12, 12, 12)));
        assertTrue(reader.intersects(Extent.fromPoints(-2, -1, 2, 0)));
        assertFalse(reader.intersects(Extent.fromPoints(-5, -6, 2, -2)));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5};

        // test cell crossing poly
        GeometryTreeReader reader = geometryTreeReader(new Line(px, py), TestCoordinateEncoder.INSTANCE);
        assertTrue(reader.intersects(Extent.fromPoints(2, -1, 11, 1)));
        assertTrue(reader.intersects(Extent.fromPoints(-12, -12, 12, 12)));
        assertTrue(reader.intersects(Extent.fromPoints(-2, -1, 2, 0)));
        assertFalse(reader.intersects(Extent.fromPoints(-5, -6, 2, -2)));
    }

    public void testPacManPoints() throws Exception {
        // pacman
        List<Point> points = Arrays.asList(
            new Point(0, 0),
            new Point(5, 10),
            new Point(9, 10),
            new Point(10, 0),
            new Point(9, -8),
            new Point(0, -10),
            new Point(-9, -8),
            new Point(-10, 0),
            new Point(-9, 10),
            new Point(-5, 10)
        );


        // candidate intersects cell
        int xMin = 0;
        int xMax = 11;
        int yMin = -10;
        int yMax = 9;

        // test cell crossing poly
        GeometryTreeReader reader = geometryTreeReader(new MultiPoint(points), TestCoordinateEncoder.INSTANCE);
        assertTrue(reader.intersects(Extent.fromPoints(xMin, yMin, xMax, yMax)));
    }

    public void testRandomMultiLineIntersections() throws IOException {
        double extentSize = randomDoubleBetween(0.01, 10, true);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        MultiLine geometry = GeometryTestUtils.randomMultiLine(false);
        geometry = (MultiLine) indexer.prepareForIndexing(geometry);

        GeometryTreeReader reader = geometryTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        Extent readerExtent = reader.getExtent();

        for (Line line : geometry) {
            // extent that intersects edges
            assertTrue(reader.intersects(bufferedExtentFromGeoPoint(line.getX(0), line.getY(0), extentSize)));

            // extent that fully encloses a line in the MultiLine
            Extent lineExtent = geometryTreeReader(line, GeoShapeCoordinateEncoder.INSTANCE).getExtent();
            assertTrue(reader.intersects(lineExtent));

            if (lineExtent.minX() != Integer.MIN_VALUE && lineExtent.maxX() != Integer.MAX_VALUE
                && lineExtent.minY() != Integer.MIN_VALUE && lineExtent.maxY() != Integer.MAX_VALUE) {
                assertTrue(reader.intersects(Extent.fromPoints(lineExtent.minX() - 1, lineExtent.minY() - 1,
                    lineExtent.maxX() + 1, lineExtent.maxY() + 1)));
            }
        }

        // extent that fully encloses the MultiLine
        assertTrue(reader.intersects(reader.getExtent()));
        if (readerExtent.minX() != Integer.MIN_VALUE && readerExtent.maxX() != Integer.MAX_VALUE
            && readerExtent.minY() != Integer.MIN_VALUE && readerExtent.maxY() != Integer.MAX_VALUE) {
            assertTrue(reader.intersects(Extent.fromPoints(readerExtent.minX() - 1, readerExtent.minY() - 1,
                readerExtent.maxX() + 1, readerExtent.maxY() + 1)));
        }

    }

    public void testRandomGeometryIntersection() throws IOException {
        int testPointCount = randomIntBetween(100, 200);
        Point[] testPoints = new Point[testPointCount];
        double extentSize = randomDoubleBetween(1, 10, true);
        boolean[] intersects = new boolean[testPointCount];
        for (int i = 0; i < testPoints.length; i++) {
            testPoints[i] = randomPoint(false);
        }

        Geometry geometry = randomGeometryTreeGeometry();
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        Geometry preparedGeometry = indexer.prepareForIndexing(geometry);

        // TODO: support multi-polygons
        if (ShapeType.POLYGON == geometry.type() && ShapeType.MULTIPOLYGON == preparedGeometry.type()) {
            return;
        }

        for (int i = 0; i < testPointCount; i++) {
            int cur = i;
            intersects[cur] = fold(preparedGeometry, false, (g, s) -> s || intersects(g, testPoints[cur], extentSize));
        }

        for (int i = 0; i < testPointCount; i++) {
            assertEquals(intersects[i], intersects(preparedGeometry, testPoints[i], extentSize));
        }
    }

    private Extent bufferedExtentFromGeoPoint(double x, double y, double extentSize) {
        int xMin = GeoShapeCoordinateEncoder.INSTANCE.encodeX(Math.max(x - extentSize, -180.0));
        int xMax = GeoShapeCoordinateEncoder.INSTANCE.encodeX(Math.min(x + extentSize, 180.0));
        int yMin = GeoShapeCoordinateEncoder.INSTANCE.encodeY(Math.max(y - extentSize, -90));
        int yMax = GeoShapeCoordinateEncoder.INSTANCE.encodeY(Math.min(y + extentSize, 90));
        return Extent.fromPoints(xMin, yMin, xMax, yMax);
    }

    private boolean intersects(Geometry g, Point p, double extentSize) throws IOException {
        return geometryTreeReader(g, GeoShapeCoordinateEncoder.INSTANCE)
            .intersects(bufferedExtentFromGeoPoint(p.getX(), p.getY(), extentSize));
    }

    private static Geometry randomGeometryTreeGeometry() {
        @SuppressWarnings("unchecked") Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint
        );
        return geometry.apply(false);
    }

    private GeometryTreeReader geometryTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        GeometryTreeWriter writer = new GeometryTreeWriter(geometry, encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        return new GeometryTreeReader(output.bytes().toBytesRef(), encoder);
    }
}
