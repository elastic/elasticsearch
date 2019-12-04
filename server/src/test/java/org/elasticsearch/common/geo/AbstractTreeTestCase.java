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

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.query.LegacyGeoShapeQueryProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.geo.GeoTestUtils.assertRelation;
import static org.elasticsearch.geo.GeometryTestUtils.fold;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractTreeTestCase extends ESTestCase {

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
            ShapeTreeReader reader = geometryTreeReader(rectangle, TestCoordinateEncoder.INSTANCE);

            assertThat(Extent.fromPoints(minX, minY, maxX, maxY), equalTo(reader.getExtent()));
            // encoder loses precision when casting to integer, so centroid is calculated using integer division here
            assertThat(reader.getCentroidX(), equalTo((double) ((minX + maxX) / 2)));
            assertThat(reader.getCentroidY(), equalTo((double) ((minY + maxY) / 2)));

            // box-query touches bottom-left corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 180),
                minY - randomIntBetween(1, 180), minX, minY));
            // box-query touches bottom-right corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(maxX, minY - randomIntBetween(1, 180),
                maxX + randomIntBetween(1, 180), minY));
            // box-query touches top-right corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(maxX, maxY, maxX + randomIntBetween(1, 180),
                maxY + randomIntBetween(1, 180)));
            // box-query touches top-left corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 180), maxY, minX,
                maxY + randomIntBetween(1, 180)));
            // box-query fully-enclosed inside rectangle
            assertRelation(GeoRelation.QUERY_INSIDE,reader, Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4,
                (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));
            // box-query fully-contains poly
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 180),
                minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query half-in-half-out-right
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4,
                maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));
            // box-query half-in-half-out-left
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4,
                (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));
            // box-query half-in-half-out-top
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4,
                maxX + randomIntBetween(1, 1000), maxY + randomIntBetween(1, 1000)));
            // box-query half-in-half-out-bottom
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000),
                maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));

            // box-query outside to the right
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(maxX + randomIntBetween(1, 1000), minY,
                maxX + randomIntBetween(1001, 2000), maxY));
            // box-query outside to the left
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(maxX - randomIntBetween(1001, 2000), minY,
                minX - randomIntBetween(1, 1000), maxY));
            // box-query outside to the top
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(minX, maxY + randomIntBetween(1, 1000), maxX,
                maxY + randomIntBetween(1001, 2000)));
            // box-query outside to the bottom
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(minX, minY - randomIntBetween(1001, 2000), maxX,
                minY - randomIntBetween(1, 1000)));
        }
    }

    public void testSimplePolygon() throws IOException  {
        for (int iter = 0; iter < 1000; iter++) {
            GeoShapeIndexer indexer = new GeoShapeIndexer(true, "name");
            ShapeBuilder builder = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POLYGON);
            Polygon geo = (Polygon) builder.buildGeometry();
            Geometry geometry = indexer.prepareForIndexing(geo);
            Polygon testPolygon;
            if (geometry instanceof Polygon) {
                testPolygon = (Polygon) geometry;
            } else if (geometry instanceof MultiPolygon) {
                testPolygon = ((MultiPolygon) geometry).get(0);
            } else {
                throw new IllegalStateException("not a polygon");
            }
            builder = LegacyGeoShapeQueryProcessor.geometryToShapeBuilder(testPolygon);
            org.locationtech.spatial4j.shape.Rectangle box = builder.buildS4J().getBoundingBox();
            int minXBox = TestCoordinateEncoder.INSTANCE.encodeX(box.getMinX());
            int minYBox = TestCoordinateEncoder.INSTANCE.encodeY(box.getMinY());
            int maxXBox = TestCoordinateEncoder.INSTANCE.encodeX(box.getMaxX());
            int maxYBox = TestCoordinateEncoder.INSTANCE.encodeY(box.getMaxY());

            double[] x = testPolygon.getPolygon().getLons();
            double[] y = testPolygon.getPolygon().getLats();

            EdgeTreeWriter writer = new EdgeTreeWriter(x, y, TestCoordinateEncoder.INSTANCE, true);
            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            EdgeTreeReader reader = new EdgeTreeReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)), true);
            Extent actualExtent = reader.getExtent();
            assertThat(actualExtent.minX(), equalTo(minXBox));
            assertThat(actualExtent.maxX(), equalTo(maxXBox));
            assertThat(actualExtent.minY(), equalTo(minYBox));
            assertThat(actualExtent.maxY(), equalTo(maxYBox));
            // polygon fully contained within box
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minXBox, minYBox, maxXBox, maxYBox));
            // relate
            if (maxYBox - 1 >= minYBox) {
                assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minXBox, minYBox, maxXBox, maxYBox - 1));
            }
            if (maxXBox -1 >= minXBox) {
                assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minXBox, minYBox, maxXBox - 1, maxYBox));
            }
            // does not cross
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(maxXBox + 1, maxYBox + 1, maxXBox + 10, maxYBox + 10));
        }
    }

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        ShapeTreeReader reader = geometryTreeReader(new Polygon(new LinearRing(py, px), Collections.emptyList()),
            TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_INSIDE, reader, Extent.fromPoints(-5, -6, 2, -2));
    }

    // adapted from org.apache.lucene.geo.TestPolygon2D#testMultiPolygon
    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[]{-50, 50, 50, -50, -50}, new double[]{-50, -50, 50, 50, -50}),
            Collections.singletonList(new LinearRing(new double[]{-10, 10, 10, -10, -10}, new double[]{-10, -10, 10, 10, -10})));

        ShapeTreeReader reader = geometryTreeReader(polyWithHole, TestCoordinateEncoder.INSTANCE);

        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(6, -6, 6, -6)); // in the hole
        assertRelation(GeoRelation.QUERY_INSIDE, reader, Extent.fromPoints(25, -25, 25, -25)); // on the mainland
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(51, 51, 52, 52)); // outside of mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-60, -60, 60, 60)); // enclosing us completely
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(49, 49, 51, 51)); // overlapping the mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(9, 9, 11, 11)); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 0, 0};
        double[] py = {0, 0, 20, 20, 0, 0, 20, 20, 0, 0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        Polygon polyWithHole = new Polygon(new LinearRing(px, py), Collections.singletonList(new LinearRing(hx, hy)));
        ShapeTreeReader reader = geometryTreeReader(polyWithHole, TestCoordinateEncoder.INSTANCE);
        // test cell crossing poly
        assertRelation(GeoRelation.QUERY_INSIDE, reader, Extent.fromPoints(5, 10, 5, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(15, 10, 15, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(25, 10, 25, 10));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        ShapeTreeReader reader = geometryTreeReader(new Line(px, py), TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(-5, -6, 2, -2));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5};

        // test cell crossing poly
        ShapeTreeReader reader = geometryTreeReader(new Line(px, py), TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(-5, -6, 2, -2));
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
        ShapeTreeReader reader = geometryTreeReader(new MultiPoint(points), TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(xMin, yMin, xMax, yMax));
    }

    public void testRandomMultiLineIntersections() throws IOException {
        double extentSize = randomDoubleBetween(0.01, 10, true);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        MultiLine geometry = GeometryTestUtils.randomMultiLine(false);
        geometry = (MultiLine) indexer.prepareForIndexing(geometry);

        ShapeTreeReader reader = geometryTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        Extent readerExtent = reader.getExtent();

        for (Line line : geometry) {
            // extent that intersects edges
            assertRelation(GeoRelation.QUERY_CROSSES, reader, bufferedExtentFromGeoPoint(line.getX(0), line.getY(0), extentSize));

            // extent that fully encloses a line in the MultiLine
            Extent lineExtent = geometryTreeReader(line, GeoShapeCoordinateEncoder.INSTANCE).getExtent();
            assertRelation(GeoRelation.QUERY_CROSSES, reader, lineExtent);

            if (lineExtent.minX() != Integer.MIN_VALUE && lineExtent.maxX() != Integer.MAX_VALUE
                && lineExtent.minY() != Integer.MIN_VALUE && lineExtent.maxY() != Integer.MAX_VALUE) {
                assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(lineExtent.minX() - 1, lineExtent.minY() - 1,
                    lineExtent.maxX() + 1, lineExtent.maxY() + 1));
            }
        }

        // extent that fully encloses the MultiLine
        assertRelation(GeoRelation.QUERY_CROSSES, reader, reader.getExtent());
        if (readerExtent.minX() != Integer.MIN_VALUE && readerExtent.maxX() != Integer.MAX_VALUE
            && readerExtent.minY() != Integer.MIN_VALUE && readerExtent.maxY() != Integer.MAX_VALUE) {
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(readerExtent.minX() - 1, readerExtent.minY() - 1,
                readerExtent.maxX() + 1, readerExtent.maxY() + 1));
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

        Extent bufferBounds = bufferedExtentFromGeoPoint(p.getX(), p.getY(), extentSize);
        GeoRelation relation = geometryTreeReader(g, GeoShapeCoordinateEncoder.INSTANCE)
            .relate(bufferBounds.minX(), bufferBounds.minY(), bufferBounds.maxX(), bufferBounds.maxY());
        return relation == GeoRelation.QUERY_CROSSES || relation == GeoRelation.QUERY_INSIDE;
    }

    private static Geometry randomGeometryTreeGeometry() {
        return randomGeometryTreeGeometry(0);
    }

    private static Geometry randomGeometryTreeGeometry(int level) {
        @SuppressWarnings("unchecked") Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint,
            level < 3 ? (b) -> randomGeometryTreeCollection(level + 1) : GeometryTestUtils::randomPoint // don't build too deep
        );
        return geometry.apply(false);
    }

    private static Geometry randomGeometryTreeCollection(int level) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometryTreeGeometry(level));
        }
        return new GeometryCollection<>(shapes);
    }

    protected abstract ShapeTreeReader geometryTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException;
}
