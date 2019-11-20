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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.geo.GeoTestUtils.assertRelation;
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
            GeometryTreeWriter writer = new GeometryTreeWriter(rectangle, TestCoordinateEncoder.INSTANCE);

            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);

            assertThat(Extent.fromPoints(minX, minY, maxX, maxY), equalTo(reader.getExtent()));
            // encoder loses precision when casting to integer, so centroid is calculated using integer division here
            assertThat(reader.getCentroidX(), equalTo((double) ((minX + maxX)/2)));
            assertThat(reader.getCentroidY(), equalTo((double) ((minY + maxY)/2)));

            // box-query touches bottom-left corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180), minX, minY));
            // box-query touches bottom-right corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(maxX, minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), minY));
            // box-query touches top-right corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(maxX, maxY, maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query touches top-left corner
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 180), maxY, minX, maxY + randomIntBetween(1, 180)));
            // box-query fully-enclosed inside rectangle
            assertRelation(GeoRelation.QUERY_INSIDE,reader, Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4));
            // box-query fully-contains poly
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query half-in-half-out-right
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));
            // box-query half-in-half-out-left
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));
            // box-query half-in-half-out-top
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000), maxY + randomIntBetween(1, 1000)));
            // box-query half-in-half-out-bottom
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000), maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));

            // box-query outside to the right
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(maxX + randomIntBetween(1, 1000), minY, maxX + randomIntBetween(1001, 2000), maxY));
            // box-query outside to the left
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(maxX - randomIntBetween(1001, 2000), minY, minX - randomIntBetween(1, 1000), maxY));
            // box-query outside to the top
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(minX, maxY + randomIntBetween(1, 1000), maxX, maxY + randomIntBetween(1001, 2000)));
            // box-query outside to the bottom
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(minX, minY - randomIntBetween(1001, 2000), maxX, minY - randomIntBetween(1, 1000)));
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
        GeometryTreeWriter writer = new GeometryTreeWriter(new Polygon(new LinearRing(py, px), Collections.emptyList()),
            TestCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_INSIDE, reader, Extent.fromPoints(-5, -6, 2, -2));
    }

    // adapted from org.apache.lucene.geo.TestPolygon2D#testMultiPolygon
    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[] { -50, 50, 50, -50, -50 }, new double[] { -50, -50, 50, 50, -50 }),
            Collections.singletonList(new LinearRing(new double[] { -10, 10, 10, -10, -10 }, new double[] { -10, -10, 10, 10, -10 })));

        GeometryTreeWriter writer = new GeometryTreeWriter(polyWithHole, TestCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), null);

        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(6, -6, 6, -6)); // in the hole
        assertRelation(GeoRelation.QUERY_INSIDE, reader, Extent.fromPoints(25, -25, 25, -25)); // on the mainland
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(51, 51, 52, 52)); // outside of mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(-60, -60, 60, 60)); // enclosing us completely
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(49, 49, 51, 51)); // overlapping the mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(9, 9, 11, 11)); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50,  0, 0};
        double[] py = {0, 0,  20, 20,  0,  0, 20, 20,  0,  0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        Polygon polyWithHole = new Polygon(new LinearRing(px, py),  Collections.singletonList(new LinearRing(hx, hy)));
        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(polyWithHole, TestCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_INSIDE, reader, Extent.fromPoints(5, 10, 5, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(15, 10, 15, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, Extent.fromPoints(25, 10, 25, 10));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Line(px, py), TestCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);
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
        GeometryTreeWriter writer = new GeometryTreeWriter(new Line(px, py), TestCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);
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
        GeometryTreeWriter writer = new GeometryTreeWriter(new MultiPoint(points), TestCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(xMin, yMin, xMax, yMax));
    }
}
