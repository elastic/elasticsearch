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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GeometryTriangleTreeTests extends ESTestCase {

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
            TriangleTreeWriter writer = new TriangleTreeWriter(rectangle, TestCoordinateEncoder.INSTANCE);

            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(), TestCoordinateEncoder.INSTANCE);

            assertThat(new int[] {minX, maxX , minY, maxY}, equalTo(reader.getExtent()));
            // encoder loses precision when casting to integer, so centroid is calculated using integer division here
            assertThat(reader.getCentroidX(), equalTo((double) ((minX + maxX)/2)));
            assertThat(reader.getCentroidY(), equalTo((double) ((minY + maxY)/2)));

            // box-query touches bottom-left corner
            assertTrue(reader.intersects(minX - randomIntBetween(1, 10), minX,
                minY - randomIntBetween(1, 10), minY));
            // box-query touches bottom-right corner
            assertTrue(reader.intersects(maxX, maxX + randomIntBetween(1, 10),
                minY - randomIntBetween(1, 10),  minY));
            // box-query touches top-right corner
            assertTrue(reader.intersects(maxX, maxX + randomIntBetween(1, 10),
                maxY, maxY + randomIntBetween(1, 10)));
            // box-query touches top-left corner
            assertTrue(reader.intersects(minX - randomIntBetween(1, 10), minX,
                maxY, maxY + randomIntBetween(1, 10)));
            // box-query fully-enclosed inside rectangle
            assertTrue(reader.intersects((3 * minX + maxX) / 4, (3 * maxX + minX) / 4,
                (3 * minY + maxY) / 4, (3 * maxY + minY) / 4));
            // box-query fully-contains poly
            assertTrue(reader.intersects(minX - randomIntBetween(1, 10), maxX + randomIntBetween(1, 10),
                minY - randomIntBetween(1, 10), maxY + randomIntBetween(1, 10)));
            // box-query half-in-half-out-right
            assertTrue(reader.intersects((3 * minX + maxX) / 4, maxX + randomIntBetween(1, 10),
                (3 * minY + maxY) / 4, (3 * maxY + minY) / 4));
            // box-query half-in-half-out-left
            assertTrue(reader.intersects(minX - randomIntBetween(1, 10), (3 * maxX + minX) / 4,
                (3 * minY + maxY) / 4, (3 * maxY + minY) / 4));
            // box-query half-in-half-out-top
            assertTrue(reader.intersects((3 * minX + maxX) / 4, maxX + randomIntBetween(1, 10),
                (3 * minY + maxY) / 4, maxY + randomIntBetween(1, 10)));
            // box-query half-in-half-out-bottom
            assertTrue(reader.intersects((3 * minX + maxX) / 4, maxX + randomIntBetween(1, 10),
                minY - randomIntBetween(1, 10), (3 * maxY + minY) / 4));

            // box-query outside to the right
            assertFalse(reader.intersects(maxX + randomIntBetween(1, 4), maxX + randomIntBetween(5, 10),
                minY, maxY));
            // box-query outside to the left
            assertFalse(reader.intersects(maxX - randomIntBetween(5, 10), minX - randomIntBetween(1, 4),
                minY, maxY));
            // box-query outside to the top
            assertFalse(reader.intersects(minX, maxX, maxY + randomIntBetween(1, 4),
                maxY + randomIntBetween(5, 10)));
            // box-query outside to the bottom
            assertFalse(reader.intersects(minX, maxX, minY - randomIntBetween(5, 10),
                minY - randomIntBetween(1, 4)));
        }
    }

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;

        // test cell crossing poly
        TriangleTreeWriter writer = new TriangleTreeWriter(
            new Polygon(new LinearRing(py, px), Collections.emptyList()), encoder);
        //System.out.println(writer.node.size(false, writer.node.minX, writer.node.minY));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(),  encoder);
        assertTrue(reader.intersects(2,  11, -1, 1));
        assertTrue(reader.intersects(-12, 12, -12, 12));
        assertTrue(reader.intersects(-2,  2, -1, 0));
        assertTrue(reader.intersects(-5,  2, -6, -2));
    }

    public void testSimple() throws Exception {
        double[] px = {0, 1, 1, 0, 0};
        double[] py = {0, 0, 1, 1, 0};

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;
        // test cell crossing poly
        TriangleTreeWriter writer = new TriangleTreeWriter(new Polygon(new LinearRing(px, py), Collections.emptyList()),
            encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(),  encoder);
        assertFalse(reader.intersects(encoder.encodeX(2),  encoder.encodeX(11),
            encoder.encodeY(-1), encoder.encodeY(1)));
        assertTrue(reader.intersects(encoder.encodeX(-12), encoder.encodeX(12),
            encoder.encodeY(-12), encoder.encodeY(12)));
        assertTrue(reader.intersects(encoder.encodeX(-2),  encoder.encodeX(2),
            encoder.encodeY(-1), encoder.encodeY(0)));
        assertTrue(reader.intersects(encoder.encodeX(-2),  encoder.encodeX(0.5),
            encoder.encodeY(0.5), encoder.encodeY(1.5)));
        assertFalse(reader.intersects(encoder.encodeX(-5), encoder.encodeX( 2),
            encoder.encodeY(-6), encoder.encodeY(-2)));
    }



    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[] { -50, 50, 50, -50, -50 },
            new double[] { -50, -50, 50, 50, -50 }),
            Collections.singletonList(new LinearRing(new double[] { -10, 10, 10, -10, -10 },
                new double[] { -10, -10, 10, 10, -10 })));

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;

        TriangleTreeWriter writer = new TriangleTreeWriter(polyWithHole, encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(), encoder);

        assertFalse(reader.intersects(encoder.encodeX(6), encoder.encodeX(6),
            encoder.encodeY(-6), encoder.encodeY(-6))); // in the hole
        assertTrue(reader.intersects(encoder.encodeX(25), encoder.encodeX(25),
            encoder.encodeY(-25), encoder.encodeY(-25))); // on the mainland
        assertFalse(reader.intersects(encoder.encodeX(51), encoder.encodeX(52),
            encoder.encodeY(51), encoder.encodeY(52))); // outside of mainland
        assertTrue(reader.intersects(encoder.encodeX(-60), encoder.encodeX(60),
            encoder.encodeY(-60), encoder.encodeY(60))); // enclosing us completely
        assertTrue(reader.intersects(encoder.encodeX(49), encoder.encodeX(51),
            encoder.encodeY(49), encoder.encodeY(51))); // overlapping the mainland
        assertTrue(reader.intersects(encoder.encodeX(9), encoder.encodeX(11),
            encoder.encodeY(9), encoder.encodeY(11))); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50,  0, 0};
        double[] py = {0, 0,  20, 20,  0,  0, 20, 20,  0,  0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;
        Polygon polyWithHole = new Polygon(new LinearRing(px, py), Collections.singletonList(new LinearRing(hx, hy)));
        // test cell crossing poly
        TriangleTreeWriter writer = new TriangleTreeWriter(polyWithHole, encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(), encoder);
        assertTrue(reader.intersects(encoder.encodeX(5), encoder.encodeX(5),
            encoder.encodeY(10), encoder.encodeY(10)));
        assertFalse(reader.intersects(encoder.encodeX(15), encoder.encodeX(15),
            encoder.encodeY(10), encoder.encodeY(10)));
        assertFalse(reader.intersects(encoder.encodeX(25), encoder.encodeX(25),
            encoder.encodeY(10), encoder.encodeY(10)));

    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;
        // test cell crossing poly
        TriangleTreeWriter writer = new TriangleTreeWriter(new Line(px, py), encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(), encoder);
        assertTrue(reader.intersects(encoder.encodeX(2), encoder.encodeX(11),
            encoder.encodeY(-1), encoder.encodeY(1)));
        assertTrue(reader.intersects(encoder.encodeX(-12), encoder.encodeX(12),
            encoder.encodeY(-12), encoder.encodeY(12)));
        assertTrue(reader.intersects(encoder.encodeX(-2), encoder.encodeX(2),
            encoder.encodeY(-1), encoder.encodeY(0)));
        assertFalse(reader.intersects(encoder.encodeX(-5), encoder.encodeX(2),
            encoder.encodeY(-6), encoder.encodeY(-2)));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5};

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;

        // test cell crossing poly
        TriangleTreeWriter writer = new TriangleTreeWriter(new Line(px, py), encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(), encoder);
        assertTrue(reader.intersects(encoder.encodeX(2), encoder.encodeX(11),
            encoder.encodeY(-1), encoder.encodeY(1)));
        assertTrue(reader.intersects(encoder.encodeX(-12), encoder.encodeX(12),
            encoder.encodeY(-12), encoder.encodeY(12)));
        assertTrue(reader.intersects(encoder.encodeX(-2), encoder.encodeX(2),
            encoder.encodeY(-1), encoder.encodeY(0)));
        assertFalse(reader.intersects(encoder.encodeX(-5), encoder.encodeX(2),
            encoder.encodeY(-6), encoder.encodeY(-2)));
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

        CoordinateEncoder encoder = TestCoordinateEncoder.INSTANCE;
        // candidate intersects cell
        int xMin = encoder.encodeX(0);
        int xMax = encoder.encodeX(11);
        int yMin = encoder.encodeY(-10);
        int yMax = encoder.encodeY(9);
        // test cell crossing poly
        TriangleTreeWriter writer = new TriangleTreeWriter(new MultiPoint(points), encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(output.bytes().toBytesRef(), encoder);
        assertTrue(reader.intersects(xMin, xMax, yMin, yMax));
    }
}
