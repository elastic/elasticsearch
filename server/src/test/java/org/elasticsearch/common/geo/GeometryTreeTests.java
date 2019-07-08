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
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GeometryTreeTests extends ESTestCase {

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-180, 170);
            int maxX = randomIntBetween(minX + 10, 180);
            int minY = randomIntBetween(-90, 80);
            int maxY = randomIntBetween(minY + 10, 90);
            double[] x = new double[]{minX, maxX, maxX, minX, minX};
            double[] y = new double[]{minY, minY, maxY, maxY, minY};
            GeometryTreeWriter writer = new GeometryTreeWriter(new Polygon(new LinearRing(y, x), Collections.emptyList()));

            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());

            assertThat(reader.getExtent(), equalTo(new Extent(minX, minY, maxX, maxY)));

            // box-query touches bottom-left corner
            assertTrue(reader.intersects(new Extent(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180), minX, minY)));
            // box-query touches bottom-right corner
            assertTrue(reader.intersects(new Extent(maxX, minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), minY)));
            // box-query touches top-right corner
            assertTrue(reader.intersects(new Extent(maxX, maxY, maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180))));
            // box-query touches top-left corner
            assertTrue(reader.intersects(new Extent(minX - randomIntBetween(1, 180), maxY, minX, maxY + randomIntBetween(1, 180))));
            // box-query fully-enclosed inside rectangle
            assertTrue(reader.intersects(new Extent((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4)));
            // box-query fully-contains poly
            assertTrue(reader.intersects(new Extent(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180),
                maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180))));
            // box-query half-in-half-out-right
            assertTrue(reader.intersects(new Extent((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),
                (3 * maxY + minY) / 4)));
            // box-query half-in-half-out-left
            assertTrue(reader.intersects(new Extent(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4)));
            // box-query half-in-half-out-top
            assertTrue(reader.intersects(new Extent((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),
                maxY + randomIntBetween(1, 1000))));
            // box-query half-in-half-out-bottom
            assertTrue(reader.intersects(new Extent((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000),
                maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4)));

            // box-query outside to the right
            assertFalse(reader.intersects(new Extent(maxX + randomIntBetween(1, 1000), minY, maxX + randomIntBetween(1001, 2000), maxY)));
            // box-query outside to the left
            assertFalse(reader.intersects(new Extent(maxX - randomIntBetween(1001, 2000), minY, minX - randomIntBetween(1, 1000), maxY)));
            // box-query outside to the top
            assertFalse(reader.intersects(new Extent(minX, maxY + randomIntBetween(1, 1000), maxX, maxY + randomIntBetween(1001, 2000))));
            // box-query outside to the bottom
            assertFalse(reader.intersects(new Extent(minX, minY - randomIntBetween(1001, 2000), maxX, minY - randomIntBetween(1, 1000))));
        }
    }

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Polygon(new LinearRing(py, px), Collections.emptyList()));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(new Extent(2, -1, 11, 1)));
        assertTrue(reader.intersects(new Extent(-12, -12, 12, 12)));
        assertTrue(reader.intersects(new Extent(-2, -1, 2, 0)));
        assertTrue(reader.intersects(new Extent(-5, -6, 2, -2)));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Line(px, py));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(new Extent(2, -1, 11, 1)));
        assertTrue(reader.intersects(new Extent(-12, -12, 12, 12)));
        assertTrue(reader.intersects(new Extent(-2, -1, 2, 0)));
        assertFalse(reader.intersects(new Extent(-5, -6, 2, -2)));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Line(px, py));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(new Extent(2, -1, 11, 1)));
        assertTrue(reader.intersects(new Extent(-12, -12, 12, 12)));
        assertTrue(reader.intersects(new Extent(-2, -1, 2, 0)));
        assertFalse(reader.intersects(new Extent(-5, -6, 2, -2)));
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
        GeometryTreeWriter writer = new GeometryTreeWriter(new MultiPoint(points));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(new Extent(xMin, yMin, xMax, yMax)));
    }
}
