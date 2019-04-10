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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.shape.Rectangle;

import java.io.IOException;

public class EdgeTreeTests extends ESTestCase {

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-180, 170);
            int maxX = randomIntBetween(minX + 10, 180);
            int minY = randomIntBetween(-180, 170);
            int maxY = randomIntBetween(minY + 10, 180);
            int[] x = new int[]{minX, maxX, maxX, minX, minX};
            int[] y = new int[]{minY, minY, maxY, maxY, minY};
            EdgeTreeWriter writer = new EdgeTreeWriter(x, y);
            BytesRef bytes = writer.toBytesRef();
            EdgeTreeReader reader = new EdgeTreeReader(bytes);

            // box-query touches bottom-left corner
            assertTrue(reader.containedInOrCrosses(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180), minX, minY));
            // box-query touches bottom-right corner
            assertTrue(reader.containedInOrCrosses(maxX, minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), minY));
            // box-query touches top-right corner
            assertTrue(reader.containedInOrCrosses(maxX, maxY, maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query touches top-left corner
            assertTrue(reader.containedInOrCrosses(minX - randomIntBetween(1, 180), maxY, minX, maxY + randomIntBetween(1, 180)));
            // box-query fully-enclosed inside rectangle
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4));
            // box-query fully-contains poly
            assertTrue(reader.containedInOrCrosses(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180),
                maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query half-in-half-out-right
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),
                (3 * maxY + minY) / 4));
            // box-query half-in-half-out-left
            assertTrue(reader.containedInOrCrosses(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4));
            // box-query half-in-half-out-top
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),
                maxY + randomIntBetween(1, 1000)));
            // box-query half-in-half-out-bottom
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000),
                maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));

            // box-query outside to the right
            assertFalse(reader.containedInOrCrosses(maxX + randomIntBetween(1, 1000), minY, maxX + randomIntBetween(1001, 2000), maxY));
            // box-query outside to the left
            assertFalse(reader.containedInOrCrosses(maxX - randomIntBetween(1001, 2000), minY, minX - randomIntBetween(1, 1000), maxY));
            // box-query outside to the top
            assertFalse(reader.containedInOrCrosses(minX, maxY + randomIntBetween(1, 1000), maxX, maxY + randomIntBetween(1001, 2000)));
            // box-query outside to the bottom
            assertFalse(reader.containedInOrCrosses(minX, minY - randomIntBetween(1001, 2000), maxX, minY - randomIntBetween(1, 1000)));
        }
    }

    public void testSimplePolygon() throws IOException  {
        for (int iter = 0; iter < 1000; iter++) {
            ShapeBuilder builder = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POLYGON);
            Polygon geo = (Polygon) builder.buildGeometry();
            Rectangle box = builder.buildS4J().getBoundingBox();
            int minXBox = (int) box.getMinX();
            int minYBox = (int) box.getMinY();
            int maxXBox = (int) box.getMaxX();
            int maxYBox = (int) box.getMaxY();

            int[] x = asIntArray(geo.getPolygon().getLons());
            int[] y = asIntArray(geo.getPolygon().getLats());

            EdgeTreeWriter writer = new EdgeTreeWriter(x, y);
            EdgeTreeReader reader = new EdgeTreeReader(writer.toBytesRef());
            // polygon fully contained within box
            assertTrue(reader.containedInOrCrosses(minXBox, minYBox, maxXBox, maxYBox));
            // containedInOrCrosses
            if (maxYBox - 1 >= minYBox) {
                assertTrue(reader.containedInOrCrosses(minXBox, minYBox, maxXBox, maxYBox - 1));
            }
            if (maxXBox -1 >= minXBox) {
                assertTrue(reader.containedInOrCrosses(minXBox, minYBox, maxXBox - 1, maxYBox));
            }
            // does not cross
            assertFalse(reader.containedInOrCrosses(maxXBox + 1, maxYBox + 1, maxXBox + 10, maxYBox + 10));
        }
    }

    public void testPacMan() throws Exception {
        // pacman
        int[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        int[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // candidate containedInOrCrosses cell
        int xMin = 2;//-5;
        int xMax = 11;//0.000001;
        int yMin = -1;//0;
        int yMax = 1;//5;

        // test cell crossing poly
        EdgeTreeWriter writer = new EdgeTreeWriter(px, py);
        EdgeTreeReader reader = new EdgeTreeReader(writer.toBytesRef());
        assertTrue(reader.containsBottomLeft(xMin, yMin, xMax, yMax));
    }

    private int[] asIntArray(double[] doub) {
        int[] intArr = new int[doub.length];
        for (int i = 0; i < intArr.length; i++) {
            intArr[i] = (int) doub[i];
        }
        return intArr;
    }
}
