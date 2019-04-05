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

import java.io.EOFException;
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
            LinearRingEdgeTreeWriter writer = new LinearRingEdgeTreeWriter(x, y);
            BytesRef bytes = writer.toBytesRef();
            LinearRingEdgeTreeReader reader = new LinearRingEdgeTreeReader(bytes);

            // box-query touches bottom-left corner
            assertTrue(reader.crosses(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180), minX, minY));
            assertTrue(reader.containedIn(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180), minX, minY));
            // box-query touches bottom-right corner
            assertTrue(reader.crosses(maxX, minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), minY));
            assertTrue(reader.containedIn(maxX, minY - randomIntBetween(1, 180), maxX + randomIntBetween(1, 180), minY));
            // box-query touches top-right corner
            assertTrue(reader.crosses(maxX, maxY, maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            assertTrue(reader.containedIn(maxX, maxY, maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query touches top-left corner
            assertTrue(reader.crosses(minX - randomIntBetween(1, 180), maxY, minX, maxY + randomIntBetween(1, 180)));

            /** TODO(talevy): COME ON! Right Ray Test! */
            //assertTrue(reader.containedIn(minX - randomIntBetween(1, 180), maxY, minX, maxY + randomIntBetween(1, 180)));

            // box-query fully-enclosed inside rectangle
            assertTrue(reader.containedIn((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));
            assertFalse(reader.crosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));

            // box-query fully-contains poly
            assertTrue(reader.containedIn(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180),
                maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));

            // box-query half-in-half-out-right
            // assertFalse(reader.containedIn((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));
            assertTrue(reader.crosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));
            // box-query half-in-half-out-left
            // assertTrue(reader.containedIn(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));
            assertTrue(reader.crosses(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4, (3 * maxY + minY) / 4));
            // box-query half-in-half-out-top
            // assertTrue(reader.containedIn((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),  maxY + randomIntBetween(1, 1000)));
            assertTrue(reader.crosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),  maxY + randomIntBetween(1, 1000)));
            // box-query half-in-half-out-bottom
            // assertTrue(reader.containedIn((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000), maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));
            assertTrue(reader.crosses((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000), maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));

            // box-query outside to the right
            assertFalse(reader.containedIn(maxX + randomIntBetween(1, 1000), minY, maxX + randomIntBetween(1001, 2000), maxY));
            assertFalse(reader.crosses(maxX + randomIntBetween(1, 1000), minY, maxX + randomIntBetween(1001, 2000), maxY));
            // box-query outside to the left
            assertFalse(reader.containedIn(maxX - randomIntBetween(1001, 2000), minY, minX - randomIntBetween(1, 1000), maxY));
            assertFalse(reader.crosses(maxX - randomIntBetween(1001, 2000), minY, minX - randomIntBetween(1, 1000), maxY));
            // box-query outside to the top
            assertFalse(reader.containedIn(minX, maxY + randomIntBetween(1, 1000), maxX, maxY + randomIntBetween(1001, 2000)));
            assertFalse(reader.crosses(minX, maxY + randomIntBetween(1, 1000), maxX, maxY + randomIntBetween(1001, 2000)));
            // box-query outside to the bottom
            assertFalse(reader.containedIn(minX, minY - randomIntBetween(1001, 2000), maxX, minY - randomIntBetween(1, 1000)));
            assertFalse(reader.crosses(minX, minY - randomIntBetween(1001, 2000), maxX, minY - randomIntBetween(1, 1000)));
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

            LinearRingEdgeTreeWriter writer = new LinearRingEdgeTreeWriter(x, y);
            LinearRingEdgeTreeReader reader = new LinearRingEdgeTreeReader(writer.toBytesRef());
            // box contained within polygon
            assertTrue(reader.containedIn((minXBox + maxXBox) / 2, (minYBox + maxYBox) / 2, maxXBox - 1, maxYBox - 1));
            // fully contained within box
            assertTrue(reader.containedIn(minXBox, minYBox, maxXBox, maxYBox));
            // crosses
            assertTrue(reader.containedIn(minXBox, minYBox, maxXBox, maxYBox - 1));
            // does not cross
            assertFalse(reader.containedIn(maxXBox + 1, maxYBox + 1, maxXBox + 10, maxYBox + 10));
        }
    }

    public void testPacMan() throws Exception {
        // pacman
        int[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        int[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // candidate crosses cell
        int xMin = 2;//-5;
        int xMax = 11;//0.000001;
        int yMin = -1;//0;
        int yMax = 1;//5;

        // test cell crossing poly
        LinearRingEdgeTreeWriter writer = new LinearRingEdgeTreeWriter(px, py);
        LinearRingEdgeTreeReader reader = new LinearRingEdgeTreeReader(writer.toBytesRef());
        assertTrue(reader.containedIn(xMin, yMin, xMax, yMax));
    }

    private int[] asIntArray(double[] doub) {
        int[] intArr = new int[doub.length];
        for (int i = 0; i < intArr.length; i++) {
            intArr[i] = (int) doub[i];
        }
        return intArr;
    }
}
