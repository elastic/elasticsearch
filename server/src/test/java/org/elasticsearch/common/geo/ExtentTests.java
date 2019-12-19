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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExtentTests extends ESTestCase {

    public void testFromPoint() {
        int x = randomFrom(-1, 0, 1);
        int y = randomFrom(-1, 0, 1);
        Extent extent = Extent.fromPoint(x, y);
        assertThat(extent.minX(), equalTo(x));
        assertThat(extent.maxX(), equalTo(x));
        assertThat(extent.minY(), equalTo(y));
        assertThat(extent.maxY(), equalTo(y));
    }

    public void testFromPoints() {
        int bottomLeftX = randomFrom(-10, 0, 10);
        int bottomLeftY = randomFrom(-10, 0, 10);
        int topRightX = bottomLeftX + randomIntBetween(0, 20);
        int topRightY = bottomLeftX + randomIntBetween(0, 20);
        Extent extent = Extent.fromPoints(bottomLeftX, bottomLeftY, topRightX, topRightY);
        assertThat(extent.minX(), equalTo(bottomLeftX));
        assertThat(extent.maxX(), equalTo(topRightX));
        assertThat(extent.minY(), equalTo(bottomLeftY));
        assertThat(extent.maxY(), equalTo(topRightY));
        assertThat(extent.top, equalTo(topRightY));
        assertThat(extent.bottom, equalTo(bottomLeftY));
        if (bottomLeftX < 0 && topRightX < 0) {
            assertThat(extent.negLeft, equalTo(bottomLeftX));
            assertThat(extent.negRight, equalTo(topRightX));
            assertThat(extent.posLeft, equalTo(Integer.MAX_VALUE));
            assertThat(extent.posRight, equalTo(Integer.MIN_VALUE));
        } else if (bottomLeftX < 0) {
            assertThat(extent.negLeft, equalTo(bottomLeftX));
            assertThat(extent.negRight, equalTo(bottomLeftX));
            assertThat(extent.posLeft, equalTo(topRightX));
            assertThat(extent.posRight, equalTo(topRightX));
        } else {
            assertThat(extent.negLeft, equalTo(Integer.MAX_VALUE));
            assertThat(extent.negRight, equalTo(Integer.MIN_VALUE));
            assertThat(extent.posLeft, equalTo(bottomLeftX));
            assertThat(extent.posRight, equalTo(topRightX));
        }
    }

    public void testAddRectangle() {
        Extent extent = new Extent();
        int bottomLeftX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(-175);
        int bottomLeftY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(-10);
        int topRightX =  GeoShapeCoordinateEncoder.INSTANCE.encodeX(-170);
        int topRightY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(10);
        extent.addRectangle(bottomLeftX, bottomLeftY, topRightX, topRightY);
        assertThat(extent.minX(), equalTo(bottomLeftX));
        assertThat(extent.maxX(), equalTo(topRightX));
        assertThat(extent.minY(), equalTo(bottomLeftY));
        assertThat(extent.maxY(), equalTo(topRightY));
        int bottomLeftX2 = GeoShapeCoordinateEncoder.INSTANCE.encodeX(170);
        int bottomLeftY2 = GeoShapeCoordinateEncoder.INSTANCE.encodeY(-20);
        int topRightX2 =  GeoShapeCoordinateEncoder.INSTANCE.encodeX(175);
        int topRightY2 = GeoShapeCoordinateEncoder.INSTANCE.encodeY(20);
        extent.addRectangle(bottomLeftX2, bottomLeftY2, topRightX2, topRightY2);
        assertThat(extent.minX(), equalTo(bottomLeftX));
        assertThat(extent.maxX(), equalTo(topRightX2));
        assertThat(extent.minY(), equalTo(bottomLeftY2));
        assertThat(extent.maxY(), equalTo(topRightY2));
    }

    public void testSerialize() throws IOException {
        for (int i =0; i < 100; i++) {
            Extent extent = randomExtent();
            ByteBuffersDataOutput output = new ByteBuffersDataOutput();
            extent.writeCompressed(output);
            BytesRef bytesRef = new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size()));
            ByteArrayDataInput input = new ByteArrayDataInput();
            input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            Extent copyExtent = new Extent();
            Extent.readFromCompressed(input, copyExtent);
            assertEquals(extent, copyExtent);
        }
    }

    private Extent randomExtent() {
        Extent extent = new Extent();
        int numberPoints = random().nextBoolean() ? 1 : randomIntBetween(2, 10);
        for (int i =0; i < numberPoints; i++) {
            Rectangle rectangle = GeometryTestUtils.randomRectangle();
            while (rectangle.getMinX() > rectangle.getMaxX()) {
                rectangle = GeometryTestUtils.randomRectangle();
            }
            int bottomLeftX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(rectangle.getMinX());
            int bottomLeftY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(rectangle.getMinY());
            int topRightX =  GeoShapeCoordinateEncoder.INSTANCE.encodeX(rectangle.getMaxX());
            int topRightY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(rectangle.getMaxY());
            extent.addRectangle(bottomLeftX, bottomLeftY, topRightX, topRightY);
        }
        return extent;
    }
}
