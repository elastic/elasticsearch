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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class Point2DTests extends ESTestCase {

    public void testOnePoint() throws IOException {
        PointBuilder gen = (PointBuilder) RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POINT);
        Point point = gen.buildGeometry();
        int x = GeoEncodingUtils.encodeLongitude(point.getLon());
        int y = GeoEncodingUtils.encodeLatitude(point.getLat());
        Point2DWriter writer = new Point2DWriter(point);

        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        Point2DReader reader = new Point2DReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)));
        assertThat(reader.getExtent(), equalTo(new Extent(x, y, x, y)));
        assertTrue(reader.intersects(new Extent(x, y, x, y)));
        assertTrue(reader.intersects(new Extent(x, y, x + randomIntBetween(1, 10), y + randomIntBetween(1, 10))));
        assertTrue(reader.intersects(new Extent(x - randomIntBetween(1, 10), y - randomIntBetween(1, 10), x, y)));
        assertTrue(reader.intersects(new Extent(x - randomIntBetween(1, 10), y - randomIntBetween(1, 10),
            x + randomIntBetween(1, 10), y + randomIntBetween(1, 10))));
        assertFalse(reader.intersects(new Extent(x - randomIntBetween(10, 100), y - randomIntBetween(10, 100),
            x - randomIntBetween(1, 10), y - randomIntBetween(1, 10))));
    }

    public void testPoints() throws IOException {
        for (int i = 0; i < 500; i++) {
            int minX = randomIntBetween(-180, 170);
            int maxX = randomIntBetween(minX + 10, 180);
            int minY = randomIntBetween(-90, 80);
            int maxY = randomIntBetween(minY + 10, 90);
            Extent extent = new Extent(GeoEncodingUtils.encodeLongitude(minX), GeoEncodingUtils.encodeLatitude(minY),
                GeoEncodingUtils.encodeLongitude(maxX), GeoEncodingUtils.encodeLatitude(maxY));
            int numPoints = randomIntBetween(2, 1000);

            List<Point> points = new ArrayList<>(numPoints);
            for (int j = 0; j < numPoints; j++) {
                points.add(new Point(randomDoubleBetween(minY, maxY, true), randomDoubleBetween(minX, maxX, true)));
            }
            Point2DWriter writer = new Point2DWriter(new MultiPoint(points));

            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            Point2DReader reader = new Point2DReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)));
            assertThat(reader.getExtent(), equalTo(writer.getExtent()));
            assertTrue(reader.intersects(extent));
        }
    }
}
