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

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.elasticsearch.common.geo.GeoTestUtils.assertRelation;
import static org.hamcrest.Matchers.equalTo;

public class Point2DTests extends ESTestCase {

    public void testOnePoint() throws IOException {
        int x = randomIntBetween(-90, 90);
        int y = randomIntBetween(-90, 90);
        Point2DWriter writer = new Point2DWriter(x, y, TestCoordinateEncoder.INSTANCE);

        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        Point2DReader reader = new Point2DReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)));
        assertThat(reader.getExtent(), equalTo(Extent.fromPoint(x, y)));
        assertThat(reader.getExtent(), equalTo(reader.getExtent()));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoint(x, y));
        assertRelation(GeoRelation.QUERY_CROSSES, reader,
            Extent.fromPoints(x, y, x + randomIntBetween(1, 10), y + randomIntBetween(1, 10)));
        assertRelation(GeoRelation.QUERY_CROSSES, reader,
            Extent.fromPoints(x - randomIntBetween(1, 10), y - randomIntBetween(1, 10), x, y));
        assertRelation(GeoRelation.QUERY_CROSSES, reader,
            Extent.fromPoints(x - randomIntBetween(1, 10), y - randomIntBetween(1, 10),
            x + randomIntBetween(1, 10), y + randomIntBetween(1, 10)));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader,
            Extent.fromPoints(x - randomIntBetween(10, 100), y - randomIntBetween(10, 100),
            x - randomIntBetween(1, 10), y - randomIntBetween(1, 10)));
    }

    public void testPoints() throws IOException {
        for (int i = 0; i < 500; i++) {
            int minX = randomIntBetween(-180, 170);
            int maxX = randomIntBetween(minX + 10, 180);
            int minY = randomIntBetween(-90, 80);
            int maxY = randomIntBetween(minY + 10, 90);
            Extent extent = Extent.fromPoints(minX, minY, maxX, maxY);
            int numPoints = randomIntBetween(2, 1000);

            double[] x = new double[numPoints];
            double[] y = new double[numPoints];
            for (int j = 0; j < numPoints; j++) {
                x[j] = randomIntBetween(minX, maxX);
                y[j] = randomIntBetween(minY, maxY);
            }
            Point2DWriter writer = new Point2DWriter(x, y, TestCoordinateEncoder.INSTANCE);

            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            Point2DReader reader = new Point2DReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)));
            // tests calling getExtent() and relate() multiple times to make sure deserialization is not affected
            assertThat(reader.getExtent(), equalTo(reader.getExtent()));
            assertThat(reader.getExtent(), equalTo(writer.getExtent()));
            assertRelation(GeoRelation.QUERY_CROSSES, reader, extent);
            assertRelation(GeoRelation.QUERY_CROSSES, reader, extent);
        }
    }
}
