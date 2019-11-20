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
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.query.LegacyGeoShapeQueryProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.shape.Rectangle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.elasticsearch.common.geo.GeoTestUtils.assertRelation;
import static org.hamcrest.Matchers.equalTo;

public class EdgeTreeTests extends ESTestCase {

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-180, 170);
            int maxX = randomIntBetween(minX + 10, 180);
            int minY = randomIntBetween(-180, 170);
            int maxY = randomIntBetween(minY + 10, 180);
            double[] x = new double[]{minX, maxX, maxX, minX, minX};
            double[] y = new double[]{minY, minY, maxY, maxY, minY};
            EdgeTreeWriter writer = new EdgeTreeWriter(x, y, TestCoordinateEncoder.INSTANCE, true);
            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            EdgeTreeReader reader = new EdgeTreeReader(
                new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)), true);

            assertThat(writer.getCentroidCalculator().getX(), equalTo((minX + maxX)/2.0));
            assertThat(writer.getCentroidCalculator().getY(), equalTo((minY + maxY)/2.0));


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
            Rectangle box = builder.buildS4J().getBoundingBox();
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

    public void testPacMan() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // candidate relate cell
        int xMin = 2;//-5;
        int xMax = 11;//0.000001;
        int yMin = -1;//0;
        int yMax = 1;//5;

        // test cell crossing poly
        EdgeTreeWriter writer = new EdgeTreeWriter(px, py, TestCoordinateEncoder.INSTANCE, true);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        EdgeTreeReader reader = new EdgeTreeReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)), true);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(xMin, yMin, xMax, yMax));
    }

    public void testGetShapeType() {
        double[] pointCoord = new double[] { 0 };
        assertThat(new EdgeTreeWriter(pointCoord, pointCoord, TestCoordinateEncoder.INSTANCE, false).getShapeType(),
            equalTo(ShapeType.LINESTRING));
        assertThat(new EdgeTreeWriter(List.of(pointCoord, pointCoord), List.of(pointCoord, pointCoord),
                TestCoordinateEncoder.INSTANCE, false).getShapeType(),
            equalTo(ShapeType.MULTILINESTRING));
    }
}
