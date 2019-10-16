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

import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


// This test class is just for comparing the size of different encodings.
public class EncodingComparisonTests extends ESTestCase {

    public void testRandomRectangle() throws Exception {
        for (int i =0; i < 100; i++) {
            org.apache.lucene.geo.Rectangle random = GeoTestUtil.nextBox();
            compareWriters(new Rectangle(random.minLon, random.maxLon, random.maxLat, random.minLat),
                new GeoShapeCoordinateEncoder());
        }
    }

    public void testRandomPolygon() throws Exception {
        for (int i =0; i < 100; i++) {
            org.apache.lucene.geo.Polygon random = GeoTestUtil.nextPolygon();
            while (true) {
                try {
                    Tessellator.tessellate(random);
                    break;
                } catch (Exception e) {
                    random = GeoTestUtil.nextPolygon();
                }
            }
            compareWriters(new Polygon(new LinearRing(random.getPolyLons(), random.getPolyLats()),
                Collections.emptyList()), new GeoShapeCoordinateEncoder());
        }
    }

    public void testRandomMultiPolygon() throws Exception {
        for (int i =0; i < 100; i++) {
            int n = TestUtil.nextInt(random(), 2, 10);
            List<Polygon> polygons = new ArrayList<>(n);
            for (int j =0; j < n; j++) {
                org.apache.lucene.geo.Polygon random = GeoTestUtil.nextPolygon();
                while (true) {
                    try {
                        Tessellator.tessellate(random);
                        break;
                    } catch (Exception e) {
                        random = GeoTestUtil.nextPolygon();
                    }
                }
                polygons.add(new Polygon(new LinearRing(random.getPolyLons(), random.getPolyLats()),
                    Collections.emptyList()));
            }
            MultiPolygon multiPolygon = new MultiPolygon(polygons);
            compareWriters(multiPolygon, new GeoShapeCoordinateEncoder());
        }
    }

    public void testRandomLine() throws Exception {
        for (int i =0; i < 100; i++) {
            double[] px = new double[2];
            double[] py = new double[2];
            for (int j =0; j < 2; j++) {
                px[j] = GeoTestUtil.nextLongitude();
                px[j] = GeoTestUtil.nextLatitude();
            }
            compareWriters(new Line(px, py), new GeoShapeCoordinateEncoder());
        }
    }

    public void testRandomLines() throws Exception {
        for (int i =0; i < 100; i++) {
            int numPoints = TestUtil.nextInt(random(), 2, 1000);
            double[] px = new double[numPoints];
            double[] py = new double[numPoints];
            for (int j =0; j < numPoints; j++) {
                px[j] = GeoTestUtil.nextLongitude();
                px[j] = GeoTestUtil.nextLatitude();
            }
            compareWriters(new Line(px, py), new GeoShapeCoordinateEncoder());
        }
    }

    public void testRandomPoint() throws Exception {
        for (int i =0; i < 100; i++) {
            compareWriters(new Point(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude()),
                new GeoShapeCoordinateEncoder());
        }
    }

    public void testRandomPoints() throws Exception {
        for (int i =0; i < 100; i++) {
            int numPoints = TestUtil.nextInt(random(), 1, 1000);
            List<Point> points = new ArrayList<>(numPoints);

            for (int j =0; j < numPoints; j++) {
                points.add(new Point(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude()));
            }
            compareWriters(new MultiPoint(points), new GeoShapeCoordinateEncoder());
        }
    }

    private void compareWriters(Geometry geometry, CoordinateEncoder encoder) throws Exception {
        TriangleTreeWriter writer1 = new TriangleTreeWriter(geometry, encoder);
        GeometryTreeWriter writer2 = new GeometryTreeWriter(geometry, encoder);
        BytesStreamOutput output1 = new BytesStreamOutput();
        BytesStreamOutput output2 = new BytesStreamOutput();
        writer1.writeTo(output1);
        writer2.writeTo(output2);
        output1.close();
        output2.close();
        String s1 = "Triangles: " + output1.bytes().length();
        String s2 = "Edge tree: " + output2.bytes().length();
        System.out.println(s1 + " / " + s2 + " /  diff: " + (double) output1.bytes().length() / output2.bytes().length());

//        org.apache.lucene.geo.Rectangle random = GeoTestUtil.nextBoxNotCrossingDateline();
//
//        int minX = encoder.encodeX(random.minLon);
//        int maxX = encoder.encodeX(random.maxLon);
//        int minY = encoder.encodeY(random.minLat);
//        int maxY = encoder.encodeY(random.maxLat);
//
//        TriangleTreeReader reader1 = new TriangleTreeReader(output1.bytes().toBytesRef(), encoder);
//        GeometryTreeReader reader2 = new GeometryTreeReader(output2.bytes().toBytesRef(), encoder);
//        long t1 = System.currentTimeMillis();
//        for (int i =0; i< 1000; i ++) {
//            reader1.intersects(minX, maxX, minY, maxY);
//        }
//        long t2 = System.currentTimeMillis();
//        long t3 = System.currentTimeMillis();
//        for (int i =0; i< 1000; i ++) {
//            reader2.intersects(Extent.fromPoints(minX, minY, maxX, maxY));
//        }
//        long t4 = System.currentTimeMillis();
//        String s3 = "Triangles: " + (t2 - t1);
//        String s4 = "Edge tree: " + (t4 - t3);
//        System.out.println(s3 + " / " + s4 + " /  diff: " + (double) (t2 - t1) / (t4 - t3));
    }

}
