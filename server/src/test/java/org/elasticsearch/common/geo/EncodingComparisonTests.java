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
import org.apache.lucene.util.LuceneTestCase;
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
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.spatial4j.io.GeohashUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


// This test class is just for comparing the size of different encodings.
@LuceneTestCase.AwaitsFix(bugUrl = "this is just for reference")
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
            int numPoints = TestUtil.nextInt(random(), 2, 1000);
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
        //String s1 = "Triangles: " + output1.bytes().length();
        //String s2 = "Edge tree: " + output2.bytes().length();
        // System.out.println(s1 + " / " + s2 + " /  diff: " + (double) output1.bytes().length() / output2.bytes().length());

        int maxPrecision = 5;
        TriangleTreeReader reader1 = new TriangleTreeReader(encoder);
        reader1.reset(output1.bytes().toBytesRef());
        //long t1 = System.nanoTime();
        int h1 = getHashesAtLevel(reader1, encoder, "", maxPrecision);
        //long t2 = System.nanoTime();
        GeometryTreeReader reader2 = new GeometryTreeReader(encoder);
        reader2.reset(output1.bytes().toBytesRef());
        //long t3 = System.nanoTime();
        int h2= getHashesAtLevel(reader2, encoder, "", maxPrecision);
        //long t4 = System.nanoTime();
        assertEquals(h1, h2);

        //String s3 = "Triangles: " + h1; //(t2 - t1);
        //String s4 = "Edge tree: " + h2; //(t4 - t3);
        //System.out.println("Query: " + s3 + " / " + s4 + " /  diff: " + (double) (t2 - t1) / (t4 - t3));
    }

    private int getHashesAtLevel(ShapeTreeReader reader, CoordinateEncoder encoder, String hash, int maxPrecision) throws IOException {
        int hits = 0;
        String[] hashes = GeohashUtils.getSubGeohashes(hash);
        for (int i =0; i < hashes.length; i++) {
            Rectangle r = Geohash.toBoundingBox(hashes[i]);
            GeoRelation rel = reader.relate(encoder.encodeX(r.getMinLon()),
                encoder.encodeY(r.getMinLat()),
                encoder.encodeX(r.getMaxLon()),
                encoder.encodeY(r.getMaxLat()));
            if (rel == GeoRelation.QUERY_CROSSES) {
                if (hashes[i].length() == maxPrecision) {
                    hits++;
                } else {
                    hits += getHashesAtLevel(reader, encoder, hashes[i], maxPrecision);
                }
            } else if (rel == GeoRelation.QUERY_INSIDE) {
                if (hashes[i].length() == maxPrecision) {
                    hits++;
                } else {
                    hits += getHashesAtLevel(hashes[i], maxPrecision);
                }
            }
        }
        return hits;
    }

    private int getHashesAtLevel(String hash, int maxPrecision) {
        int hits = 0;
        String[] hashes = GeohashUtils.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            if (hashes[i].length() == maxPrecision) {
                hits++;
            } else {
                hits += getHashesAtLevel(hashes[i], maxPrecision);
            }
        }
        return hits;
    }
}
