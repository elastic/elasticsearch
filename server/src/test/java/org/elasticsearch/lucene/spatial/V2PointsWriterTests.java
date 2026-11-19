/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Checks the V2 points-only layout against the legacy triangle tree for the same points: the header, the
 * reconstructed points, and the points a box query finds when blocks of points are skipped.
 */
public class V2PointsWriterTests extends ESTestCase {

    public void testMatchesLegacy() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            Geometry geometry = randomBoolean() ? GeometryTestUtils.randomPoint(false) : GeometryTestUtils.randomMultiPoint(false);
            assertMatchesLegacy(geometry);
        }
    }

    /** Many points in random order, so they span several blocks of the vertex table. */
    public void testManyPointsMatchLegacy() throws IOException {
        List<Point> points = new ArrayList<>();
        int numPoints = randomIntBetween(VertexLookupTable.BLOCK_SIZE + 1, 20 * VertexLookupTable.BLOCK_SIZE);
        for (int i = 0; i < numPoints; i++) {
            points.add(GeometryTestUtils.randomPoint(false));
        }
        assertMatchesLegacy(new MultiPoint(points));
    }

    /** Duplicate points must all be kept, since the weight and reconstruction count them. */
    public void testDuplicatePoints() throws IOException {
        Point point = GeometryTestUtils.randomPoint(false);
        assertMatchesLegacy(new MultiPoint(List.of(point, point, point)));
    }

    private static void assertMatchesLegacy(Geometry geometry) throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(geometry);
        List<IndexableField> fields = indexer.getIndexableFields(normalized);
        CentroidCalculator centroid = new CentroidCalculator();
        centroid.add(geometry);

        BytesRef legacy = GeometryDocValueWriter.writeLegacy(fields, CoordinateEncoder.GEO, centroid);
        GeometryDocValueReader legacyReader = new GeometryDocValueReader();
        legacyReader.reset(legacy);
        List<Long> expected = sortedPoints(legacyReader.getGeometry(CoordinateEncoder.GEO));
        Extent legacyExtent = legacyReader.getExtent();
        int[] box = randomBox(legacyExtent);
        int expectedHits = boxHits(legacyReader, legacy, box);

        BytesRef bytes = GeometryDocValueWriter.write(fields, CoordinateEncoder.GEO, centroid, List.of(geometry));
        String message = geometry.toString();
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(bytes);
        assertTrue(message, reader.isV2Format());
        assertThat(message, reader.getDimensionalShapeType(), equalTo(DimensionalShapeType.POINT));
        assertThat(message, reader.getCentroidX(), equalTo(legacyReader.getCentroidX()));
        assertThat(message, reader.getCentroidY(), equalTo(legacyReader.getCentroidY()));
        assertThat(message, reader.getSumCentroidWeight(), equalTo(legacyReader.getSumCentroidWeight()));
        assertThat(message, reader.getExtent(), equalTo(legacyExtent));
        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        if (fields.size() == 1) {
            assertThat(message, reconstructed, instanceOf(Point.class));
        }
        assertThat(message, sortedPoints(reconstructed), equalTo(expected));
        assertThat(message, boxHits(reader, bytes, box), equalTo(expectedHits));
    }

    private static List<Long> sortedPoints(Geometry geometry) {
        List<Long> points = new ArrayList<>();
        if (geometry instanceof Point p) {
            points.add(encode(p));
        } else {
            for (Point p : (MultiPoint) geometry) {
                points.add(encode(p));
            }
        }
        points.sort(Long::compare);
        return points;
    }

    private static long encode(Point p) {
        return ((long) CoordinateEncoder.GEO.encodeX(p.getX()) << 32) | (CoordinateEncoder.GEO.encodeY(p.getY()) & 0xFFFFFFFFL);
    }

    private static int[] randomBox(Extent extent) {
        int x1 = randomIntBetween(extent.minX(), extent.maxX());
        int x2 = randomIntBetween(extent.minX(), extent.maxX());
        int y1 = randomIntBetween(extent.minY(), extent.maxY());
        int y2 = randomIntBetween(extent.minY(), extent.maxY());
        return new int[] { Math.min(x1, x2), Math.min(y1, y2), Math.max(x1, x2), Math.max(y1, y2) };
    }

    /** Counts the points inside {@code box}, skipping subtrees or blocks of points that cannot intersect it. */
    private static int boxHits(GeometryDocValueReader reader, BytesRef bytes, int[] box) throws IOException {
        int[] hits = new int[1];
        reader.reset(bytes);
        reader.visit(new TriangleTreeVisitor() {
            @Override
            public void visitPoint(int x, int y) {
                if (x >= box[0] && x <= box[2] && y >= box[1] && y <= box[3]) {
                    hits[0]++;
                }
            }

            @Override
            public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {}

            @Override
            public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {}

            @Override
            public boolean push() {
                return true;
            }

            @Override
            public boolean pushX(int minX) {
                return minX <= box[2];
            }

            @Override
            public boolean pushY(int minY) {
                return minY <= box[3];
            }

            @Override
            public boolean push(int maxX, int maxY) {
                return maxX >= box[0] && maxY >= box[1];
            }

            @Override
            public boolean push(int minX, int minY, int maxX, int maxY) {
                return minX <= box[2] && maxX >= box[0] && minY <= box[3] && maxY >= box[1];
            }
        });
        return hits[0];
    }
}
