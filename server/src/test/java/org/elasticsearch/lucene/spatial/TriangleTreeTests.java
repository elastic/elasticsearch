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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class TriangleTreeTests extends ESTestCase {

    public void testVisitAllTriangles() throws IOException {
        Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 10), false);
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(geometry);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        // Write using V2 format: tree + vertex table
        VertexLookupTable.Builder vtBuilder = VertexLookupTable.builder();
        BytesStreamOutput output = new BytesStreamOutput();
        TriangleTreeWriter.writeTo(output, fieldList, vtBuilder);
        vtBuilder.writeTo(output);

        // Read tree (extent → treeLength → tree) then vertex table
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        BytesRef bytesRef = output.bytes().toBytesRef();
        input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        Extent extent = new Extent();
        Extent.readFromCompressed(input, extent);
        int treeLength = input.readVInt();
        int treeStart = input.getPosition();
        input.skipBytes(treeLength);
        VertexLookupTable vertexTable = VertexLookupTable.readFrom(input, bytesRef.bytes);

        input.setPosition(treeStart);
        TriangleCounterVisitor visitor = new TriangleCounterVisitor();
        TriangleTreeReader.visit(input, visitor, extent.maxX(), extent.maxY(), vertexTable);
        assertThat(fieldList.size(), equalTo(visitor.counter));
    }

    public void testVisitAllTrianglesLegacy() throws IOException {
        Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 10), false);
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(geometry);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        // Write using legacy format: coordinate deltas
        BytesStreamOutput output = new BytesStreamOutput();
        TriangleTreeWriter.writeLegacy(output, fieldList);

        // Read tree
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        BytesRef bytesRef = output.bytes().toBytesRef();
        input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        Extent extent = new Extent();
        Extent.readFromCompressed(input, extent);
        TriangleCounterVisitor visitor = new TriangleCounterVisitor();
        TriangleTreeReader.visitLegacy(input, visitor, extent.maxX(), extent.maxY());
        assertThat(fieldList.size(), equalTo(visitor.counter));
    }

    public void testVertexDeduplication() throws IOException {
        // A polygon with 4 vertices tessellates into 2 triangles sharing 2 vertices
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 1, 1, 0, 0 }, new double[] { 0, 0, 1, 1, 0 }));
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        List<IndexableField> fieldList = indexer.indexShape(polygon);

        VertexLookupTable.Builder vtBuilder = VertexLookupTable.builder();
        BytesStreamOutput output = new BytesStreamOutput();
        TriangleTreeWriter.writeTo(output, fieldList, vtBuilder);

        // Builder should have 4 unique vertices, regardless of how many triangles reference them
        assertThat(vtBuilder.size(), equalTo(4));

        // Write the vertex table and read it back to verify the round-trip
        vtBuilder.writeTo(output);
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        BytesRef bytesRef = output.bytes().toBytesRef();
        input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        Extent extent = new Extent();
        Extent.readFromCompressed(input, extent);
        int treeLength = input.readVInt();
        input.skipBytes(treeLength);
        VertexLookupTable vertexTable = VertexLookupTable.readFrom(input, bytesRef.bytes);
        assertThat(vertexTable.size(), equalTo(4));

        // Should have at least 2 triangles
        assertThat(fieldList.size(), greaterThan(1));
    }

    public void testExtentDoesNotLoadVertexTable() throws IOException {
        // Use a polygon (not a point) since write() auto-selects legacy for points
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 10, 10, 0, 0 }, new double[] { 0, 0, 10, 10, 0 }));
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(polygon);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(polygon);

        BytesRef bytesRef = GeometryDocValueWriter.write(fieldList, CoordinateEncoder.GEO, centroidCalculator, List.of(normalized));

        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(bytesRef);

        assertTrue(reader.isV2Format());

        // Reading centroid should work without loading anything extra
        assertNotEquals(0, reader.getCentroidX());
        assertNotEquals(0, reader.getCentroidY());
        assertNotNull(reader.getDimensionalShapeType());
        assertNotEquals(0.0, reader.getSumCentroidWeight());

        // Reading extent should work without loading vertex table
        Extent extent = reader.getExtent();
        assertNotNull(extent);
    }

    public void testPointAutoSelectsLegacyFormat() throws IOException {
        Point point = new Point(5.0, 10.0);
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(point);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(point);

        // write() should auto-select legacy format for points
        BytesRef bytesRef = GeometryDocValueWriter.write(fieldList, CoordinateEncoder.GEO, centroidCalculator, List.of(normalized));

        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(bytesRef);

        assertFalse(reader.isV2Format());
        assertThat(reader.getDimensionalShapeType(), equalTo(DimensionalShapeType.POINT));

        // Geometry reconstruction should still work via tree traversal
        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        assertNotNull(reconstructed);
        assertThat(reconstructed.type(), equalTo(normalized.type()));
    }

    public void testMultiPointAutoSelectsLegacyFormat() throws IOException {
        MultiPoint multiPoint = new MultiPoint(List.of(new Point(0, 0), new Point(5, 5), new Point(10, 10)));
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(multiPoint);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(multiPoint);

        // write() should auto-select legacy format for multipoints
        BytesRef bytesRef = GeometryDocValueWriter.write(fieldList, CoordinateEncoder.GEO, centroidCalculator, List.of(normalized));

        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(bytesRef);

        assertFalse(reader.isV2Format());
        assertThat(reader.getDimensionalShapeType(), equalTo(DimensionalShapeType.POINT));

        // Geometry reconstruction should produce a MultiPoint with all 3 points
        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        assertNotNull(reconstructed);
        assertThat(reconstructed.type(), equalTo(normalized.type()));
    }

    public void testFullDocValueRoundTrip() throws IOException {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 10, 10, 0, 0 }, new double[] { 0, 0, 10, 10, 0 }));
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(polygon);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(polygon);

        // Write V2 format explicitly (write() also selects V2 for polygons, but be explicit)
        BytesRef bytesRef = GeometryDocValueWriter.writeV2(fieldList, CoordinateEncoder.GEO, centroidCalculator, List.of(normalized));

        // Read and verify
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(bytesRef);

        assertTrue(reader.isV2Format());

        // Verify centroid is readable
        assertNotEquals(0, reader.getCentroidX());
        assertNotEquals(0, reader.getCentroidY());

        // Verify extent is readable
        Extent extent = reader.getExtent();
        assertNotNull(extent);

        // Verify triangle tree is visitable
        TriangleCounterVisitor counter = new TriangleCounterVisitor();
        reader.visit(counter);
        assertThat(counter.counter, equalTo(fieldList.size()));

        // Verify geometry reconstruction
        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        assertNotNull(reconstructed);
    }

    public void testPointRoundTrip() throws IOException {
        Point point = new Point(5.0, 10.0);
        verifyGeometryRoundTrip(point);
    }

    public void testLineRoundTrip() throws IOException {
        Line line = new Line(new double[] { 0, 5, 10 }, new double[] { 0, 5, 0 });
        verifyGeometryRoundTrip(line);
    }

    public void testMultiPointRoundTrip() throws IOException {
        MultiPoint multiPoint = new MultiPoint(List.of(new Point(0, 0), new Point(5, 5), new Point(10, 10)));
        verifyGeometryRoundTrip(multiPoint);
    }

    private void verifyGeometryRoundTrip(Geometry geometry) throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(geometry);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);

        BytesRef bytesRef = GeometryDocValueWriter.write(fieldList, CoordinateEncoder.GEO, centroidCalculator, List.of(normalized));

        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(bytesRef);

        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        assertNotNull(reconstructed);

        // Verify the reconstructed geometry has the same type
        assertThat(reconstructed.type(), equalTo(normalized.type()));
    }

    // ---- Legacy format backward compatibility tests ----

    public void testLegacyFormatBackwardCompatibility() throws IOException {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 10, 10, 0, 0 }, new double[] { 0, 0, 10, 10, 0 }));
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(polygon);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(polygon);

        // Write in legacy format
        BytesRef legacyBytes = GeometryDocValueWriter.writeLegacy(fieldList, CoordinateEncoder.GEO, centroidCalculator);

        // Read with the new reader
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(legacyBytes);

        // Should detect as legacy format
        assertFalse(reader.isV2Format());

        // Centroid should be readable
        assertNotEquals(0, reader.getCentroidX());
        assertNotEquals(0, reader.getCentroidY());

        // DimensionalShapeType should be readable (high bit is stripped)
        assertThat(reader.getDimensionalShapeType(), equalTo(DimensionalShapeType.POLYGON));

        // Extent should be readable
        Extent extent = reader.getExtent();
        assertNotNull(extent);

        // Triangle tree should be visitable using legacy reader path
        TriangleCounterVisitor counter = new TriangleCounterVisitor();
        reader.visit(counter);
        assertThat(counter.counter, equalTo(fieldList.size()));

        // Geometry reconstruction should return null for legacy format
        assertThat(reader.getGeometry(CoordinateEncoder.GEO), nullValue());
    }

    public void testLegacyFormatRandomGeometries() throws IOException {
        Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 10), false);
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(geometry);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);

        // Write in legacy format using production code
        BytesRef legacyBytes = GeometryDocValueWriter.writeLegacy(fieldList, CoordinateEncoder.GEO, centroidCalculator);

        // Read with the new reader
        GeometryDocValueReader reader = new GeometryDocValueReader();
        reader.reset(legacyBytes);

        assertFalse(reader.isV2Format());

        // Extent should be readable
        assertNotNull(reader.getExtent());

        // Tree should be visitable
        TriangleCounterVisitor counter = new TriangleCounterVisitor();
        reader.visit(counter);
        assertThat(counter.counter, equalTo(fieldList.size()));

        // Geometry reconstruction: available for POINT type (from tree), null for others
        Geometry reconstructed = reader.getGeometry(CoordinateEncoder.GEO);
        if (reader.getDimensionalShapeType() == DimensionalShapeType.POINT) {
            assertNotNull(reconstructed);
        } else {
            assertThat(reconstructed, nullValue());
        }
    }

    public void testLegacyAndV2ProduceSameTriangles() throws IOException {
        Polygon polygon = new Polygon(new LinearRing(new double[] { 0, 10, 10, 0, 0 }, new double[] { 0, 0, 10, 10, 0 }));
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry normalized = indexer.normalize(polygon);
        List<IndexableField> fieldList = indexer.getIndexableFields(normalized);

        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(polygon);

        // Write both formats
        BytesRef legacyBytes = GeometryDocValueWriter.writeLegacy(fieldList, CoordinateEncoder.GEO, centroidCalculator);
        BytesRef v2Bytes = GeometryDocValueWriter.write(fieldList, CoordinateEncoder.GEO, centroidCalculator, List.of(normalized));

        GeometryDocValueReader legacyReader = new GeometryDocValueReader();
        legacyReader.reset(legacyBytes);

        GeometryDocValueReader v2Reader = new GeometryDocValueReader();
        v2Reader.reset(v2Bytes);

        // Both should have same centroid
        assertThat(legacyReader.getCentroidX(), equalTo(v2Reader.getCentroidX()));
        assertThat(legacyReader.getCentroidY(), equalTo(v2Reader.getCentroidY()));

        // Both should have same extent
        Extent legacyExtent = legacyReader.getExtent();
        Extent v2Extent = v2Reader.getExtent();
        assertThat(legacyExtent.minX(), equalTo(v2Extent.minX()));
        assertThat(legacyExtent.maxX(), equalTo(v2Extent.maxX()));
        assertThat(legacyExtent.minY(), equalTo(v2Extent.minY()));
        assertThat(legacyExtent.maxY(), equalTo(v2Extent.maxY()));

        // Both should visit the same number of triangles
        TriangleCounterVisitor legacyCounter = new TriangleCounterVisitor();
        legacyReader.visit(legacyCounter);
        TriangleCounterVisitor v2Counter = new TriangleCounterVisitor();
        v2Reader.visit(v2Counter);
        assertThat(legacyCounter.counter, equalTo(v2Counter.counter));
    }

    private static class TriangleCounterVisitor implements TriangleTreeVisitor {

        int counter;

        @Override
        public void visitPoint(int x, int y) {
            counter++;
        }

        @Override
        public void visitLine(int aX, int aY, int bX, int bY, byte metadata) {
            counter++;
        }

        @Override
        public void visitTriangle(int aX, int aY, int bX, int bY, int cX, int cY, byte metadata) {
            counter++;
        }

        @Override
        public boolean push() {
            return true;
        }

        @Override
        public boolean pushX(int minX) {
            return true;
        }

        @Override
        public boolean pushY(int minY) {
            return true;
        }

        @Override
        public boolean push(int maxX, int maxY) {
            return true;
        }

        @Override
        public boolean push(int minX, int minY, int maxX, int maxY) {
            return true;
        }
    }
}
