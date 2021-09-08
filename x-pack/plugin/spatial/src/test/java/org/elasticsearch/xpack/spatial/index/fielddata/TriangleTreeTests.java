/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TriangleTreeTests extends ESTestCase {

    public void testVisitAllTriangles() throws IOException {
        Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 10), false);
        // write tree
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        List<IndexableField> fieldList = indexer.indexShape(geometry);
        BytesStreamOutput output = new BytesStreamOutput();
        TriangleTreeWriter.writeTo(output, fieldList);
        // read tree
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        BytesRef bytesRef = output.bytes().toBytesRef();
        input.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        Extent extent = new Extent();
        Extent.readFromCompressed(input, extent);
        TriangleCounterVisitor visitor = new TriangleCounterVisitor();
        TriangleTreeReader.visit(input, visitor, extent.maxX(), extent.maxY());
        assertThat(fieldList.size(), equalTo(visitor.counter));
    }

    private static class TriangleCounterVisitor implements TriangleTreeReader.Visitor  {

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
