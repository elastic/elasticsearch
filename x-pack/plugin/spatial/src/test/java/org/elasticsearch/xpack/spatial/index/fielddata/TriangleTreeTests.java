/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TriangleTreeTests extends ESTestCase {

    public void testVisitAllTriangles() throws IOException {
        Geometry geometry = GeometryTestUtils.randomGeometryWithoutCircle(randomIntBetween(1, 10), false);
        ShapeField.DecodedTriangle[] triangles = GeoTestUtils.toDecodedTriangles(geometry);
        GeometryDocValueReader reader = GeoTestUtils.GeometryDocValueReader(geometry, TestCoordinateEncoder.INSTANCE);
        TriangleCounterVisitor visitor = new TriangleCounterVisitor();
        reader.visit(visitor);
        assertThat(triangles.length, equalTo(visitor.counter));
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
