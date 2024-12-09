/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry.utils;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SpatialEnvelopeVisitorTests extends ESTestCase {

    public void testVisitCartesianShape() {
        for (int i = 0; i < 1000; i++) {
            var geometry = ShapeTestUtils.randomGeometryWithoutCircle(0, false);
            var bbox = SpatialEnvelopeVisitor.visitCartesian(geometry);
            assertNotNull(bbox);
            assertTrue(i + ": " + geometry, bbox.isPresent());
            var result = bbox.get();
            assertThat(i + ": " + geometry, result.getMinX(), lessThanOrEqualTo(result.getMaxX()));
            assertThat(i + ": " + geometry, result.getMinY(), lessThanOrEqualTo(result.getMaxY()));
        }
    }

    public void testVisitGeoShapeNoWrap() {
        for (int i = 0; i < 1000; i++) {
            var geometry = GeometryTestUtils.randomGeometryWithoutCircle(0, false);
            var bbox = SpatialEnvelopeVisitor.visitGeo(geometry, false);
            assertNotNull(bbox);
            assertTrue(i + ": " + geometry, bbox.isPresent());
            var result = bbox.get();
            assertThat(i + ": " + geometry, result.getMinX(), lessThanOrEqualTo(result.getMaxX()));
            assertThat(i + ": " + geometry, result.getMinY(), lessThanOrEqualTo(result.getMaxY()));
        }
    }

    public void testVisitGeoShapeWrap() {
        for (int i = 0; i < 1000; i++) {
            var geometry = GeometryTestUtils.randomGeometryWithoutCircle(0, true);
            var bbox = SpatialEnvelopeVisitor.visitGeo(geometry, false);
            assertNotNull(bbox);
            assertTrue(i + ": " + geometry, bbox.isPresent());
            var result = bbox.get();
            assertThat(i + ": " + geometry, result.getMinX(), lessThanOrEqualTo(result.getMaxX()));
            assertThat(i + ": " + geometry, result.getMinY(), lessThanOrEqualTo(result.getMaxY()));
        }
    }

    public void testVisitCartesianPoints() {
        var visitor = new SpatialEnvelopeVisitor(new SpatialEnvelopeVisitor.CartesianPointVisitor());
        double minX = Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        for (int i = 0; i < 1000; i++) {
            var x = randomFloat();
            var y = randomFloat();
            var point = new Point(x, y);
            visitor.visit(point);
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
            var result = visitor.getResult();
            assertThat(i + ": " + point, result.getMinX(), equalTo(minX));
            assertThat(i + ": " + point, result.getMinY(), equalTo(minY));
            assertThat(i + ": " + point, result.getMaxX(), equalTo(maxX));
            assertThat(i + ": " + point, result.getMaxY(), equalTo(maxY));
        }
    }

    public void testVisitGeoPointsNoWrapping() {
        var visitor = new SpatialEnvelopeVisitor(new SpatialEnvelopeVisitor.GeoPointVisitor(false));
        double minY = Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        double minX = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        for (int i = 0; i < 1000; i++) {
            var point = GeometryTestUtils.randomPoint();
            visitor.visit(point);
            minY = Math.min(minY, point.getY());
            maxY = Math.max(maxY, point.getY());
            minX = Math.min(minX, point.getX());
            maxX = Math.max(maxX, point.getX());
            var result = visitor.getResult();
            assertThat(i + ": " + point, result.getMinX(), lessThanOrEqualTo(result.getMaxX()));
            assertThat(i + ": " + point, result.getMinX(), equalTo(minX));
            assertThat(i + ": " + point, result.getMinY(), equalTo(minY));
            assertThat(i + ": " + point, result.getMaxX(), equalTo(maxX));
            assertThat(i + ": " + point, result.getMaxY(), equalTo(maxY));
        }
    }

    public void testVisitGeoPointsWrapping() {
        var visitor = new SpatialEnvelopeVisitor(new SpatialEnvelopeVisitor.GeoPointVisitor(true));
        double minY = Double.POSITIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        double minNegX = Double.POSITIVE_INFINITY;
        double maxNegX = Double.NEGATIVE_INFINITY;
        double minPosX = Double.POSITIVE_INFINITY;
        double maxPosX = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < 1000; i++) {
            var point = GeometryTestUtils.randomPoint();
            visitor.visit(point);
            minY = Math.min(minY, point.getY());
            maxY = Math.max(maxY, point.getY());
            if (point.getX() >= 0) {
                minPosX = Math.min(minPosX, point.getX());
                maxPosX = Math.max(maxPosX, point.getX());
            } else {
                minNegX = Math.min(minNegX, point.getX());
                maxNegX = Math.max(maxNegX, point.getX());
            }
            var result = visitor.getResult();
            if (Double.isInfinite(minPosX)) {
                // Only negative x values were considered
                assertRectangleResult(i + ": " + point, result, minNegX, maxNegX, maxY, minY, false);
            } else if (Double.isInfinite(minNegX)) {
                // Only positive x values were considered
                assertRectangleResult(i + ": " + point, result, minPosX, maxPosX, maxY, minY, false);
            } else {
                // Both positive and negative x values exist, we need to decide which way to wrap the bbox
                double unwrappedWidth = maxPosX - minNegX;
                double wrappedWidth = (180 - minPosX) - (-180 - maxNegX);
                if (unwrappedWidth <= wrappedWidth) {
                    // The smaller bbox is around the front of the planet, no dateline wrapping required
                    assertRectangleResult(i + ": " + point, result, minNegX, maxPosX, maxY, minY, false);
                } else {
                    // The smaller bbox is around the back of the planet, dateline wrapping required (minx > maxx)
                    assertRectangleResult(i + ": " + point, result, minPosX, maxNegX, maxY, minY, true);
                }
            }
        }
    }

    public void testWillCrossDateline() {
        var visitor = new SpatialEnvelopeVisitor(new SpatialEnvelopeVisitor.GeoPointVisitor(true));
        visitor.visit(new Point(-90.0, 0.0));
        visitor.visit(new Point(90.0, 0.0));
        assertCrossesDateline(visitor, false);
        visitor.visit(new Point(-89.0, 0.0));
        visitor.visit(new Point(89.0, 0.0));
        assertCrossesDateline(visitor, false);
        visitor.visit(new Point(-100.0, 0.0));
        visitor.visit(new Point(100.0, 0.0));
        assertCrossesDateline(visitor, true);
        visitor.visit(new Point(-70.0, 0.0));
        visitor.visit(new Point(70.0, 0.0));
        assertCrossesDateline(visitor, false);
        visitor.visit(new Point(-120.0, 0.0));
        visitor.visit(new Point(120.0, 0.0));
        assertCrossesDateline(visitor, true);
    }

    private void assertCrossesDateline(SpatialEnvelopeVisitor visitor, boolean crossesDateline) {
        var result = visitor.getResult();
        if (crossesDateline) {
            assertThat("Crosses dateline, minx>maxx", result.getMinX(), greaterThanOrEqualTo(result.getMaxX()));
        } else {
            assertThat("Does not cross dateline, minx<maxx", result.getMinX(), lessThanOrEqualTo(result.getMaxX()));
        }
    }

    private void assertRectangleResult(
        String s,
        Rectangle result,
        double minX,
        double maxX,
        double maxY,
        double minY,
        boolean crossesDateline
    ) {
        if (crossesDateline) {
            assertThat(s, result.getMinX(), greaterThanOrEqualTo(result.getMaxX()));
        } else {
            assertThat(s, result.getMinX(), lessThanOrEqualTo(result.getMaxX()));
        }
        assertThat(s, result.getMinX(), equalTo(minX));
        assertThat(s, result.getMaxX(), equalTo(maxX));
        assertThat(s, result.getMaxY(), equalTo(maxY));
        assertThat(s, result.getMinY(), equalTo(minY));
    }
}
