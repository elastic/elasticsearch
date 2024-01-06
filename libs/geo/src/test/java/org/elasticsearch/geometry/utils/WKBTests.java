/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.utils;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class WKBTests extends ESTestCase {

    public void testEmptyPoint() {
        Point point = Point.EMPTY;
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> WellKnownBinary.toWKB(point, randomByteOrder()));
        assertEquals("Empty POINT cannot be represented in WKB", ex.getMessage());
    }

    public void testPoint() {
        Point point = GeometryTestUtils.randomPoint(randomBoolean());
        assertWKB(point);
    }

    public void testEmptyMultiPoint() {
        MultiPoint multiPoint = MultiPoint.EMPTY;
        assertWKB(multiPoint);
    }

    public void testMultiPoint() {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(randomBoolean());
        assertWKB(multiPoint);
    }

    public void testEmptyLine() {
        Line line = Line.EMPTY;
        assertWKB(line);
    }

    public void testLine() {
        Line line = GeometryTestUtils.randomLine(randomBoolean());
        assertWKB(line);
    }

    public void tesEmptyMultiLine() {
        MultiLine multiLine = MultiLine.EMPTY;
        assertWKB(multiLine);
    }

    public void testMultiLine() {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(randomBoolean());
        assertWKB(multiLine);
    }

    public void testEmptyPolygon() {
        Polygon polygon = Polygon.EMPTY;
        assertWKB(polygon);
    }

    public void testPolygon() {
        final boolean hasZ = randomBoolean();
        Polygon polygon = GeometryTestUtils.randomPolygon(hasZ);
        if (randomBoolean()) {
            int numHoles = randomInt(4);
            List<LinearRing> holes = new ArrayList<>(numHoles);
            for (int i = 0; i < numHoles; i++) {
                holes.add(GeometryTestUtils.randomPolygon(hasZ).getPolygon());
            }
            polygon = new Polygon(polygon.getPolygon(), holes);
        }
        assertWKB(polygon);
    }

    public void testEmptyMultiPolygon() {
        MultiPolygon multiPolygon = MultiPolygon.EMPTY;
        assertWKB(multiPolygon);
    }

    public void testMultiPolygon() {
        MultiPolygon multiPolygon = GeometryTestUtils.randomMultiPolygon(randomBoolean());
        assertWKB(multiPolygon);
    }

    public void testEmptyGeometryCollection() {
        GeometryCollection<Geometry> collection = GeometryCollection.EMPTY;
        assertWKB(collection);
    }

    public void testGeometryCollection() {
        GeometryCollection<Geometry> collection = GeometryTestUtils.randomGeometryCollection(randomBoolean());
        assertWKB(collection);
    }

    public void testEmptyCircle() {
        Circle circle = Circle.EMPTY;
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> WellKnownBinary.toWKB(circle, randomByteOrder()));
        assertEquals("Empty CIRCLE cannot be represented in WKB", ex.getMessage());
    }

    public void testCircle() {
        Circle circle = GeometryTestUtils.randomCircle(randomBoolean());
        assertWKB(circle);
    }

    public void testEmptyRectangle() {
        Rectangle rectangle = Rectangle.EMPTY;
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> WellKnownBinary.toWKB(rectangle, randomByteOrder())
        );
        assertEquals("Empty ENVELOPE cannot be represented in WKB", ex.getMessage());
    }

    public void testRectangle() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        assertWKB(rectangle);
    }

    private ByteOrder randomByteOrder() {
        return randomBoolean() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    private void assertWKB(Geometry geometry) {
        final boolean hasZ = geometry.hasZ();
        final ByteOrder byteOrder = randomByteOrder();
        final byte[] b = WellKnownBinary.toWKB(geometry, byteOrder);
        if (randomBoolean()) {
            // add padding to the byte array
            final int extraBytes = randomIntBetween(1, 500);
            final byte[] oversizeB = new byte[b.length + extraBytes];
            random().nextBytes(oversizeB);
            final int offset = randomInt(extraBytes);
            System.arraycopy(b, 0, oversizeB, offset, b.length);
            assertEquals(geometry, WellKnownBinary.fromWKB(StandardValidator.instance(hasZ), randomBoolean(), oversizeB, offset, b.length));
        } else {
            assertEquals(geometry, WellKnownBinary.fromWKB(StandardValidator.instance(hasZ), randomBoolean(), b));
        }
    }

}
