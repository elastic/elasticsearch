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

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class WKBTests extends ESTestCase {

    public void testEmptyPoint() {
        Point point = Point.EMPTY;
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> WellKnownBinary.toWKB(point, randomByteOrder()));
        assertEquals("Empty POINT cannot be represented in WKB", ex.getMessage());
    }

    public void testPoint() throws IOException {
        Point point = GeometryTestUtils.randomPoint(randomBoolean());
        assertWKB(point);
    }

    public void testEmptyMultiPoint() throws IOException {
        MultiPoint multiPoint = MultiPoint.EMPTY;
        assertWKB(multiPoint);
    }

    public void testMultiPoint() throws IOException {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(randomBoolean());
        assertWKB(multiPoint);
    }

    public void testEmptyLine() throws IOException {
        Line line = Line.EMPTY;
        assertWKB(line);
    }

    public void testLine() throws IOException {
        Line line = GeometryTestUtils.randomLine(randomBoolean());
        assertWKB(line);
    }

    public void tesEmptyMultiLine() throws IOException {
        MultiLine multiLine = MultiLine.EMPTY;
        assertWKB(multiLine);
    }

    public void testMultiLine() throws IOException {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(randomBoolean());
        assertWKB(multiLine);
    }

    public void testEmptyPolygon() throws IOException {
        Polygon polygon = Polygon.EMPTY;
        assertWKB(polygon);
    }

    public void testPolygon() throws IOException {
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

    public void testEmptyMultiPolygon() throws IOException {
        MultiPolygon multiPolygon = MultiPolygon.EMPTY;
        assertWKB(multiPolygon);
    }

    public void testMultiPolygon() throws IOException {
        MultiPolygon multiPolygon = GeometryTestUtils.randomMultiPolygon(randomBoolean());
        assertWKB(multiPolygon);
    }

    public void testEmptyGeometryCollection() throws IOException {
        GeometryCollection<Geometry> collection = GeometryCollection.EMPTY;
        assertWKB(collection);
    }

    public void testGeometryCollection() throws IOException {
        GeometryCollection<Geometry> collection = GeometryTestUtils.randomGeometryCollection(randomBoolean());
        assertWKB(collection);
    }

    public void testEmptyCircle() {
        Circle circle = Circle.EMPTY;
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> WellKnownBinary.toWKB(circle, randomByteOrder()));
        assertEquals("Empty CIRCLE cannot be represented in WKB", ex.getMessage());
    }

    public void testCircle() throws IOException {
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

    public void testRectangle() throws IOException {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        assertWKB(rectangle);
    }

    private ByteOrder randomByteOrder() {
        return randomBoolean() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    private void assertWKB(Geometry geometry) throws IOException {
        final boolean hasZ = geometry.hasZ();
        final ByteOrder byteOrder = randomByteOrder();
        byte[] b = WellKnownBinary.toWKB(geometry, byteOrder);
        assertEquals(geometry, WellKnownBinary.fromWKB(StandardValidator.instance(hasZ), randomBoolean(), b));
    }

}
