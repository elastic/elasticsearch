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
import java.text.ParseException;
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

    public void testFromWKTPoint() throws Exception {
        assertFromWKT(GeometryTestUtils.randomPoint(randomBoolean()));
    }

    public void testFromWKTMultiPoint() throws Exception {
        assertFromWKT(GeometryTestUtils.randomMultiPoint(randomBoolean()));
    }

    public void testFromWKTEmptyMultiPoint() throws Exception {
        assertFromWKT(MultiPoint.EMPTY);
    }

    public void testFromWKTLine() throws Exception {
        assertFromWKT(GeometryTestUtils.randomLine(randomBoolean()));
    }

    public void testFromWKTEmptyLine() throws Exception {
        assertFromWKT(Line.EMPTY);
    }

    public void testFromWKTMultiLine() throws Exception {
        assertFromWKT(GeometryTestUtils.randomMultiLine(randomBoolean()));
    }

    public void testFromWKTEmptyMultiLine() throws Exception {
        assertFromWKT(MultiLine.EMPTY);
    }

    public void testFromWKTPolygon() throws Exception {
        assertFromWKT(GeometryTestUtils.randomPolygon(randomBoolean()));
    }

    public void testFromWKTEmptyPolygon() throws Exception {
        assertFromWKT(Polygon.EMPTY);
    }

    public void testFromWKTMultiPolygon() throws Exception {
        assertFromWKT(GeometryTestUtils.randomMultiPolygon(randomBoolean()));
    }

    public void testFromWKTEmptyMultiPolygon() throws Exception {
        assertFromWKT(MultiPolygon.EMPTY);
    }

    public void testFromWKTGeometryCollection() throws Exception {
        assertFromWKT(GeometryTestUtils.randomGeometryCollection(randomBoolean()));
    }

    public void testFromWKTEmptyGeometryCollection() throws Exception {
        assertFromWKT(GeometryCollection.EMPTY);
    }

    public void testFromWKTCircle() throws Exception {
        assertFromWKT(GeometryTestUtils.randomCircle(randomBoolean()));
    }

    public void testFromWKTRectangle() throws Exception {
        assertFromWKT(GeometryTestUtils.randomRectangle());
    }

    public void testFromWKTLineStringZWithOnly2D() {
        assertFromWKTRejectsLikeGeometryPath("LINESTRING Z (0 0, 1 1)");
    }

    public void testFromWKTPolygonMixedShellAndHoleDimension() {
        assertFromWKTRejectsLikeGeometryPath("POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0), (1 1 1, 1 2 1, 2 2 1, 2 1 1, 1 1 1))");
    }

    public void testFromWKTMultiLineStringMixedDimension() {
        assertFromWKTRejectsLikeGeometryPath("MULTILINESTRING ((0 0, 1 1), (2 2 2, 3 3 3))");
    }

    public void testFromWKTMultiPolygonMixedDimension() {
        assertFromWKTRejectsLikeGeometryPath("MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2 2, 2 3 2, 3 3 2, 3 2 2, 2 2 2)))");
    }

    public void testFromWKTGeometryCollectionMixedDimension() {
        assertFromWKTRejectsLikeGeometryPath("GEOMETRYCOLLECTION (POINT (0 0), POINT (1 1 1))");
    }

    /**
     * Verifies that WellKnownBinary.fromWKT rejects a malformed WKT string with the same behavior
     * (an exception) as the Geometry-based WellKnownText.fromWKT path.
     */
    private void assertFromWKTRejectsLikeGeometryPath(String wkt) {
        expectThrows(IllegalArgumentException.class, () -> WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt));
        expectThrowsAnyOf(
            List.of(IllegalArgumentException.class, ParseException.class),
            () -> WellKnownBinary.fromWKT(wkt, randomByteOrder(), false)
        );
    }

    /** Verifies that WellKnownBinary.fromWKT produces the same WKB as the Geometry-based path. */
    private void assertFromWKT(Geometry geometry) throws Exception {
        final ByteOrder byteOrder = randomByteOrder();
        final String wkt = WellKnownText.toWKT(geometry);
        final byte[] expected = WellKnownBinary.toWKB(geometry, byteOrder);
        final byte[] actual = WellKnownBinary.fromWKT(wkt, byteOrder, false);
        assertArrayEquals("fromWKT result mismatch for: " + wkt, expected, actual);
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
            assertEquals(WellKnownText.toWKT(geometry), WellKnownText.fromWKB(oversizeB, offset, b.length));
        } else {
            assertEquals(geometry, WellKnownBinary.fromWKB(StandardValidator.instance(hasZ), randomBoolean(), b));
            assertEquals(WellKnownText.toWKT(geometry), WellKnownText.fromWKB(b, 0, b.length));
        }
    }
}
