/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

public class JtsGeometryConverterTests extends ESTestCase {
    private final JtsGeometryConverter converter = new JtsGeometryConverter(new GeometryFactory());

    /** Converting directly must give exactly the JTS geometry that reading the WKB of the same geometry gives. */
    public void testMatchesWkbConversion() throws Exception {
        for (int i = 0; i < 100; i++) {
            Geometry geometry = randomGeometry(randomIntBetween(0, 2));
            org.locationtech.jts.geom.Geometry viaWkb = UNSPECIFIED.wkbToJtsGeometry(
                new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN))
            );
            org.locationtech.jts.geom.Geometry direct = converter.toJts(geometry);
            assertEquals(viaWkb.getGeometryType(), direct.getGeometryType());
            assertTrue("expected " + viaWkb + " but was " + direct, direct.equalsExact(viaWkb));
        }
    }

    public void testRectangleIsPolygon() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        org.locationtech.jts.geom.Geometry direct = converter.toJts(rectangle);
        org.locationtech.jts.geom.Geometry expected = new GeometryFactory().toGeometry(
            new Envelope(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMinY(), rectangle.getMaxY())
        );
        assertEquals("Polygon", direct.getGeometryType());
        assertTrue("expected " + expected + " but was " + direct, direct.norm().equalsExact(expected.norm()));
    }

    public void testCircleIsRejected() {
        Circle circle = GeometryTestUtils.randomCircle(false);
        expectThrows(IllegalArgumentException.class, () -> converter.toJts(circle));
    }

    private static Geometry randomGeometry(int depth) {
        return switch (randomIntBetween(0, depth > 0 ? 6 : 5)) {
            case 0 -> GeometryTestUtils.randomPoint(false);
            case 1 -> GeometryTestUtils.randomMultiPoint(false);
            case 2 -> GeometryTestUtils.randomLine(false);
            case 3 -> GeometryTestUtils.randomMultiLine(false);
            case 4 -> GeometryTestUtils.randomPolygon(false);
            case 5 -> GeometryTestUtils.randomMultiPolygon(false);
            case 6 -> {
                List<Geometry> geometries = new ArrayList<>();
                for (int i = randomIntBetween(1, 4); i > 0; i--) {
                    geometries.add(randomGeometry(depth - 1));
                }
                yield new GeometryCollection<>(geometries);
            }
            default -> throw new AssertionError("unexpected geometry kind");
        };
    }
}
